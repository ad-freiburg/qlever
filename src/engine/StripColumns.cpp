// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/StripColumns.h"

#include "engine/QueryExecutionTree.h"

// _____________________________________________________________________________
StripColumns::StripColumns(QueryExecutionContext* ctx,
                           std::shared_ptr<QueryExecutionTree> child,
                           const std::set<Variable>& keepVariables)
    : Operation{ctx}, child_{std::move(child)} {
  const auto& childVars = child_->getVariableColumns();
  // For each of the `keepVariables` find the corresponding column index in the
  // child. Silently skip nonexisting variables.
  for (const auto& var : keepVariables) {
    auto it = childVars.find(var);
    if (it == childVars.end()) {
      continue;
    }
    // Make a copy of the entry (preserve the definedness), and adapt the
    // column index.
    auto info = it->second;
    auto colIdxInChild = it->second.columnIndex_;
    info.columnIndex_ = subset_.size();
    varToCol_.emplace(var, std::move(info));
    subset_.push_back(colIdxInChild);
  }
}

// _____________________________________________________________________________
StripColumns::StripColumns(QueryExecutionContext* ctx,
                           std::shared_ptr<QueryExecutionTree> child,
                           std::vector<ColumnIndex> subset,
                           VariableToColumnMap varToCol)
    : Operation{ctx},
      child_{std::move(child)},
      subset_{std::move(subset)},
      varToCol_{std::move(varToCol)} {};

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> StripColumns::getChildren() {
  return {child_.get()};
}

// _____________________________________________________________________________
std::string StripColumns::getCacheKeyImpl() const {
  return absl::StrCat("StripColumns(", absl::StrJoin(subset_, ","), " - ",
                      child_->getCacheKey(), ")");
}

// _____________________________________________________________________________
std::string StripColumns::getDescriptor() const { return "Strip Columns"; }

// _____________________________________________________________________________
size_t StripColumns::getResultWidth() const { return subset_.size(); }

// _____________________________________________________________________________
size_t StripColumns::getCostEstimate() { return child_->getCostEstimate(); }

// _____________________________________________________________________________
uint64_t StripColumns::getSizeEstimateBeforeLimit() {
  return child_->getSizeEstimate();
}

// _____________________________________________________________________________
float StripColumns::getMultiplicity(size_t col) {
  return col < subset_.size() ? child_->getMultiplicity(subset_.at(col)) : 1.0f;
}

// _____________________________________________________________________________
bool StripColumns::knownEmptyResult() { return child_->knownEmptyResult(); }

// _____________________________________________________________________________
std::unique_ptr<Operation> StripColumns::cloneImpl() const {
  return std::make_unique<StripColumns>(getExecutionContext(), child_->clone(),
                                        subset_, varToCol_);
}

// _____________________________________________________________________________
std::vector<ColumnIndex> StripColumns::resultSortedOn() const {
  std::vector<ColumnIndex> sortedOn;
  const auto& fromChild = child_->resultSortedOn();
  // Find the largest prefix of `fromChild` that is part of the `subset` and
  // return it with the columns mapped accordingly. In particular, if the child
  // is ordered by three variables
  // `?a ?b ?c`, but only `?a` and `?c` are preserved by this `StripColumns`
  // operation, then the result of `*this` is only sorted by `?a`.
  for (auto col : fromChild) {
    auto it = ql::ranges::find(subset_, col);
    if (it == subset_.end()) {
      break;
    }
    // the position in the subset is the column index wrt `*this`.
    sortedOn.push_back(it - subset_.begin());
  }
  return sortedOn;
}

// _____________________________________________________________________________
Result StripColumns::computeResult(bool requestLaziness) {
  auto res = child_->getResult(requestLaziness);
  if (res->isFullyMaterialized()) {
    // TODO<joka921> This case currently is inefficient, we should really
    // implement moving the tables from materialized results that are too big
    // for the cache or having a `shared_ptr<IdTable+SubsetView>` type in the
    // result.
    auto table = res->idTable().asColumnSubsetView(subset_).clone();
    return {std::move(table), resultSortedOn(), res->getSharedLocalVocab()};
  } else {
    return {Result::LazyResult{ad_utility::CachingTransformInputRange(
                res->idTables(),
                [this](auto& tableAndVocab) {
                  tableAndVocab.idTable_.setColumnSubset(subset_);
                  return std::move(tableAndVocab);
                })},
            resultSortedOn()};
  }
}

// _____________________________________________________________________________
VariableToColumnMap StripColumns::computeVariableToColumnMap() const {
  return varToCol_;
}
