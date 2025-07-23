//
// Created by kalmbacj on 7/22/25.
//

#include "engine/StripColumns.h"

#include "engine/QueryExecutionTree.h"

// _____________________________________________________________________________
StripColumns::StripColumns(QueryExecutionContext* ctx,
                           std::shared_ptr<QueryExecutionTree> child,
                           ad_utility::HashSet<Variable> keepVariables)
    : Operation{ctx}, child_{std::move(child)} {
  const auto& childVars = child_->getVariableColumns();
  for (const auto& var : keepVariables) {
    auto it = childVars.find(var);
    if (it != childVars.end()) {
      varToCol_.emplace(var, it->second);
      subset_.push_back(it->second.columnIndex_);
    }
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

std::string StripColumns::getCacheKeyImpl() const {
  return absl::StrCat("StripColumns(", absl::StrJoin(subset_, ","), " - ",
                      child_->getCacheKey(), ")");
}

std::string StripColumns::getDescriptor() const { return "Strip Columns"; }

size_t StripColumns::getResultWidth() const { return subset_.size(); }

size_t StripColumns::getCostEstimate() { return child_->getCostEstimate(); }

uint64_t StripColumns::getSizeEstimateBeforeLimit() {
  return child_->getSizeEstimate();
}

float StripColumns::getMultiplicity(size_t col) {
  return child_->getMultiplicity(subset_.at(col));
}

bool StripColumns::knownEmptyResult() { return child_->knownEmptyResult(); }

std::unique_ptr<Operation> StripColumns::cloneImpl() const {
  return std::make_unique<StripColumns>(getExecutionContext(), child_->clone(),
                                        subset_, varToCol_);
}

// ___________________________________________________________________________________________
std::vector<ColumnIndex> StripColumns::resultSortedOn() const {
  std::vector<ColumnIndex> sortedOn;
  const auto& fromChild = child_->resultSortedOn();
  for (auto col : fromChild) {
    auto it = ql::ranges::find(subset_, col);
    if (it == subset_.end()) {
      break;
    }
    sortedOn.push_back(*it);
  }
  return sortedOn;
}

Result StripColumns::computeResult(bool requestLaziness) {
  auto res = child_->getResult(requestLaziness);
  if (res->isFullyMaterialized()) {
    // TODO<joka921> This is really really inefficient.
    auto table = res->idTable().clone();
    table.setColumnSubset(subset_);
    return {std::move(table), resultSortedOn(), res->localVocab().clone()};
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
VariableToColumnMap StripColumns::computeVariableToColumnMap() const {
  return varToCol_;
}
