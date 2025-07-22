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
  for (const auto& var : keepVariables) {
    auto idx = child_->getVariableColumnOrNullopt();
    if (idx.has_value) {
      subset_.push_back(idx.value());
    }
  }
}

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
  // TODO<joka921> implement;
}

std::vector<ColumnIndex> StripColumns::resultSortedOn() const {
  // TODO<joka921> implement;
}
