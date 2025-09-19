//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/ExplicitIdTableOperation.h"

ExplicitIdTableOperation::ExplicitIdTableOperation(
    QueryExecutionContext* ctx, std::shared_ptr<const IdTable> table,
    VariableToColumnMap variables, std::vector<ColumnIndex> sortedColumns,
    LocalVocab localVocab)
    : Operation(ctx),
      idTable_(std::move(table)),
      variables_(std::move(variables)),
      sortedColumns_(std::move(sortedColumns)),
      localVocab_(std::move(localVocab)) {
  disableStoringInCache();
}

Result ExplicitIdTableOperation::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  return {idTable_, resultSortedOn(), localVocab_.clone()};
}

std::vector<QueryExecutionTree*> ExplicitIdTableOperation::getChildren() {
  return {};
}

std::string ExplicitIdTableOperation::getCacheKeyImpl() const { return ""; }

std::string ExplicitIdTableOperation::getDescriptor() const {
  return "Explicitly IdTable";
}

size_t ExplicitIdTableOperation::getResultWidth() const {
  return idTable_->numColumns();
}

size_t ExplicitIdTableOperation::getCostEstimate() { return 0; }

uint64_t ExplicitIdTableOperation::getSizeEstimateBeforeLimit() {
  return sizeEstimate();
}

float ExplicitIdTableOperation::getMultiplicity([[maybe_unused]] size_t col) {
  return 1.0f;
}

bool ExplicitIdTableOperation::knownEmptyResult() { return idTable_->empty(); }

std::unique_ptr<Operation> ExplicitIdTableOperation::cloneImpl() const {
  return std::make_unique<ExplicitIdTableOperation>(
      getExecutionContext(), idTable_, variables_, sortedColumns_,
      localVocab_.clone());
}

std::vector<ColumnIndex> ExplicitIdTableOperation::resultSortedOn() const {
  return sortedColumns_;
}

VariableToColumnMap ExplicitIdTableOperation::computeVariableToColumnMap()
    const {
  return variables_;
}
