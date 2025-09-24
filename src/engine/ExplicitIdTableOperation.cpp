// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#include "engine/ExplicitIdTableOperation.h"

// _____________________________________________________________________________
ExplicitIdTableOperation::ExplicitIdTableOperation(
    QueryExecutionContext* ctx, std::shared_ptr<const IdTable> table,
    VariableToColumnMap variables, std::vector<ColumnIndex> sortedColumns,
    LocalVocab localVocab)
    : Operation(ctx),
      idTable_(std::move(table)),
      variables_(std::move(variables)),
      sortedColumns_(std::move(sortedColumns)),
      localVocab_(std::move(localVocab)) {
  // An explicit IdTable operation is never stored in the cache because it
  // 1. Doesn't have a valid cache key and
  // 2. Is mostly used to implement already cached results (the
  // `shared_ptr<IdTable>` typically originates from a cache).
  disableStoringInCache();
}

// _____________________________________________________________________________
Result ExplicitIdTableOperation::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  return {idTable_, resultSortedOn(), localVocab_.clone()};
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> ExplicitIdTableOperation::getChildren() {
  return {};
}

// We disable the storing in the cache in the destructor, so it is not important
// to have a valid cache key.
std::string ExplicitIdTableOperation::getCacheKeyImpl() const { return ""; }

// _____________________________________________________________________________
std::string ExplicitIdTableOperation::getDescriptor() const {
  return "Explicit Result";
}

// _____________________________________________________________________________
size_t ExplicitIdTableOperation::getResultWidth() const {
  return idTable_->numColumns();
}

// The result is ready immediately.
size_t ExplicitIdTableOperation::getCostEstimate() { return 0; }

// _____________________________________________________________________________
uint64_t ExplicitIdTableOperation::getSizeEstimateBeforeLimit() {
  return sizeEstimate();
}

// _____________________________________________________________________________
float ExplicitIdTableOperation::getMultiplicity([[maybe_unused]] size_t col) {
  // The multiplicity currently is a dummy, we could extend this class to also
  // (optionally) store multiplicities.
  return 1.0f;
}

// _____________________________________________________________________________
bool ExplicitIdTableOperation::knownEmptyResult() { return idTable_->empty(); }

// _____________________________________________________________________________
std::unique_ptr<Operation> ExplicitIdTableOperation::cloneImpl() const {
  return std::make_unique<ExplicitIdTableOperation>(
      getExecutionContext(), idTable_, variables_, sortedColumns_,
      localVocab_.clone());
}

// _____________________________________________________________________________
std::vector<ColumnIndex> ExplicitIdTableOperation::resultSortedOn() const {
  return sortedColumns_;
}

// _____________________________________________________________________________
VariableToColumnMap ExplicitIdTableOperation::computeVariableToColumnMap()
    const {
  return variables_;
}
