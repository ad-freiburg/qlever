// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#include "engine/ExplicitIdTableOperation.h"

// _____________________________________________________________________________
ExplicitIdTableOperation::ExplicitIdTableOperation(
    QueryExecutionContext* ctx, IdTableOrView table,
    VariableToColumnMap variables, std::vector<ColumnIndex> sortedColumns,
    LocalVocab localVocab, std::string cacheKey)
    : Operation(ctx),
      idTable_(std::move(table)),
      view_(viewOf(idTable_)),
      variables_(std::move(variables)),
      sortedColumns_(std::move(sortedColumns)),
      localVocab_(std::move(localVocab)),
      cacheKey_(std::move(cacheKey)) {
  // An explicit IdTable operation is never stored in the cache because it is
  // mostly used to implement already cached results (the `shared_ptr<IdTable>`
  // typically originates from a cache). However, the results of operations
  // using an `ExplicitIdTableOperation` may still be cached.
  disableStoringInCache();
}

// _____________________________________________________________________________
IdTableView<0> ExplicitIdTableOperation::viewOf(const IdTableOrView& table) {
  return std::visit(
      [](const auto& arg) -> IdTableView<0> {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::shared_ptr<const IdTable>>) {
          AD_CONTRACT_CHECK(arg != nullptr);
          return arg->template asStaticView<0>();
        } else {
          static_assert(std::is_same_v<T, IdTableView<0>>);
          return arg;
        }
      },
      table);
}

// _____________________________________________________________________________
Result ExplicitIdTableOperation::computeResult(
    [[maybe_unused]] bool requestLaziness) {
  // Pass the `shared_ptr<const IdTable>` alternative through to `Result`
  // as-is (instead of converting it to a view) so that `Result` keeps its own
  // independent claim on the underlying `IdTable` via reference counting,
  // rather than depending on this `ExplicitIdTableOperation` to stay alive.
  // The `IdTableView<0>` alternative is passed through unchanged; its
  // lifetime is guaranteed by whoever created the view (e.g. the `Qlever`
  // instance that owns the deserialized blob).
  return std::visit(
      [this](const auto& arg) -> Result {
        return {arg, resultSortedOn(), localVocab_.clone()};
      },
      idTable_);
}

// _____________________________________________________________________________
std::vector<QueryExecutionTree*> ExplicitIdTableOperation::getChildren() {
  return {};
}

// _____________________________________________________________________________
std::string ExplicitIdTableOperation::getCacheKeyImpl() const {
  return cacheKey_;
}

// _____________________________________________________________________________
std::string ExplicitIdTableOperation::getDescriptor() const {
  return "Explicit Result";
}

// _____________________________________________________________________________
size_t ExplicitIdTableOperation::getResultWidth() const {
  return idTableView().numColumns();
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
bool ExplicitIdTableOperation::knownEmptyResult() {
  return idTableView().empty();
}

// _____________________________________________________________________________
std::unique_ptr<Operation> ExplicitIdTableOperation::cloneImpl() const {
  return std::make_unique<ExplicitIdTableOperation>(
      getExecutionContext(), idTable_, variables_, sortedColumns_,
      localVocab_.clone(), cacheKey_);
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
