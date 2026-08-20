// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H
#define QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H

#include <variant>

#include "engine/Operation.h"
#include "engine/QueryExecutionContext.h"
#include "engine/Result.h"

// An operation that owns its explicit `Result` via `shared_ptr`s (or a
// non-owning view) and just returns this result when `computeResult` is
// called.
class ExplicitIdTableOperation : public Operation {
 public:
  // The underlying table is either an owning `shared_ptr<const IdTable>`
  // (e.g. coming from a cache), or a non-owning `IdTableView<0>` (e.g. coming
  // from a zero-copy deserialized blob). In the latter case, the caller is
  // responsible for ensuring that the underlying data outlives this
  // `ExplicitIdTableOperation` and any `Result` computed from it.
  using IdTableOrView =
      std::variant<std::shared_ptr<const IdTable>, IdTableView<0>>;

 private:
  IdTableOrView idTable_;
  // A non-owning view of `idTable_`, computed once in the constructor and
  // reused by `idTableView()`. This avoids rebuilding an `IdTableView<0>`
  // (which copies the small but non-trivial `ViewSpans` of column pointers)
  // on every call. This caching is safe because `idTable_` is never
  // reassigned after construction: in the owning `shared_ptr<const IdTable>`
  // alternative, the pointee is `const`, so it cannot be mutated (via this
  // class) after `view_` has been computed from it.
  IdTableView<0> view_;
  VariableToColumnMap variables_;
  std::vector<ColumnIndex> sortedColumns_;
  LocalVocab localVocab_;
  std::string cacheKey_;

 public:
  ExplicitIdTableOperation(QueryExecutionContext* ctx, IdTableOrView table,
                           VariableToColumnMap variables,
                           std::vector<ColumnIndex> sortedColumns,
                           LocalVocab localVocab, std::string cacheKey);

  // Return a non-owning view of `table`, regardless of which alternative of
  // `IdTableOrView` is currently active. Also useful for other holders of an
  // `IdTableOrView` (e.g. `NamedResultCache::Value`).
  static IdTableView<0> viewOf(const IdTableOrView& table);

  // Return the cached non-owning view of the underlying table (see `view_`
  // above), regardless of which alternative of `IdTableOrView` is currently
  // active.
  const IdTableView<0>& idTableView() const { return view_; }

  // Const and public getter for testing.
  size_t sizeEstimate() const { return idTableView().numRows(); }

  // Overridden methods from the `Operation` base class.
  std::vector<QueryExecutionTree*> getChildren() override;
  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;
  uint64_t getSizeEstimateBeforeLimit() override;
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;
  std::unique_ptr<Operation> cloneImpl() const override;
  std::vector<ColumnIndex> resultSortedOn() const override;
  VariableToColumnMap computeVariableToColumnMap() const override;
  Result computeResult(bool requestLaziness) override;

 private:
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }
};

#endif  // QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H
