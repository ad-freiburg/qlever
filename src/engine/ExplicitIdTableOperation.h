// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//                  Chair of Algorithms and Data Structures.

#ifndef QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H
#define QLEVER_SRC_ENGINE_EXPLICITIDTABLEOPERATION_H

#include <optional>
#include <utility>
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
  //
  // This class also caches a non-owning `IdTableView<0>` alongside the
  // variant, computed lazily on the first call to `view()`, so that `view()`
  // and `viewOf()` can return a stable reference instead of recomputing the
  // view on every call (see `Result::IdTableSharedLocalVocabPair` for a
  // similar pattern). Also useful for other holders of a table that is either
  // owned or a non-owning view (e.g. `NamedResultCache::Value`).
  class IdTableOrView {
   public:
    using Table = std::variant<std::shared_ptr<const IdTable>, IdTableView<0>>;

   private:
    Table table_;
    mutable std::optional<IdTableView<0>> view_;

    static IdTableView<0> makeView(const Table& table);

   public:
    // Default-constructs to a null `shared_ptr<const IdTable>`, which must be
    // overwritten (e.g. by `readFromSerializer`) before `view()` is called.
    IdTableOrView() = default;

    // Implicit on purpose: construct directly from anything that can
    // construct `Table` (e.g. `std::shared_ptr<IdTable>`,
    // `std::shared_ptr<const IdTable>`, or `IdTableView<0>`), mirroring the
    // ergonomics of a plain `Table` variant. Gating on
    // `std::is_constructible_v<Table, T&&>` (rather than declaring one
    // constructor per alternative) lets `Table`'s own converting constructor
    // handle conversions such as `shared_ptr<IdTable>` to
    // `shared_ptr<const IdTable>`, which would otherwise require two
    // user-defined conversions and thus not happen implicitly.
    CPP_template(typename T)(requires std::is_constructible_v<Table, T&&>)
        IdTableOrView(T&& table)
        : table_(std::forward<T>(table)) {}

    const Table& table() const { return table_; }

    // The returned reference is stable for the lifetime of this object.
    const IdTableView<0>& view() const {
      if (!view_.has_value()) {
        view_ = makeView(table_);
      }
      return view_.value();
    }
  };

 private:
  IdTableOrView idTable_;
  VariableToColumnMap variables_;
  std::vector<ColumnIndex> sortedColumns_;
  LocalVocab localVocab_;
  std::string cacheKey_;

 public:
  ExplicitIdTableOperation(QueryExecutionContext* ctx, IdTableOrView table,
                           VariableToColumnMap variables,
                           std::vector<ColumnIndex> sortedColumns,
                           LocalVocab localVocab, std::string cacheKey);

  // Return a non-owning, stable view of `table`, regardless of which
  // alternative is currently active.
  static const IdTableView<0>& viewOf(const IdTableOrView& table) {
    return table.view();
  }

  // Return a non-owning, stable view of the underlying table, regardless of
  // which alternative is currently active.
  const IdTableView<0>& idTableView() const { return viewOf(idTable_); }

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
