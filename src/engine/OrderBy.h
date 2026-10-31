// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_ORDERBY_H
#define QLEVER_SRC_ENGINE_ORDERBY_H

#include <optional>
#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "util/ContainersWithAllocator.h"

// The implementation of the SPARQL `ORDER BY` operation.
//
// Note: This class sorts its input in the way that is expected by an end user
// e.g. `-3 < 0` etc. This is different from the internal order of the IDs
// which is cheaper to compute and used to compute efficient JOIN`s etc. The
// internal ordering is computed by the `Sort` operation in `Sort.h`. It is thus
// important to use the `OrderBy` operation only as the last step during query
// processing directly before exporting the result.
class OrderBy : public Operation {
 public:
  // TODO<joka921> This should be `pair<ColumnIndex, IsAscending>`
  // The bool means "isDescending"
  using SortIndices = std::vector<std::pair<ColumnIndex, bool>>;

 private:
  std::shared_ptr<QueryExecutionTree> subtree_;
  SortIndices sortIndices_;

 public:
  OrderBy(QueryExecutionContext* qec,
          std::shared_ptr<QueryExecutionTree> subtree, SortIndices sortIndices);

 protected:
  std::string getCacheKeyImpl() const override;

 public:
  std::string getDescriptor() const override;

  // The function `resultSortedOn` refers to the `internal` sorting by ID value.
  // This is different from the `semantic` sorting that the ORDER BY operation
  // computes.
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }

  // Expose the variables on which this OrderBy is performed. Currently mostly
  // used for testing.
  enum class AscOrDesc { Asc, Desc };
  using SortedVariables = std::vector<std::pair<Variable, AscOrDesc>>;
  SortedVariables getSortedVariables() const;

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    return subtree_->getSizeEstimate();
  }

 public:
  float getMultiplicity(size_t col) override {
    return subtree_->getMultiplicity(col);
  }

  // The cost is `n log n` for the sort, or linear if there is a single sort
  // column and the input is already sorted by it (see
  // `computeResultForSortedInput`).
  size_t getCostEstimate() override;

  // `ORDER BY` handles `LIMIT` and `OFFSET` itself if the input is sorted by
  // the single sort column: `computeResultForSortedInput` then copies only the
  // requested rows, and `onLimitOffsetChanged` restricts an `IndexScan` below
  // to the blocks that can contain them. Otherwise the limit is applied
  // externally on the completely sorted result, which the cache can then
  // reuse for other limits.
  [[nodiscard]] LimitOffsetHandling handlesLimitOffset() const override;

  bool knownEmptyResult() override { return subtree_->knownEmptyResult(); }

  size_t getResultWidth() const override;

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& requestedVariables) const override;

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override {
    return {{subtree_.get()}, allocator()};
  }

 private:
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }

  std::unique_ptr<Operation> cloneImpl() const override;

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  // Return true iff there is a single sort column and the subtree's result is
  // sorted by it (in the internal order of the `Id`s). This is the part of the
  // precondition of the fast path below that is known at planning time.
  bool hasSingleSortColumnWithSortedInput() const;

  // Fast path for a single sort column, where the `input` is already sorted by
  // that column in the internal order and the column contains only integers or
  // only doubles (see `OrderByOnSortedNumericInput.h`). Return `std::nullopt`
  // if the fast path does not apply.
  std::optional<IdTable> computeResultForSortedInput(
      const IdTableView<0>& input) const;

  // If the input is an `IndexScan` and the fast path above applies according
  // to the block metadata of the scan, replace the scan by a copy that only
  // reads the blocks which can contain the rows selected by the
  // `LIMIT`/`OFFSET` (see `orderByOnSortedNumericInput::selectBlocksForLimit`).
  // The scan is replaced, not modified, so it may be shared.
  void onLimitOffsetChanged(const LimitOffsetClause& limitOffset,
                            bool childrenAreExclusivelyOwned) override;

  // Set if the `LIMIT`/`OFFSET` was pushed into an `IndexScan`. Only used for
  // the runtime information.
  struct NumBlocksReadForLimit {
    size_t numBlocksRead_;
    size_t numBlocksTotal_;
  };
  std::optional<NumBlocksReadForLimit> blocksReadForLimit_;

  VariableToColumnMap computeVariableToColumnMap() const override {
    return subtree_->getVariableColumns();
  }
};

#endif  // QLEVER_SRC_ENGINE_ORDERBY_H
