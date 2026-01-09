// Copyright 2015 - 2026 The QLever Authors, in particular:
//
// 2015 - 2017 Björn Buchhold <buchhold@cs.uni-freiburg.de>, UFR
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SORT_H
#define QLEVER_SRC_ENGINE_SORT_H

#include "engine/LocalVocab.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"

// This operation sorts an `IdTable` by the `internal` order of the IDs. This
// order is cheap to compute (just a bitwise compare of integers), but is
// different from the `semantic` order that is computed by ORDER BY. For
// example, in the internal Order `Int(0) < Int(-3)`. For details on the
// different orderings see `ValueId.h` and `ValueIdComparators.h`. The `Sort`
// class has to be used when an operation requires a presorted input (e.g. JOIN,
// GROUP BY). To compute an `ORDER BY` clause at the end of the query
// processing, the `OrderBy` class from `OrderBy.h/.cpp` has to be used.
class Sort : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> subtree_;
  std::vector<ColumnIndex> sortColumnIndices_;

 public:
  Sort(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
       std::vector<ColumnIndex> sortColumnIndices);

 public:
  virtual std::string getDescriptor() const override;

  virtual std::vector<ColumnIndex> resultSortedOn() const override {
    return sortColumnIndices_;
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    return subtree_->getSizeEstimate();
  }

 public:
  virtual float getMultiplicity(size_t col) override {
    return subtree_->getMultiplicity(col);
  }

  virtual size_t getCostEstimate() override {
    size_t size = getSizeEstimateBeforeLimit();
    size_t logSize =
        size < 4 ? 2 : static_cast<size_t>(logb(static_cast<double>(size)));
    size_t nlogn = size * logSize;
    size_t subcost = subtree_->getCostEstimate();
    // Return  at least 1, s.t. the query planner will never emit an unnecessary
    // sort of an empty `IndexScan`. This makes the testing of the query
    // planner much easier.
    return std::max(1UL, nlogn + subcost);
  }

  virtual bool knownEmptyResult() override {
    return subtree_->knownEmptyResult();
  }

  [[nodiscard]] size_t getResultWidth() const override;

  std::vector<QueryExecutionTree*> getChildren() override {
    return {subtree_.get()};
  }

  std::optional<std::shared_ptr<QueryExecutionTree>> makeSortedTree(
      const std::vector<ColumnIndex>& sortColumns) const override;

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  virtual Result computeResult(bool requestLaziness) override;

  // Sort in memory, using `Engine::sort`.
  Result computeResultInMemory(IdTable idTable, LocalVocab localVocab) const;

  // Sort externally, using `CompressedExternalIdTableSorter`, using the value
  // of `sort-in-memory-threshold` as memory limit.
  //
  // The `collectedBlocks` are the blocks that have already been read from
  // `input` (until the `sort-in-memory-threshold` was exceeded),
  // `mergedLocalVocab` is the merged local vocabs for these blocks, and the
  // remaining blocks to be read are provided via `it` and `end`. The shared
  // pointer `input` is provided so that its resources can be freed once all
  // blocks have been pushed to the external sorter.
  //
  // NOTE: `Iterator` and `Sentinel` are separate template types because C++20
  // ranges (like `InputRangeFromGet`) use different types for begin and end.
  template <typename Iterator, typename Sentinel>
  Result computeResultExternal(std::vector<IdTable> collectedBlocks,
                               LocalVocab mergedLocalVocab, Iterator it,
                               Sentinel end,
                               std::shared_ptr<const Result> input,
                               bool requestLaziness) const;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap()
      const override {
    return subtree_->getVariableColumns();
  }

  std::string getCacheKeyImpl() const override;
};

#endif  // QLEVER_SRC_ENGINE_SORT_H
