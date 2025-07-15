// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_SORT_H
#define QLEVER_SRC_ENGINE_SORT_H

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

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

  virtual vector<ColumnIndex> resultSortedOn() const override {
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

  vector<QueryExecutionTree*> getChildren() override {
    return {subtree_.get()};
  }

  std::optional<std::shared_ptr<QueryExecutionTree>> makeSortedTree(
      const vector<ColumnIndex>& sortColumns) const override;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  virtual Result computeResult([[maybe_unused]] bool requestLaziness) override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap()
      const override {
    return subtree_->getVariableColumns();
  }

  std::string getCacheKeyImpl() const override;
};

#endif  // QLEVER_SRC_ENGINE_SORT_H
