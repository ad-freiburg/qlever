// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#pragma once

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

  // The constructor is private. The only way to create a `Sort` operation is
  // via `ad_utility::createSortedTree` (in `QueryExecutionTree.h`). The reason
  // for this is that the `createdSortedTree` function has an optimization if
  // the argument is already (implicitly) sorted, but has to perform additional
  // operations on the argument if this optimization applies. So calling the
  // constructor of `Sort` without going through `createSortedTree` would very
  // likely be a bug.
  Sort(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
       std::vector<ColumnIndex> sortColumnIndices);

  // The actual way to create a `Sort` operation. Declared and defined in
  // `QueryExecutionTree.h/.cpp`.
  friend std::shared_ptr<QueryExecutionTree> ad_utility::createSortedTree(
      std::shared_ptr<QueryExecutionTree> qet,
      const vector<size_t>& sortColumns);

 public:
  string getDescriptor() const override;

  vector<size_t> resultSortedOn() const override { return sortColumnIndices_; }

  void setTextLimit(size_t limit) override { subtree_->setTextLimit(limit); }

 private:
  size_t getSizeEstimateBeforeLimit() override {
    return subtree_->getSizeEstimate();
  }

 public:
  float getMultiplicity(size_t col) override {
    return subtree_->getMultiplicity(col);
  }

  std::shared_ptr<QueryExecutionTree> getSubtree() const { return subtree_; }

  size_t getCostEstimate() override {
    size_t size = getSizeEstimateBeforeLimit();
    size_t logSize = std::max(
        size_t(2), static_cast<size_t>(logb(static_cast<double>(size))));
    size_t nlogn = size * logSize;
    size_t subcost = subtree_->getCostEstimate();
    return nlogn + subcost;
  }

  bool knownEmptyResult() override { return subtree_->knownEmptyResult(); }

  [[nodiscard]] size_t getResultWidth() const override;

  vector<QueryExecutionTree*> getChildren() override {
    return {subtree_.get()};
  }

 private:
  ResultTable computeResult() override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap()
      const override {
    return subtree_->getVariableColumns();
  }

  string asStringImpl(size_t indent = 0) const override;
};
