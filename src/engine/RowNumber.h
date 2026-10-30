// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_ROWNUMBER_H
#define QLEVER_SRC_ENGINE_ROWNUMBER_H

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "engine/Operation.h"
#include "rdfTypes/Variable.h"
#include "util/ContainersWithAllocator.h"

// An operation that prepends a column with the consecutive row numbers
// `offset, offset + 1, ...` to the result of its child. It is created via the
// `RowNumberQuery` magic SERVICE.
//
// NOTE: The row numbers are unique, so the result is sorted by the row number
// column followed by any other columns. By default, these are the sort columns
// of the child, but `makeSortedTree` provides any other order that starts with
// the row number column for free. The row number column is the first column,
// such that joins on the row number and further variables (which choose their
// join columns in the order of the column indices) require no additional
// sorting.
class RowNumber : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> child_;
  Variable rowNumberVariable_;
  int64_t offset_;
  std::vector<ColumnIndex> sortedOn_;

 public:
  // Construct with the given `sortedOn` columns, which must start with the row
  // number column. If `sortedOn` is empty, the result is sorted by the row
  // number column followed by the sort columns of the child.
  RowNumber(QueryExecutionContext* qec,
            std::shared_ptr<QueryExecutionTree> child,
            Variable rowNumberVariable, int64_t offset = 0,
            std::vector<ColumnIndex> sortedOn = {});

  // Member functions inherited from `Operation` that have to be implemented by
  // each child class.
  std::string getCacheKeyImpl() const override;
  std::string getDescriptor() const override;
  size_t getResultWidth() const override;
  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  float getMultiplicity(size_t col) override;
  bool knownEmptyResult() override;

  // Return a copy of this operation that is sorted by `sortColumns` if they
  // start with the row number column, `std::nullopt` otherwise.
  std::optional<std::shared_ptr<QueryExecutionTree>> makeSortedTree(
      const std::vector<ColumnIndex>& sortColumns) const override;

 private:
  qlm::vector<QueryExecutionTree*> getChildrenImpl() const override;
  [[nodiscard]] bool isDeterministicImpl() const override { return true; }
  std::unique_ptr<Operation> cloneImpl() const override;
  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override;
  Result computeResult(bool requestLaziness) override;
  VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_SRC_ENGINE_ROWNUMBER_H
