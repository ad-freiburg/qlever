// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: 2015 - 2017 Björn Buchhold (buchhold@cs.uni-freiburg.de)
// Author: 2023 -      Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)
#pragma once

#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

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
  string asStringImpl(size_t indent = 0) const override;

 public:
  string getDescriptor() const override;

  // The function `resultSortedOn` refers to the `internal` sorting by ID value.
  // This is different from the `semantic` sorting that the ORDER BY operation
  // computes.
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }

  void setTextLimit(size_t limit) override { subtree_->setTextLimit(limit); }

 private:
  size_t getSizeEstimateBeforeLimit() override {
    return subtree_->getSizeEstimate();
  }

 public:
  float getMultiplicity(size_t col) override {
    return subtree_->getMultiplicity(col);
  }

  size_t getCostEstimate() override {
    size_t size = getSizeEstimateBeforeLimit();
    size_t logSize = std::max(
        size_t(1), static_cast<size_t>(logb(
                       static_cast<double>(getSizeEstimateBeforeLimit()))));
    size_t nlogn = size * logSize;
    size_t subcost = subtree_->getCostEstimate();
    return nlogn + subcost;
  }

  bool knownEmptyResult() override { return subtree_->knownEmptyResult(); }

  size_t getResultWidth() const override;

  vector<QueryExecutionTree*> getChildren() override {
    return {subtree_.get()};
  }

 private:
  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override {
    return subtree_->getVariableColumns();
  }
};
