// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

class Distinct : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> subtree_;
  std::vector<ColumnIndex> _keepIndices;

 public:
  Distinct(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           const std::vector<ColumnIndex>& keepIndices);

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override {
    return subtree_->resultSortedOn();
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    return subtree_->getSizeEstimate();
  }

 public:
  size_t getCostEstimate() override {
    return getSizeEstimateBeforeLimit() + subtree_->getCostEstimate();
  }

  float getMultiplicity(size_t col) override {
    return subtree_->getMultiplicity(col);
  }

  bool knownEmptyResult() override { return subtree_->knownEmptyResult(); }

  std::vector<QueryExecutionTree*> getChildren() override {
    return {subtree_.get()};
  }

 protected:
  [[nodiscard]] string getCacheKeyImpl() const override;

 private:
  ProtoResult computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  template <size_t WIDTH>
  static cppcoro::generator<IdTable> lazyDistinct(
      cppcoro::generator<IdTable> originalGenerator,
      std::vector<ColumnIndex> keepIndices,
      std::optional<IdTable> aggregateTable);

  // Removes all duplicates from input with regards to the columns
  // in keepIndices. The input needs to be sorted on the keep indices,
  // otherwise the result of this function is undefined.
  template <size_t WIDTH>
  static IdTable distinct(
      IdTable dynInput, const std::vector<ColumnIndex>& keepIndices,
      std::optional<typename IdTableStatic<WIDTH>::row_type> previousRow);

  FRIEND_TEST(Distinct, distinct);
  FRIEND_TEST(Distinct, distinctWithEmptyInput);
};
