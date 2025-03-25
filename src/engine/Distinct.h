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
  std::vector<ColumnIndex> keepIndices_;

  static constexpr int64_t CHUNK_SIZE = 100'000;

 public:
  Distinct(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           const std::vector<ColumnIndex>& keepIndices);

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] std::vector<ColumnIndex> resultSortedOn() const override {
    return subtree_->resultSortedOn();
  }

  // Get all columns that need to be distinct.
  const std::vector<ColumnIndex>& getDistinctColumns() const {
    return keepIndices_;
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
  std::unique_ptr<Operation> cloneImpl() const override;
  Result computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // Helper function that only compares rows on the columns in `keepIndices_`.
  bool matchesRow(const auto& a, const auto& b) const;

  // Return a generator that applies an in-place unique algorithm to the
  // `IdTables`s yielded by the input generator. The `yieldOnce` flag controls
  // if every `IdTable` from `input` should yield it's own `IdTable` or if all
  // of them should get aggregated into a single big `IdTable`.
  template <size_t WIDTH>
  Result::Generator lazyDistinct(Result::LazyResult input,
                                 bool yieldOnce) const;

  // Removes all duplicates from input with regards to the columns
  // in keepIndices. The input needs to be sorted on the keep indices,
  // otherwise the result of this function is undefined. The argument
  // `previousRow` might hold a row representing the last row of the previous
  // `IdTable`, so that the `IdTable` that will be returned doesn't return
  // values that were already returned in the previous `IdTable`.
  template <size_t WIDTH>
  IdTable distinct(
      IdTable dynInput,
      std::optional<typename IdTableStatic<WIDTH>::row_type> previousRow) const;

  // Out-of-place implementation of the unique algorithm. Does only copy values
  // if they're actually unique.
  template <size_t WIDTH>
  IdTable outOfPlaceDistinct(const IdTable& dynInput) const;

  FRIEND_TEST(Distinct, distinct);
  FRIEND_TEST(Distinct, distinctWithEmptyInput);
  FRIEND_TEST(Distinct, testChunkEdgeCases);
};
