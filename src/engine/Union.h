// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "util/HashMap.h"

class Union : public Operation {
 private:
  /**
   * @brief This stores the input column from each of the two subtrees or
   * NO_COLUMN if the subtree does not have a matching column for each result
   * column.
   */
  std::vector<std::array<size_t, 2>> _columnOrigins;
  std::array<std::shared_ptr<QueryExecutionTree>, 2> _subtrees;
  // Stores the indices of the columns that the result of this operation should
  // be sorted on. If set, the expensive union with merge implementation has to
  // be used (which is most likely cheaper than sorting afterwards).
  std::vector<ColumnIndex> targetOrder_;

 public:
  Union(QueryExecutionContext* qec,
        const std::shared_ptr<QueryExecutionTree>& t1,
        const std::shared_ptr<QueryExecutionTree>& t2,
        std::vector<ColumnIndex> targetOrder = {});

 protected:
  virtual string getCacheKeyImpl() const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<ColumnIndex> resultSortedOn() const override;

  virtual bool knownEmptyResult() override;

  virtual float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  const static size_t NO_COLUMN;

  static constexpr size_t chunkSize = 1'000'000;

  // The method is declared here to make it unit testable
  IdTable computeUnion(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<size_t, 2>>& columnOrigins) const;

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtrees[0].get(), _subtrees[1].get()};
  }

  // Create a sorted variant of this operation. This can be more efficient than
  // stacking a `Sort` operation on top of this one because Union can simply
  // push the sort down to its children. If one of the children is already
  // sorted properly then it is way cheaper to sort the other child and then
  // merge the two sorted results.
  std::shared_ptr<Operation> createSortedVariant(
      const vector<ColumnIndex>& sortColumns) const;

  // Provide access the the left child of this union.
  const std::shared_ptr<QueryExecutionTree>& leftChild() const {
    return _subtrees[0];
  }

  // Provide access the the right child of this union.
  const std::shared_ptr<QueryExecutionTree>& rightChild() const {
    return _subtrees[1];
  }

  // Return the original index of the column in the left or right child that the
  // respective column of this union maps to. If the index does not map to the
  // respective child, std::nullopt is returned.
  std::optional<ColumnIndex> getOriginalColumn(bool leftChild,
                                               ColumnIndex unionColumn) const;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  ProtoResult computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  // Compute the permutation of the `IdTable` being yielded for the left or
  // right child depending on `left`. This permutation can then be used to swap
  // the columns without any copy operations.
  template <bool left>
  std::vector<ColumnIndex> computePermutation() const;

  // Take the given `IdTable`, add any missing columns to it (filled with
  // undefined values) and permutate the columns to match the end result.
  IdTable transformToCorrectColumnFormat(
      IdTable idTable, const std::vector<ColumnIndex>& permutation) const;

  // Create a generator that yields the `IdTable` for the left or right child
  // one after another and apply a potential differing permutation to it. Write
  // the merged LocalVocab to the given `LocalVocab` object at the end.
  Result::Generator computeResultLazily(
      std::shared_ptr<const Result> result1,
      std::shared_ptr<const Result> result2) const;

  // Similar to `computeResultLazily` but it keeps the order of the results.
  // This means that instead of just returning the results of the left and right
  // child one after another, the results are merged in a way that the order of
  // the results is preserved.
  Result::LazyResult computeResultKeepOrder(
      bool requestLaziness, std::shared_ptr<const Result> result1,
      std::shared_ptr<const Result> result2) const;
};
