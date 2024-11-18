//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

// An operation that takes a set of subresults that pairwise-disjoint sets of
// bound variables and materializes the full Cartesian product of these
// operations.
class CartesianProductJoin : public Operation {
 public:
  using Children = std::vector<std::shared_ptr<QueryExecutionTree>>;

 private:
  Children children_;
  size_t chunkSize_;

  // Access to the actual operations of the children.
  // TODO<joka921> We can move this whole children management into a base class
  // and clean up the implementation of several other children.
  auto childView() {
    return std::views::transform(children_, [](auto& child) -> Operation& {
      return *child->getRootOperation();
    });
  }
  auto childView() const {
    return std::views::transform(children_,
                                 [](auto& child) -> const Operation& {
                                   return *child->getRootOperation();
                                 });
  }

 public:
  // Constructor. `children` must not be empty and the variables of all the
  // children must be disjoint, else an `AD_CONTRACT_CHECK` fails. Accept a
  // custom `chunkSize` for chunking lazy results.
  explicit CartesianProductJoin(QueryExecutionContext* executionContext,
                                Children children,
                                size_t chunkSize = 1'000'000);

  /// get non-owning pointers to all the held subtrees to actually use the
  /// Execution Trees as trees
  std::vector<QueryExecutionTree*> getChildren() override;

 private:
  // The individual implementation of `getCacheKey` (see above) that has to be
  // customized by every child class.
  string getCacheKeyImpl() const override;

 public:
  // Gets a very short (one line without line ending) descriptor string for
  // this Operation.  This string is used in the RuntimeInformation
  string getDescriptor() const override { return "Cartesian Product Join"; }
  size_t getResultWidth() const override;

  size_t getCostEstimate() override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

  VariableToColumnMap computeVariableToColumnMap() const override;

 public:
  float getMultiplicity([[maybe_unused]] size_t col) override;

  bool knownEmptyResult() override;

  // The Cartesian product join can efficiently evaluate a limited result.
  [[nodiscard]] bool supportsLimit() const override { return true; }

 protected:
  // Don't promise any sorting of the result.
  // TODO<joka921> Depending on the implementation we could propagate sorted
  // columns from either the first or the last input, but it is questionable if
  // there would be any real benefit from this and it would only increase the
  // complexity of the query planning and required testing.
  vector<ColumnIndex> resultSortedOn() const override { return {}; }

 private:
  //! Compute the result of the query-subtree rooted at this element..
  ProtoResult computeResult(bool requestLaziness) override;

  // Copy each element from the `inputColumn` `groupSize` times to the
  // `targetColumn`. Repeat until the `targetColumn` is completely filled. Skip
  // the first `offset` write operations to the `targetColumn`. Call
  // `checkCancellation` after each write.
  void writeResultColumn(std::span<Id> targetColumn,
                         std::span<const Id> inputColumn, size_t groupSize,
                         size_t offset) const;

  // Write all columns of the subresults into an `IdTable` and return it.
  // `offset` indicates how many rows to skip in the result and `limit` how many
  // rows to write at most. `lastTableOffset` is the offset of the last table,
  // to account for cases where the last table does not cover the whole result
  // and so index 0 of a table does not correspond to row 0 of the result.
  IdTable writeAllColumns(std::ranges::random_access_range auto idTables,
                          size_t offset, size_t limit,
                          size_t lastTableOffset = 0) const;

  // Calculate the subresults of the children and store them into a vector. If
  // the rightmost child can produce a lazy result, it will be stored outside of
  // the vector and returned as the first element of the pair. Otherwise this
  // will be an empty shared_ptr. The vector is guaranteed to only contain fully
  // materialized results.
  std::pair<std::vector<std::shared_ptr<const Result>>,
            std::shared_ptr<const Result>>
  calculateSubResults(bool requestLaziness);

  // Take a range of `IdTable`s and a corresponding `LocalVocab` and yield
  // `IdTable`s with sizes up to `chunkSize_` until the limit is reached.
  // `offset` indicates the total offset of the desired result.
  // `limit` is the maximum number of rows to yield.
  // `lastTableOffset` is the offset of the last table in the range. This is
  // used to handle `IdTable`s yielded by generators where the range of indices
  // they represent do not cover the whole result.
  Result::Generator produceTablesLazily(LocalVocab mergedVocab,
                                        std::ranges::range auto idTables,
                                        size_t offset, size_t limit,
                                        size_t lastTableOffset = 0) const;

  // Similar to `produceTablesLazily` but can handle a single lazy result.
  Result::Generator createLazyConsumer(
      LocalVocab staticMergedVocab,
      std::vector<std::shared_ptr<const Result>> subresults,
      std::shared_ptr<const Result> lazyResult) const;
};
