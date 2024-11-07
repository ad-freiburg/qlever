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

  struct IdTableWithMetadata {
    const IdTable& idTable_;
    size_t totalOffset_;
    size_t tableOffset_;
    size_t totalLimit_;
    bool done_ = false;
  };

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
  // children must be disjoint, else an `AD_CONTRACT_CHECK` fails.
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
  size_t writeResultColumn(std::span<Id> targetColumn,
                           std::span<const Id> inputColumn, size_t groupSize,
                           size_t offset) const;

  // Write all columns of the subresults into an `IdTable` and return it.
  IdTable writeAllColumns(std::ranges::range auto subResults,
                          IdTableWithMetadata* partialIdTable = nullptr) const;

  // Calculate the subresults of the children and store them into a vector.
  std::pair<std::shared_ptr<const Result>,
            std::vector<std::shared_ptr<const Result>>>
  calculateSubResults(bool requestLaziness);

  Result::Generator createLazyProducer(
      LocalVocab staticMergedVocab,
      std::vector<std::shared_ptr<const Result>> subresults,
      size_t offsetFromFront, std::optional<size_t> limit = std::nullopt,
      size_t* offsetPtr = nullptr) const;

  Result::Generator createLazyConsumer(
      LocalVocab staticMergedVocab, std::shared_ptr<const Result> lazyResult,
      std::vector<std::shared_ptr<const Result>> subresults,
      size_t offsetFromFront) const;
};
