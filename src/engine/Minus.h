// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_MINUS_H
#define QLEVER_SRC_ENGINE_MINUS_H

#include <array>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

class Minus : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  std::vector<float> _multiplicities;
  std::vector<std::array<ColumnIndex, 2>> _matchedColumns;

  enum class RowComparison { EQUAL, LEFT_SMALLER, RIGHT_SMALLER };

 public:
  Minus(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> left,
        std::shared_ptr<QueryExecutionTree> right);

 protected:
  std::string getCacheKeyImpl() const override;

 public:
  std::string getDescriptor() const override;

  size_t getResultWidth() const override;

  std::vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override { return _left->knownEmptyResult(); }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

  // Create a variant of functions that find undefined values in a range. In
  // case the selected operation (via `left`) might conceptually contain
  // undefined values all values are checked for undef values. If undef values
  // are found an instance of `ad_utility::FindSmallerUndefRanges` will be
  // returned, otherwise the function will be replaced with `ad_utility::Noop`,
  // each wrapped in a `std::variant` respectively.
  // `auto` is used instead of
  // `std::variant<ad_utility::Noop, ad_utility::FindSmallerUndefRanges>` to
  // avoid including expensive headers that are only relevant for the
  // implementation of this function.
  auto makeUndefRangesChecker(bool left, const IdTable& idTable) const;

  // Helper function to copy all rows from `left` that have a corresponding
  // value of `reference` in `keepEntry`.
  template <typename T>
  IdTable copyMatchingRows(
      const IdTable& left, T reference,
      const std::vector<T, ad_utility::AllocatorWithLimit<T>>& keepEntry) const;

 public:
  size_t getCostEstimate() override;

  std::vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  bool columnOriginatesFromGraphOrUndef(
      const Variable& variable) const override;

  /**
   * @brief Joins a and b using the column defined int joinColumns, storing the
   *        result in result. R should have width resultWidth (or be a vector
   *        that should have resultWidth entries).
   *        This method is made public here for unit testing purposes.
   **/
  IdTable computeMinus(
      const IdTable& a, const IdTable& b,
      const std::vector<std::array<ColumnIndex, 2>>& matchedColumns) const;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  // Nested loop join optimization than can apply when a memory intensive sort
  // can be avoided this way.
  std::optional<Result> tryIndexNestedLoopJoinIfSuitable();

  // Lazily compute the minus join of two results when at least one of the
  // results is computed lazily. This currently only works if we have just a
  // single join column, otherwise this function will throw.
  Result lazyMinusJoin(std::shared_ptr<const Result> left,
                       std::shared_ptr<const Result> right,
                       bool requestLaziness);

  Result computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  std::optional<std::shared_ptr<QueryExecutionTree>>
  makeTreeWithStrippedColumns(
      const std::set<Variable>& variables) const override;
};

#endif  // QLEVER_SRC_ENGINE_MINUS_H
