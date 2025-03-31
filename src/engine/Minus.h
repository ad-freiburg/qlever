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

  vector<float> _multiplicities;
  std::vector<std::array<ColumnIndex, 2>> _matchedColumns;

  enum class RowComparison { EQUAL, LEFT_SMALLER, RIGHT_SMALLER };

 public:
  Minus(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> left,
        std::shared_ptr<QueryExecutionTree> right);

  // Uninitialized Object for testing the computeMinus method
  struct OnlyForTestingTag {};
  explicit Minus(OnlyForTestingTag) {}

 protected:
  string getCacheKeyImpl() const override;

 public:
  string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override { return _left->knownEmptyResult(); }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  /**
   * @brief Joins a and b using the column defined int joinColumns, storing the
   *        result in result. R should have width resultWidth (or be a vector
   *        that should have resultWidth entries).
   *        This method is made public here for unit testing purposes.
   **/
  template <int A_WIDTH, int B_WIDTH>
  void computeMinus(const IdTable& a, const IdTable& b,
                    const vector<std::array<ColumnIndex, 2>>& matchedColumns,
                    IdTable* result) const;

 private:
  std::unique_ptr<Operation> cloneImpl() const override;

  /**
   * @brief Compares the two rows under the assumption that the first
   * entries of the rows are equal.
   */
  template <int A_WIDTH, int B_WIDTH>
  static RowComparison isRowEqSkipFirst(
      const IdTableView<A_WIDTH>& a, const IdTableView<B_WIDTH>& b, size_t ia,
      size_t ib, const vector<std::array<ColumnIndex, 2>>& matchedColumns);

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_SRC_ENGINE_MINUS_H
