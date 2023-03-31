// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

class Minus : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  vector<float> _multiplicities;
  std::vector<std::array<size_t, 2>> _matchedColumns;

  enum class RowComparison { EQUAL, LEFT_SMALLER, RIGHT_SMALLER };

 public:
  Minus(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> left,
        std::shared_ptr<QueryExecutionTree> right,
        std::vector<std::array<size_t, 2>> matchedColumns);

  // Uninitialized Object for testing the computeMinus method
  struct OnlyForTestingTag {};
  explicit Minus(OnlyForTestingTag){};

 protected:
  string asStringImpl(size_t indent) const override;

 public:
  string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<size_t> resultSortedOn() const override;

  void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  bool knownEmptyResult() override { return _left->knownEmptyResult(); }

  float getMultiplicity(size_t col) override;

  size_t getSizeEstimate() override;

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
                    const vector<std::array<size_t, 2>>& matchedColumns,
                    IdTable* result) const;

 private:
  /**
   * @brief Compares the two rows under the assumption that the first
   * entries of the rows are equal.
   */
  template <int A_WIDTH, int B_WIDTH>
  static RowComparison isRowEqSkipFirst(
      const IdTableView<A_WIDTH>& a, const IdTableView<B_WIDTH>& b, size_t ia,
      size_t ib, const vector<std::array<size_t, 2>>& matchedColumns);

  ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
