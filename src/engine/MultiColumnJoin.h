// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

class MultiColumnJoin : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  std::vector<std::array<ColumnIndex, 2>> _joinColumns;

  vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed;

 public:
  MultiColumnJoin(QueryExecutionContext* qec,
                  std::shared_ptr<QueryExecutionTree> t1,
                  std::shared_ptr<QueryExecutionTree> t2,
                  const std::vector<std::array<ColumnIndex, 2>>& joinCols);

 protected:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  /**
   * @brief Joins a and b using the column defined int joinColumns, storing the
   *        result in result. R should have width resultWidth (or be a vector
   *        that should have resultWidth entries).
   *        This method is made public here for unit testing purposes.
   **/
  template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
  static void computeMultiColumnJoin(
      const IdTable& a, const IdTable& b,
      const vector<array<ColumnIndex, 2>>& joinColumns, IdTable* result);

 private:
  virtual void computeResult(ResultTable* result) override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  void computeSizeEstimateAndMultiplicities();
};

template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void MultiColumnJoin::computeMultiColumnJoin(
    const IdTable& dynA, const IdTable& dynB,
    const vector<array<ColumnIndex, 2>>& joinColumns, IdTable* dynResult) {
  // check for trivial cases
  if (dynA.size() == 0 || dynB.size() == 0) {
    return;
  }

  IdTableView<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableView<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = std::move(*dynResult).toStatic<OUT_WIDTH>();

  auto lessThan = [&joinColumns](const auto& row1, const auto& row2) {
    for (const auto [jc1, jc2] : joinColumns) {
      if (row1[jc1] != row2[jc2]) {
        return row1[jc1] < row2[jc2];
      }
    }
    return false;
  };
  auto lessThanReversed = [&joinColumns](const auto& row1, const auto& row2) {
    for (const auto [jc1, jc2] : joinColumns) {
      if (row1[jc2] != row2[jc1]) {
        return row1[jc2] < row2[jc1];
      }
    }
    return false;
  };

  // Marks the columns in b that are join columns. Used to skip these
  // when computing the result of the join
  int joinColumnBitmap_b = 0;
  for (const array<ColumnIndex, 2>& jc : joinColumns) {
    joinColumnBitmap_b |= (1 << jc[1]);
  }
  auto combineRows = [&](const auto& row1, const auto& row2) {
    result.emplace_back();
    size_t backIdx = result.size() - 1;

    // fill the result
    size_t rIndex = 0;
    for (size_t col = 0; col < a.numColumns(); col++) {
      result(backIdx, rIndex) = row1[col];
      rIndex++;
    }
    for (size_t col = 0; col < b.numColumns(); col++) {
      if ((joinColumnBitmap_b & (1 << col)) == 0) {
        result(backIdx, rIndex) = row2[col];
        rIndex++;
      }
    }
  };

  ad_utility::zipperJoin(a, b, lessThan, lessThanReversed, combineRows);
  *dynResult = std::move(result).toDynamic();
}
