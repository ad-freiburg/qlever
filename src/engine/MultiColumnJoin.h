// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"
#include "util/JoinAlgorithms.h"

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

  auto lessThanBoth = ad_utility::makeLessThanAndReversed(joinColumns);

  std::vector<size_t> joinColumnsLeft;
  std::vector<size_t> joinColumnsRight;
  for (auto [jc1, jc2] : joinColumns) {
    joinColumnsLeft.push_back(jc1);
    joinColumnsRight.push_back(jc2);
  }

  using RowLeft = typename IdTableView<A_WIDTH>::row_type;
  using RowRight = typename IdTableView<B_WIDTH>::row_type;
  auto smallerUndefRanges =
      ad_utility::makeSmallerUndefRanges<RowLeft, RowRight>(
          joinColumns, joinColumnsLeft, joinColumnsRight);
  // Marks the columns in b that are join columns. Used to skip these
  // when computing the result of the join

  auto combineRows = ad_utility::makeCombineRows(joinColumns, result);

  ad_utility::zipperJoinWithUndef(a, b, lessThanBoth.first, lessThanBoth.second,
                                  combineRows, smallerUndefRanges.first,
                                  smallerUndefRanges.second);
  *dynResult = std::move(result).toDynamic();
}
