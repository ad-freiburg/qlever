// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

class MultiColumnJoin : public Operation {
 public:
  // TODO<joka921> Make ColumnIndex a strong type across QLever
  using ColumnIndex = uint64_t;
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

  ad_utility::HashMap<string, size_t> getVariableColumns() const override;

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
  void computeSizeEstimateAndMultiplicities();

  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  std::vector<std::array<ColumnIndex, 2>> _joinColumns;

  vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed;

  virtual void computeResult(ResultTable* result) override;
};

template <int A_WIDTH, int B_WIDTH, int OUT_WIDTH>
void MultiColumnJoin::computeMultiColumnJoin(
    const IdTable& dynA, const IdTable& dynB,
    const vector<array<ColumnIndex, 2>>& joinColumns, IdTable* dynResult) {
  // check for trivial cases
  if (dynA.size() == 0 || dynB.size() == 0) {
    return;
  }

  // Marks the columns in b that are join columns. Used to skip these
  // when computing the result of the join
  int joinColumnBitmap_b = 0;
  for (const array<ColumnIndex, 2>& jc : joinColumns) {
    joinColumnBitmap_b |= (1 << jc[1]);
  }

  IdTableView<A_WIDTH> a = dynA.asStaticView<A_WIDTH>();
  IdTableView<B_WIDTH> b = dynB.asStaticView<B_WIDTH>();
  IdTableStatic<OUT_WIDTH> result = dynResult->moveToStatic<OUT_WIDTH>();

  bool matched = false;
  size_t ia = 0, ib = 0;
  while (ia < a.size() && ib < b.size()) {
    // Join columns 0 are the primary sort columns
    while (a(ia, joinColumns[0][0]) < b(ib, joinColumns[0][1])) {
      ia++;
      if (ia >= a.size()) {
        goto finish;
      }
    }
    while (b(ib, joinColumns[0][1]) < a(ia, joinColumns[0][0])) {
      ib++;
      if (ib >= b.size()) {
        goto finish;
      }
    }

    // check if the rest of the join columns also match
    matched = true;
    for (size_t joinColIndex = 0; joinColIndex < joinColumns.size();
         joinColIndex++) {
      const array<ColumnIndex, 2>& joinColumn = joinColumns[joinColIndex];
      if (a(ia, joinColumn[0]) < b(ib, joinColumn[1])) {
        ia++;
        matched = false;
        break;
      }
      if (b(ib, joinColumn[1]) < a(ia, joinColumn[0])) {
        ib++;
        matched = false;
        break;
      }
    }

    // Compute the cross product of the row in a and all matching
    // rows in b.
    while (matched && ia < a.size() && ib < b.size()) {
      // used to reset ib if another cross product needs to be computed
      size_t initIb = ib;

      while (matched) {
        result.emplace_back();
        size_t backIdx = result.size() - 1;

        // fill the result
        size_t rIndex = 0;
        for (size_t col = 0; col < a.cols(); col++) {
          result(backIdx, rIndex) = a(ia, col);
          rIndex++;
        }
        for (size_t col = 0; col < b.cols(); col++) {
          if ((joinColumnBitmap_b & (1 << col)) == 0) {
            result(backIdx, rIndex) = b(ib, col);
            rIndex++;
          }
        }

        ib++;

        // do the rows still match?
        for (const array<ColumnIndex, 2>& jc : joinColumns) {
          if (ib >= b.size() || a(ia, jc[0]) != b(ib, jc[1])) {
            matched = false;
            break;
          }
        }
      }
      ia++;
      // Check if the next row in a also matches the initial row in b
      matched = true;
      for (const array<ColumnIndex, 2>& jc : joinColumns) {
        if (ia >= a.size() || a(ia, jc[0]) != b(initIb, jc[1])) {
          matched = false;
          break;
        }
      }
      // If they match reset ib and compute another cross product
      if (matched) {
        ib = initIb;
      }
    }
  }
finish:
  *dynResult = result.moveToDynamic();
}
