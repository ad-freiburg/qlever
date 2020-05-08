// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

class Minus : public Operation {
  enum class RowComparison {
    EQUAL,
    NOT_EQUAL_LEFT_SMALLER,
    NOT_EQUAL_RIGHT_SMALLER,
    DISJOINT_DOMAINS
  };

 public:
  Minus(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> left,
        std::shared_ptr<QueryExecutionTree> right,
        const std::vector<array<size_t, 2>>& matchedColumns);

  virtual string asString(size_t indent = 0) const override;

  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const;

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
  static void computeMinus(const IdTable& a, const IdTable& b,
                           const vector<array<size_t, 2>>& matchedColumns,
                           IdTable* result);

 private:
  template <int A_WIDTH, int B_WIDTH>
  static RowComparison isRowEq(const IdTableStatic<A_WIDTH>& a,
                               const IdTableStatic<B_WIDTH>& b, size_t* ia,
                               size_t* ib,
                               const vector<array<size_t, 2>>& matchedColumns);

  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed;

  std::vector<array<size_t, 2>> _matchedColumns;

  virtual void computeResult(ResultTable* result) override;
};
