// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <array>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

class MultiColumnJoin : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  std::vector<std::array<ColumnIndex, 2>> _joinColumns;

  vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed = false;

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

  virtual vector<ColumnIndex> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override;

 private:
  size_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

  /**
   * @brief Joins left and right using the column defined int joinColumns,
   *storing the resultMightBeUnsorted in resultMightBeUnsorted. R should have
   *width resultWidth (or be left vector that should have resultWidth entries).
   *This method is made public here for unit testing purposes.
   **/
  static void computeMultiColumnJoin(
      const IdTable& left, const IdTable& right,
      const std::vector<std::array<ColumnIndex, 2>>& joinColumns,
      IdTable* resultMightBeUnsorted);

 private:
  virtual ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;

  void computeSizeEstimateAndMultiplicities();
};
