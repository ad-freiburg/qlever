// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

class TwoColumnJoin : public Operation {
 public:
  TwoColumnJoin(QueryExecutionContext* qec,
                std::shared_ptr<QueryExecutionTree> t1,
                std::shared_ptr<QueryExecutionTree> t2,
                const std::vector<array<Id, 2>>& joinCols);

  virtual string asString(size_t indent = 0) const override;

  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  VariableColumnMap getVariableColumns() const override;

  virtual void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  virtual size_t getSizeEstimate() override {
    return (_left->getSizeEstimate() + _right->getSizeEstimate()) / 10;
  }

  virtual size_t getCostEstimate() override {
    if ((_left->getResultWidth() == 2 && _jc1Left == 0 && _jc2Left == 1) ||
        (_right->getResultWidth() == 2 && _jc1Right == 0 && _jc2Right == 1)) {
      return _left->getSizeEstimate() + _left->getCostEstimate() +
             _right->getSizeEstimate() + _right->getCostEstimate();
    }
    // The case where the above condition does not hold is currently
    // not implemented so really don't use it!
    // Important: The / 1000000 prevents overflow
    // TODO(schnelle) this is pretty fragile
    return std::numeric_limits<size_t>::max() / 1000000;
  }

  virtual bool knownEmptyResult() override {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_left.get(), _right.get()};
  }

 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  size_t _jc1Left;
  size_t _jc2Left;
  size_t _jc1Right;
  size_t _jc2Right;

  vector<float> _multiplicities;

  void computeMultiplicities();

  virtual void computeResult(ResultTable* result) override;
};
