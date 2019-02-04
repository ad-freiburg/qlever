// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//         Florian Kramer (florian.kramer@netpun.uni-freiburg.de)
#pragma once

#include <list>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

class OptionalJoin : public Operation {
 public:
  OptionalJoin(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> t1, bool t1Optional,
               std::shared_ptr<QueryExecutionTree> t2, bool t2Optional,
               const std::vector<array<Id, 2>>& joinCols);

  virtual string asString(size_t indent = 0) const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) override {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() override {
    return (_left->knownEmptyResult() && !_leftOptional) ||
           (_right->knownEmptyResult() && !_rightOptional) ||
           (_left->knownEmptyResult() && _right->knownEmptyResult());
  }

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

 private:
  void computeSizeEstimateAndMultiplicities();

  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;
  bool _leftOptional;
  bool _rightOptional;

  std::vector<std::array<Id, 2>> _joinColumns;

  vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed;

  virtual void computeResult(ResultTable* result) override;
};
