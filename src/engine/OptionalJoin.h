// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;


class OptionalJoin : public Operation {
public:

  OptionalJoin(QueryExecutionContext* qec,
               std::shared_ptr<QueryExecutionTree> t1,
               bool t1Optional,
               std::shared_ptr<QueryExecutionTree> t2,
               bool t2Optional,
               const std::vector<array<size_t, 2>>& joinCols);

  virtual string asString(size_t indent = 0) const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  std::unordered_map<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() {
    return (_left->knownEmptyResult() && !_leftOptional)
        || (_right->knownEmptyResult() && !_rightOptional)
        || (_left->knownEmptyResult() && _right->knownEmptyResult());
  }

  virtual float getMultiplicity(size_t col);

  virtual size_t getSizeEstimate();

  virtual size_t getCostEstimate();

private:
  void computeSizeEstimateAndMultiplicities();

  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;
  bool _leftOptional;
  bool _rightOptional;

  std::vector<std::array<size_t, 2>> _joinColumns;

  vector<float> _multiplicities;
  size_t _sizeEstimate;
  bool _multiplicitiesComputed;

  virtual void computeResult(ResultTable* result) const;
};
