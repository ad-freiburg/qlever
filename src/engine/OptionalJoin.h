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

  virtual size_t getSizeEstimate();

  virtual size_t getCostEstimate() {
    // This operation is currently only supposed to be used to join two optional
    // results.
    return std::numeric_limits<size_t>::max() / 1000000;
  }

  virtual bool knownEmptyResult() {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col);

private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;
  bool _leftOptional;
  bool _rightOptional;

  std::vector<std::array<size_t, 2>> _joinColumns;

  vector<float> _multiplicities;

  virtual void computeResult(ResultTable* result) const;
};
