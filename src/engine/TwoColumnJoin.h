// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <unordered_map>
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;
using std::unordered_map;


class TwoColumnJoin : public Operation {
public:

  TwoColumnJoin(QueryExecutionContext* qec, const QueryExecutionTree& t1,
                const QueryExecutionTree& t2,
                const std::vector<array<size_t, 2>>& joinCols);

  TwoColumnJoin(const TwoColumnJoin& other);

  TwoColumnJoin& operator=(const TwoColumnJoin& other);

  virtual ~TwoColumnJoin();

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  unordered_map<string, size_t> getVariableColumns() const;

  virtual void setTextLimit(size_t limit) {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  virtual size_t getSizeEstimate() const {
    return (_left->getSizeEstimate() + _right->getSizeEstimate()) / 6;
  }

  virtual size_t getCostEstimate() const {
    return _left->getSizeEstimate() + _left->getCostEstimate() +
           _right->getSizeEstimate() + _right->getCostEstimate();
  }

private:
  QueryExecutionTree* _left;
  QueryExecutionTree* _right;

  size_t _jc1Left;
  size_t _jc2Left;
  size_t _jc1Right;
  size_t _jc2Right;

  virtual void computeResult(ResultTable* result) const;
};
