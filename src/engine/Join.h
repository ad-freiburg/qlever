// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include "./Operation.h"
#include "./QueryExecutionTree.h"
#include "./IndexScan.h"
#include "../util/HashMap.h"
#include "../util/HashSet.h"

using std::list;

class Join : public Operation {
public:

  Join(QueryExecutionContext* qec, const QueryExecutionTree& t1,
       const QueryExecutionTree& t2, size_t t1JoinCol, size_t t2JoinCol,
       bool keepJoinColumn = true);

  Join(const Join& other);

  Join& operator=(const Join& other);

  virtual ~Join();

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  ad_utility::HashMap<string, size_t> getVariableColumns() const;
  ad_utility::HashSet<string> getContextVars() const;

  virtual void setTextLimit(size_t limit) {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
  }

  bool isSelfJoin() const;

  virtual size_t getSizeEstimate() const {
    if (_left->getSizeEstimate() == 0 || _right->getSizeEstimate() == 0) {
      return 0;
    }
    // Check if there are easy sides, i.e. a scan with only one
    // variable.
    // As a very basic heuristic, we expect joins with those to be even more
    // restrictive. Obvious counter examples are stuff like "?x <is-a> <Topic>",
    // i.e. very large lists, but at least we certainly account for size
    // already and such joins are still very nice
    // (no sorting, certainly restrictive).
    // Without any easy side, we assume the worst, i.e. that the join actually
    // increases the result size over the sum of two subtree sizes.
    size_t easySides = 0;
    if (_left->getType() == QueryExecutionTree::SCAN) {
      if (static_cast<const IndexScan*>(
              _left->getRootOperation())->getResultWidth() == 1) {
        ++easySides;
      }
    }
    if (_right->getType() == QueryExecutionTree::SCAN) {
      if (static_cast<const IndexScan*>(
              _right->getRootOperation())->getResultWidth() == 1) {
        ++easySides;
      }
    }
    // return std::min(_left->getSizeEstimate(), _right->getSizeEstimate()) / 2;
    // Self joins generally increase the size significantly.
    if (isSelfJoin()) {
      return std::max(
          size_t(1),
          (_left->getSizeEstimate() + _right->getSizeEstimate()) * 10);
    }
    if (easySides == 0) {
      return (_left->getSizeEstimate() + _right->getSizeEstimate()) * 4;
    } else if (easySides == 1) {
      return std::max(
          size_t(1),
          (_left->getSizeEstimate() + _right->getSizeEstimate()) / 4);
    } else {
      return std::max(
          size_t(1),
          (_left->getSizeEstimate() + _right->getSizeEstimate()) / 10);
    }
  }

  virtual size_t getCostEstimate() const {
    return getSizeEstimate() +
           _left->getSizeEstimate() + _left->getCostEstimate() +
           _right->getSizeEstimate() + _right->getCostEstimate();
  }

  virtual bool knownEmptyResult() const {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  private:
  QueryExecutionTree* _left;
  QueryExecutionTree* _right;

  size_t _leftJoinCol;
  size_t _rightJoinCol;

  bool _keepJoinColumn;

  virtual void computeResult(ResultTable* result) const;
};
