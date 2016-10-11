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

  Join(QueryExecutionContext* qec,
       std::shared_ptr<QueryExecutionTree> t1,
       std::shared_ptr<QueryExecutionTree> t2,
       size_t t1JoinCol,
       size_t t2JoinCol,
       bool keepJoinColumn = true);

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const;

  std::unordered_map<string, size_t> getVariableColumns() const;
  std::unordered_set<string> getContextVars() const;

  virtual void setTextLimit(size_t limit) {
    _left->setTextLimit(limit);
    _right->setTextLimit(limit);
    _sizeEstimateComputed = false;
  }

  bool isSelfJoin() const;

  virtual size_t getSizeEstimate() {
    if (!_sizeEstimateComputed) {
      _sizeEstimate = computeSizeEstimate();
      _sizeEstimateComputed = true;
    }
    return _sizeEstimate;
  }

  virtual size_t getCostEstimate() {
    return getSizeEstimate() +
        _left->getSizeEstimate() + _left->getCostEstimate() +
        _right->getSizeEstimate() + _right->getCostEstimate();
  }

  virtual bool knownEmptyResult() {
    return _left->knownEmptyResult() || _right->knownEmptyResult();
  }

  size_t computeSizeEstimate() const {
    if (_left->getSizeEstimate() == 0
        || _right->getSizeEstimate() == 0) {
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
          _left->getRootOperation().get())->getResultWidth() == 1) {
        ++easySides;
      }
    }
    if (_right->getType() == QueryExecutionTree::SCAN) {
      if (static_cast<const IndexScan*>(
          _right->getRootOperation().get())->getResultWidth() == 1) {
        ++easySides;
      }
    }
    // return std::min(_left->getSizeEstimate(), _right->getSizeEstimate()) / 2;
    // Self joins generally increase the size significantly.
    if (isSelfJoin()) {
      return std::max(
          size_t(1),
          (_left->getSizeEstimate() + _right->getSizeEstimate())
              * 10);
    }
    if (easySides == 0) {
      return (_left->getSizeEstimate() + _right->getSizeEstimate())
          * 4;
    } else if (easySides == 1) {
      return std::max(
          size_t(1),
          (_left->getSizeEstimate() + _right->getSizeEstimate())
              / 4);
    } else {
      return std::max(
          size_t(1),
          (_left->getSizeEstimate() + _right->getSizeEstimate())
              / 10);
    }
  }

 private:
  std::shared_ptr<QueryExecutionTree> _left;
  std::shared_ptr<QueryExecutionTree> _right;

  size_t _leftJoinCol;
  size_t _rightJoinCol;

  bool _keepJoinColumn;

  bool _sizeEstimateComputed;
  size_t _sizeEstimate;

  virtual void computeResult(ResultTable* result) const;
};
