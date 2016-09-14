// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <utility>
#include <vector>
#include <unordered_map>
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;
using std::unordered_map;
using std::pair;
using std::vector;

class TextOperationForEntities : public Operation {
public:

  TextOperationForEntities(
      QueryExecutionContext *qec, const string& words,
      const vector<pair<QueryExecutionTree, size_t>>& subtrees,
      size_t textLimit, size_t nofFreeVars = 0);

  // Delegate in the case without a subtree.
  TextOperationForEntities(
      QueryExecutionContext *qec, const string& words,
      size_t textLimit, size_t nofFreeVars = 0)
      : TextOperationForEntities(qec, words,
                                 vector<pair<QueryExecutionTree, size_t>>(),
                                 textLimit, nofFreeVars) { };

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const {
    if (_subtrees.size() == 0) {
      return std::numeric_limits<size_t>::max();
    } else {
      return 0;
    };
  }

  virtual void setTextLimit(size_t limit) {
    _textLimit = limit;
    for (auto& st: _subtrees) {
      st.first.setTextLimit(limit);
    }
  }

  virtual size_t getSizeEstimate() const {
    if (_executionContext) {
      // TODO: return a better estimate!
    }
    return 10000;
  }

  virtual size_t getCostEstimate() const {
    size_t sum = 10000;
    for (auto& pair : _subtrees) {
      sum += pair.first.getCostEstimate();
    }
    return sum;
  }

    virtual bool knownEmptyResult() const {
      return getSizeEstimate() == 0;
    }

private:
  string _words;
  vector<pair<QueryExecutionTree, size_t>> _subtrees;
  size_t _textLimit;
  size_t _freeVars;

  virtual void computeResult(ResultTable *result) const;
  void computeResultNoSubtrees(ResultTable *result) const;
  void computeResultOneSubtree(ResultTable *result) const;
  void computeResultMultSubtrees(ResultTable *result) const;
};
