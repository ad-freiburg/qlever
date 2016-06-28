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

class TextOperationWithoutFilter : public Operation {
public:

  TextOperationWithoutFilter(QueryExecutionContext *qec, const string& words,
                             size_t nofVars, size_t textLimit = 1);

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const {
    // unsorted, obtained from iterating over hashmap.
    return std::numeric_limits<size_t>::max();
  }

  virtual void setTextLimit(size_t limit) {
    _textLimit = limit;
  }

  virtual size_t getSizeEstimate() const {
    if (_executionContext) {
      return _executionContext->getIndex().getSizeEstimate(_words);
    }
    return 10000;
  }

  virtual size_t getCostEstimate() const {
    return getSizeEstimate() * _nofVars;
  }

  const string& getWordPart() const {
    return _words;
  }

  size_t getNofVars() const {
    return _nofVars;
  }

private:
  string _words;
  size_t _nofVars;
  size_t _textLimit;


  virtual void computeResult(ResultTable *result) const;

  void computeResultNoVar(ResultTable *result) const;

  void computeResultOneVar(ResultTable *result) const;

  void computeResultMultVars(ResultTable *result) const;
};
