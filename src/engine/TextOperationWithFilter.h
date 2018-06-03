// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <utility>
#include <vector>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

using std::pair;
using std::vector;

class TextOperationWithFilter : public Operation {
 public:
  TextOperationWithFilter(QueryExecutionContext* qec, const string& words,
                          size_t nofVars,
                          std::shared_ptr<QueryExecutionTree> filterResult,
                          size_t filterColumn, size_t textLimit = 1);

  virtual string asString(size_t indent) const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const {
    // unsorted, obtained from iterating over a hash map.
    return std::numeric_limits<size_t>::max();
  }

  virtual void setTextLimit(size_t limit) {
    _textLimit = limit;
    _filterResult->setTextLimit(limit);
    _sizeEstimate = std::numeric_limits<size_t>::max();
    _multiplicities.clear();
  }

  virtual size_t getSizeEstimate();
  virtual size_t getCostEstimate();

  const string& getWordPart() const { return _words; }

  size_t getNofVars() const { return _nofVars; }

  virtual bool knownEmptyResult() {
    return _filterResult->knownEmptyResult() ||
           (_executionContext &&
            _executionContext->getIndex().getSizeEstimate(_words) == 0);
  }

  virtual float getMultiplicity(size_t col);

 private:
  string _words;
  size_t _nofVars;
  size_t _textLimit;

  std::shared_ptr<QueryExecutionTree> _filterResult;
  size_t _filterColumn;

  size_t _sizeEstimate;
  vector<float> _multiplicities;

  void computeMultiplicities();

  virtual void computeResult(ResultTable* result) const;
};
