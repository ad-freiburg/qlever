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

  virtual string asString() const;

  virtual size_t getResultWidth() const;

  virtual size_t resultSortedOn() const {
    // unsorted, obtained from iterating over a hash map.
    return std::numeric_limits<size_t>::max();
  }

  virtual void setTextLimit(size_t limit) {
    _textLimit = limit;
    _filterResult->setTextLimit(limit);
    _multiplicities.clear();
  }

  virtual size_t getSizeEstimate() {
    if (_executionContext) {
      // NEW at 05 Dec 2016:
      // Estimate the size of the result like the equivalent text without filter
      // plus join.
      auto textEst = _executionContext->getIndex().getSizeEstimate(_words);
      size_t entityColInResult = 2;  // for readability
      size_t nofDistinctFilter = static_cast<size_t>(
          _filterResult->getSizeEstimate() /
          _filterResult->getMultiplicity(_filterColumn));
      return static_cast<size_t>(getMultiplicity(entityColInResult) *
                                 std::min(nofDistinctFilter, textEst));
    }
    return size_t(10000 * 0.8);
  }

  virtual size_t getCostEstimate() {
    if (_filterResult->knownEmptyResult()) {
      return 0;
    }
    if (_executionContext) {
      return static_cast<size_t>(
          _executionContext->getCostFactor("FILTER_PUNISH") * (
              getSizeEstimate() * _nofVars +
              _filterResult->getSizeEstimate() *
              _executionContext->getCostFactor("HASH_MAP_OPERATION_COST") +
              _filterResult->getCostEstimate()));
    } else {
      return _filterResult->getSizeEstimate() * 2 +
             _filterResult->getCostEstimate();
    }
  }

  const string& getWordPart() const {
    return _words;
  }

  size_t getNofVars() const {
    return _nofVars;
  }

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

  vector<float> _multiplicities;

  void computeMultiplicities();

  virtual void computeResult(ResultTable* result) const;
};
