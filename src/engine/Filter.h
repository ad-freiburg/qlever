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
#include "../parser/ParsedQuery.h"

using std::list;
using std::unordered_map;
using std::pair;
using std::vector;

class Filter : public Operation {
  public:
    virtual size_t getResultWidth() const;

  public:

    Filter(QueryExecutionContext *qec, const QueryExecutionTree& subtree,
           SparqlFilter::FilterType type, size_t var1Column, size_t var2Column);

    Filter(const Filter& other);

    Filter& operator=(const Filter& other);

    virtual ~Filter();

    virtual string asString() const;

    virtual size_t resultSortedOn() const {
      return _subtree->resultSortedOn();
    }

    virtual void setTextLimit(size_t limit) {
      _subtree->setTextLimit(limit);
    }

    virtual size_t getSizeEstimate() const {
      // TODO: return a better estimate
      if (_type == SparqlFilter::FilterType::EQ) {
        return _subtree->getSizeEstimate() / 100;
      } if (_type == SparqlFilter::FilterType::NE) {
        return _subtree->getSizeEstimate() / 2;
      } else {
        return _subtree->getSizeEstimate() / 10;
      }
    }

    virtual size_t getCostEstimate() const {
      return getSizeEstimate() + _subtree->getCostEstimate();
    }

  private:
    QueryExecutionTree *_subtree;
    SparqlFilter::FilterType _type;
    size_t _lhsInd;
    size_t _rhsInd;

    virtual void computeResult(ResultTable *result) const;
};
