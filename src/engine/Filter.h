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
           SparqlFilter::FilterType type, size_t var1Column, size_t var2Column,
           Id rhsId = std::numeric_limits<Id>::max());

    Filter(const Filter& other);

    Filter(Filter&& other) noexcept;

    Filter& operator=(Filter other);

    Filter& operator=(Filter&& other) noexcept;

    friend void swap(Filter& a, Filter& b) noexcept {
      using std::swap;
      swap(a._subtree, b._subtree);
      swap(a._type, b._type);
      swap(a._lhsInd, b._lhsInd);
      swap(a._rhsInd, b._rhsInd);
      swap(a._rhsId, b._rhsId);
    }

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
      if (_rhsId == std::numeric_limits<Id>::max()) {
        if (_type == SparqlFilter::FilterType::EQ) {
          return _subtree->getSizeEstimate() / 1000;
        }
        if (_type == SparqlFilter::FilterType::NE) {
          return _subtree->getSizeEstimate() / 4;
        } else {
          return _subtree->getSizeEstimate() / 2;
        }
      } else {
        if (_type == SparqlFilter::FilterType::EQ) {
          return _subtree->getSizeEstimate() / 1000;
        }
        if (_type == SparqlFilter::FilterType::NE) {
          return _subtree->getSizeEstimate() / 4;
        } else {
          return _subtree->getSizeEstimate() / 50;
        }
      }
    }

    virtual size_t getCostEstimate() const {
      return getSizeEstimate() + _subtree->getSizeEstimate() +
             _subtree->getCostEstimate();
    }

    const QueryExecutionTree* getSubtree() const {
        return _subtree;
    };

    virtual bool knownEmptyResult() const {
      return _subtree->knownEmptyResult();
    }

  private:
    QueryExecutionTree* _subtree;
    SparqlFilter::FilterType _type;
    size_t _lhsInd;
    size_t _rhsInd;
    Id _rhsId;

    virtual void computeResult(ResultTable *result) const;

    void computeResultFixedValue(ResultTable *result) const;
};
