// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <utility>
#include <vector>
#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;
using std::pair;
using std::vector;

class Filter : public Operation {
 public:
  virtual size_t getResultWidth() const;

 public:
  Filter(QueryExecutionContext* qec,
         std::shared_ptr<QueryExecutionTree> subtree,
         SparqlFilter::FilterType type, size_t var1Column, size_t var2Column,
         Id rhsId = std::numeric_limits<Id>::max());

  virtual string asString(size_t indent = 0) const;

  virtual size_t resultSortedOn() const { return _subtree->resultSortedOn(); }

  virtual void setTextLimit(size_t limit) { _subtree->setTextLimit(limit); }

  virtual size_t getSizeEstimate() {
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
        return _subtree->getSizeEstimate();
      } else {
        return _subtree->getSizeEstimate() / 50;
      }
    }
  }

  virtual size_t getCostEstimate() {
    return getSizeEstimate() + _subtree->getSizeEstimate() +
           _subtree->getCostEstimate();
  }

  void setRightHandSideString(std::string s) { _rhsString = s; }

  std::shared_ptr<QueryExecutionTree> getSubtree() const { return _subtree; };

  virtual bool knownEmptyResult() { return _subtree->knownEmptyResult(); }

  virtual float getMultiplicity(size_t col) {
    return _subtree->getMultiplicity(col);
  }

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  SparqlFilter::FilterType _type;
  size_t _lhsInd;
  size_t _rhsInd;
  Id _rhsId;
  std::string _rhsString;

  virtual void computeResult(ResultTable* result) const;

  void computeResultFixedValue(ResultTable* result) const;
};
