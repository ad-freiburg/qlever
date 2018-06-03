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

class Distinct : public Operation {
 public:
  virtual size_t getResultWidth() const;

 public:
  Distinct(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           const vector<size_t>& keepIndices);

  virtual string asString(size_t indent = 0) const;

  virtual size_t resultSortedOn() const { return _subtree->resultSortedOn(); }

  virtual void setTextLimit(size_t limit) { _subtree->setTextLimit(limit); }

  virtual size_t getSizeEstimate() { return _subtree->getSizeEstimate(); }

  virtual size_t getCostEstimate() {
    return getSizeEstimate() + _subtree->getCostEstimate();
  }

  virtual float getMultiplicity(size_t col) {
    return _subtree->getMultiplicity(col);
  }

  virtual bool knownEmptyResult() { return _subtree->knownEmptyResult(); }

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<size_t> _keepIndices;

  virtual void computeResult(ResultTable* result) const;
};
