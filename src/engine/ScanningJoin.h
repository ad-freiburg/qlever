// Copyright 2016, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>

#include "./IndexScan.h"
#include "./QueryExecutionTree.h"

using std::list;

// Special operation that replaces a join between a (usually very small
// in terms of #rows) result and the result of a (usually very large) scan
// by performing a scan for each row in the sub-result and thus creating
// the result of the Join without ever scanning the full, huge relation.
class ScanningJoin : public IndexScan {
  ScanningJoin(QueryExecutionContext* qec, const QueryExecutionTree& subtree,
               size_t subtreeJoinCol, IndexScan::ScanType scanType);

  ScanningJoin(const ScanningJoin& other);

  ScanningJoin& operator=(const ScanningJoin& other);

  virtual ~ScanningJoin();

  virtual string asString(size_t indent = 0) const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override {
    return {_subtreeJoinCol};
  }

  virtual void setTextLimit(size_t limit) { _subtree->setTextLimit(limit); }

  virtual size_t getSizeEstimate() { return _subtree->getSizeEstimate(); }

  virtual float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

  virtual size_t getCostEstimate() override {
    return _subtree->getSizeEstimate() + getSizeEstimate() * 10;
  }

  virtual bool knownEmptyResult() override {
    return _subtree->knownEmptyResult() || IndexScan::knownEmptyResult();
  }

  vector<QueryExecutionTree*> getChildren() override { return {_subtree}; }

 private:
  QueryExecutionTree* _subtree;
  size_t _subtreeJoinCol;
  virtual void computeResult(ResultTable* result) override;
};
