// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <list>
#include <utility>
#include <vector>

#include "./IndexScan.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

using std::pair;
using std::vector;

class OrderBy : public Operation {
 public:
  OrderBy(QueryExecutionContext* qec,
          std::shared_ptr<QueryExecutionTree> subtree,
          const vector<pair<size_t, bool>>& sortIndices);

  virtual string asString(size_t indent = 0) const override;

  virtual string getDescriptor() const override;

  virtual vector<size_t> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override {
    _subtree->setTextLimit(limit);
  }

  virtual size_t getSizeEstimate() override {
    return _subtree->getSizeEstimate();
  }

  virtual float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

  virtual size_t getCostEstimate() override {
    size_t size = getSizeEstimate();
    size_t logSize = std::max(
        size_t(1),
        static_cast<size_t>(logb(static_cast<double>(getSizeEstimate()))));
    size_t nlogn = size * logSize;
    size_t subcost = _subtree->getCostEstimate();
    return nlogn + subcost;
  }

  virtual bool knownEmptyResult() override {
    return _subtree->knownEmptyResult();
  }

  virtual size_t getResultWidth() const override;

  virtual ad_utility::HashMap<string, size_t> getVariableColumns()
      const override {
    return _subtree->getVariableColumns();
  }

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<pair<size_t, bool>> _sortIndices;

  virtual void computeResult(ResultTable* result) override;
};
