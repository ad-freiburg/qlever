// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <list>

#include "./Operation.h"
#include "./QueryExecutionTree.h"

using std::list;

class Sort : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  size_t _sortCol;

 public:
  Sort(QueryExecutionContext* qec, std::shared_ptr<QueryExecutionTree> subtree,
       size_t sortCol);

 public:
  virtual string getDescriptor() const override;

  virtual vector<size_t> resultSortedOn() const override { return {_sortCol}; }

  virtual void setTextLimit(size_t limit) override {
    _subtree->setTextLimit(limit);
  }

  virtual size_t getSizeEstimate() override {
    return _subtree->getSizeEstimate();
  }

  virtual float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

  std::shared_ptr<QueryExecutionTree> getSubtree() const { return _subtree; }

  virtual size_t getCostEstimate() override {
    size_t size = getSizeEstimate();
    size_t logSize = std::max(
        size_t(2), static_cast<size_t>(logb(static_cast<double>(size))));
    size_t nlogn = size * logSize;
    size_t subcost = _subtree->getCostEstimate();
    return nlogn + subcost;
  }

  virtual bool knownEmptyResult() override {
    return _subtree->knownEmptyResult();
  }

  [[nodiscard]] size_t getResultWidth() const override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

 private:
  virtual void computeResult(ResultTable* result) override;

  [[nodiscard]] VariableToColumnMap computeVariableToColumnMap()
      const override {
    return _subtree->getVariableColumns();
  }

  virtual string asStringImpl(size_t indent = 0) const override;
};
