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
  [[nodiscard]] size_t getResultWidth() const override;

 public:
  Distinct(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           const vector<size_t>& keepIndices);

 protected:
  [[nodiscard]] string asStringImpl(size_t indent = 0) const override;

 public:
  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] vector<size_t> resultSortedOn() const override {
    return _subtree->resultSortedOn();
  }

  virtual void setTextLimit(size_t limit) override {
    _subtree->setTextLimit(limit);
  }

  virtual size_t getSizeEstimate() override {
    return _subtree->getSizeEstimate();
  }

  virtual size_t getCostEstimate() override {
    return getSizeEstimate() + _subtree->getCostEstimate();
  }

  virtual float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

  bool knownEmptyResult() override { return _subtree->knownEmptyResult(); }

  ad_utility::HashMap<string, size_t> getVariableColumns() const override;

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<size_t> _keepIndices;

  virtual void computeResult(ResultTable* result) override;
};
