// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "parser/ParsedQuery.h"

using std::vector;

class Distinct : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<ColumnIndex> _keepIndices;

 public:
  Distinct(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           const vector<ColumnIndex>& keepIndices);

  [[nodiscard]] size_t getResultWidth() const override;

 public:
  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override {
    return _subtree->resultSortedOn();
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    return _subtree->getSizeEstimate();
  }

 public:
  virtual size_t getCostEstimate() override {
    return getSizeEstimateBeforeLimit() + _subtree->getCostEstimate();
  }

  virtual float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

  bool knownEmptyResult() override { return _subtree->knownEmptyResult(); }

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

 protected:
  [[nodiscard]] string getCacheKeyImpl() const override;

 private:
  virtual ResultTable computeResult() override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
