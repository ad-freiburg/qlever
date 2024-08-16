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
  std::shared_ptr<QueryExecutionTree> subtree_;
  vector<ColumnIndex> _keepIndices;

 public:
  Distinct(QueryExecutionContext* qec,
           std::shared_ptr<QueryExecutionTree> subtree,
           const vector<ColumnIndex>& keepIndices);

  [[nodiscard]] size_t getResultWidth() const override;

  [[nodiscard]] string getDescriptor() const override;

  [[nodiscard]] vector<ColumnIndex> resultSortedOn() const override {
    return subtree_->resultSortedOn();
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override {
    return subtree_->getSizeEstimate();
  }

 public:
  size_t getCostEstimate() override {
    return getSizeEstimateBeforeLimit() + subtree_->getCostEstimate();
  }

  float getMultiplicity(size_t col) override {
    return subtree_->getMultiplicity(col);
  }

  bool knownEmptyResult() override { return subtree_->knownEmptyResult(); }

  vector<QueryExecutionTree*> getChildren() override {
    return {subtree_.get()};
  }

 protected:
  [[nodiscard]] string getCacheKeyImpl() const override;

 private:
  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
