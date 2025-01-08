//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

class ExistsScan : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> left_;
  std::shared_ptr<QueryExecutionTree> right_;
  std::vector<std::array<ColumnIndex, 2>> joinColumns_;

  Variable existsVariable_;

  vector<float> _multiplicities;
  std::vector<std::array<ColumnIndex, 2>> _matchedColumns;

 public:
  ExistsScan(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> left,
             std::shared_ptr<QueryExecutionTree> right,
             Variable existsVariable);

  static std::shared_ptr<QueryExecutionTree> addExistsScansToSubtree(
      const sparqlExpression::SparqlExpressionPimpl& expression,
      std::shared_ptr<QueryExecutionTree> subtree, QueryExecutionContext* qec,
      const ad_utility::SharedCancellationHandle& cancellationHandle);

 protected:
  string getCacheKeyImpl() const override;

 public:
  string getDescriptor() const override;

  size_t getResultWidth() const override;

  vector<ColumnIndex> resultSortedOn() const override;

  bool knownEmptyResult() override { return left_->knownEmptyResult(); }

  float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

  vector<QueryExecutionTree*> getChildren() override {
    return {left_.get(), right_.get()};
  }

 private:
  ProtoResult computeResult([[maybe_unused]] bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};
