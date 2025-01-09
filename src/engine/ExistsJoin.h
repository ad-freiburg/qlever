//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

// The implementation of the SPARQL `EXISTS` function. It takes two subtrees,
// and returns the left subtree with an additional boolean column that is `true`
// iff at least one matching row is contained in the right subtree.
class ExistsJoin : public Operation {
 private:
  // The left and right child.
  std::shared_ptr<QueryExecutionTree> left_;
  std::shared_ptr<QueryExecutionTree> right_;
  std::vector<std::array<ColumnIndex, 2>> joinColumns_;

  // The variable of the added result column.
  Variable existsVariable_;

 public:
  // Constructor. The `existsVariable` (the variable for the added boolean
  // column) must not yet be bound by `left`.
  ExistsJoin(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> left,
             std::shared_ptr<QueryExecutionTree> right,
             Variable existsVariable);

  // For a given subtree and a given expression, extract all the
  // `ExistsExpressions` from the expression and add one `ExistsJoin` per
  // `ExistsExpression` to the subtree. The left side of the `ExistsJoin` is the
  // input subtree, the right hand side of the `ExistsJoin` as well as the
  // variable to which the result is bound are extracted from the
  // `ExistsExpression`. The returned subtree can then be used to evaluate the
  // `expression`. Note: `ExistsExpression` is a simple dummy that only reads
  // the values of the column that is added by the `ExistsJoin`.
  static std::shared_ptr<QueryExecutionTree> addExistsJoinsToSubtree(
      const sparqlExpression::SparqlExpressionPimpl& expression,
      std::shared_ptr<QueryExecutionTree> subtree, QueryExecutionContext* qec,
      const ad_utility::SharedCancellationHandle& cancellationHandle);

  // All following functions are inherited from `Operation`, see there for
  // comments.
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
