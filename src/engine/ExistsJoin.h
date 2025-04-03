// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_EXISTSJOIN_H
#define QLEVER_SRC_ENGINE_EXISTSJOIN_H

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

// The implementation of an "EXISTS join", which we use to realize the semantics
// of the SPARQL `EXISTS` function. The join takes two subtrees as input, and
// returns the left subtree with an additional boolean column that is `true` iff
// at least one matching row is contained in the right subtree.
class ExistsJoin : public Operation {
 private:
  // The left and right child.
  std::shared_ptr<QueryExecutionTree> left_;
  std::shared_ptr<QueryExecutionTree> right_;
  std::vector<std::array<ColumnIndex, 2>> joinColumns_;

  // The variable of the added (Boolean) result column.
  Variable existsVariable_;

 public:
  // Constructor. The `existsVariable` (the variable for the added column) must
  // not yet be bound in `left`.
  ExistsJoin(QueryExecutionContext* qec,
             std::shared_ptr<QueryExecutionTree> left,
             std::shared_ptr<QueryExecutionTree> right,
             Variable existsVariable);

  // Extract all `ExistsExpression`s from the given `expression`. For each
  // `ExistsExpression`, add an `ExistsJoin`. The left side of the first
  // `ExistsJoin` is the input `subtree`. The left side of subsequent
  // `ExistsJoin`s is the previous `ExistsJoin`. The right side of each
  // `ExistsJoin` is the argument of the respective `ExistsExpression`. When
  // there are no `ExistsExpression`s, return the input `subtree` unchanged.
  //
  // The returned subtree will contain one additional column for each
  // `ExistsExpression`, which contains the result of the respective
  // `ExistsJoin`. The `ExistsExpression` just reads the values of this column.
  // The main work is done by the `ExistsJoin`.
  //
  // This function should be called in the constructor of each `Operation`,
  // where an `EXISTS` expression can occur. For example, in the constructor of
  // `BIND` and `FILTER`.
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
  std::unique_ptr<Operation> cloneImpl() const override;

  Result computeResult(bool requestLaziness) override;

  VariableToColumnMap computeVariableToColumnMap() const override;
};

#endif  // QLEVER_SRC_ENGINE_EXISTSJOIN_H
