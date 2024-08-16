// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)
#pragma once

#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/ParsedQuery.h"

class Filter : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  sparqlExpression::SparqlExpressionPimpl _expression;

 public:
  size_t getResultWidth() const override;

 public:
  Filter(QueryExecutionContext* qec,
         std::shared_ptr<QueryExecutionTree> subtree,
         sparqlExpression::SparqlExpressionPimpl expression);

 private:
  string getCacheKeyImpl() const override;

 public:
  string getDescriptor() const override;

  std::vector<ColumnIndex> resultSortedOn() const override {
    return _subtree->resultSortedOn();
  }

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  size_t getCostEstimate() override;

  std::shared_ptr<QueryExecutionTree> getSubtree() const { return _subtree; };
  std::vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

  bool knownEmptyResult() override { return _subtree->knownEmptyResult(); }

  float getMultiplicity(size_t col) override {
    return _subtree->getMultiplicity(col);
  }

 private:
  VariableToColumnMap computeVariableToColumnMap() const override {
    return _subtree->getVariableColumns();
  }

  ProtoResult computeResult(bool requestLaziness) override;

  // Perform the actual filter operation of the data provided by
  // `evaluationContext`.
  template <size_t WIDTH>
  IdTable computeFilterImpl(
      sparqlExpression::EvaluationContext& evaluationContext);

  // Run `computeFilterImpl` on the provided IdTable
  IdTable filterIdTable(const std::shared_ptr<const Result>& subRes,
                        const IdTable& idTable);
};
