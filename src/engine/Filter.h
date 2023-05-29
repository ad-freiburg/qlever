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
  string asStringImpl(size_t indent = 0) const override;

 public:
  string getDescriptor() const override;

  std::vector<ColumnIndex> resultSortedOn() const override {
    return _subtree->resultSortedOn();
  }

  void setTextLimit(size_t limit) override { _subtree->setTextLimit(limit); }

 private:
  size_t getSizeEstimateBeforeLimit() override;

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

  ResultTable computeResult() override;

  template <size_t WIDTH>
  void computeFilterImpl(IdTable* outputIdTable,
                         const ResultTable& inputResultTable);
};
