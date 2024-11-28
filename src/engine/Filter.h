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
#include "parser/ParsedQuery.h"

class Filter : public Operation {
  using PrefilterVariablePair = sparqlExpression::PrefilterExprVariablePair;

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

  // The method is directly invoked with the construction of this `Filter`
  // object. Its implementation retrieves <PrefilterExpression, Variable> pairs
  // from the corresponding `SparqlExpression` and uses them to call
  // `QueryExecutionTree::setPrefilterGetUpdatedQueryExecutionTree()` on the
  // `subtree_`. If necessary the `QueryExecutionTree` for this
  // entity will be updated.
  void setPrefilterExpressionForChildren();

  ProtoResult computeResult(bool requestLaziness) override;

  // Perform the actual filter operation of the data provided.
  template <int WIDTH, ad_utility::SimilarTo<IdTable> Table>
  void computeFilterImpl(IdTable& dynamicResultTable, Table&& input,
                         const LocalVocab& localVocab,
                         std::vector<ColumnIndex> sortedBy) const;

  // Run `computeFilterImpl` on the provided IdTable
  template <ad_utility::SimilarTo<IdTable> Table>
  IdTable filterIdTable(std::vector<ColumnIndex> sortedBy, Table&& idTable,
                        const LocalVocab& localVocab) const;
};
