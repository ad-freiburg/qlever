// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2015-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_ENGINE_FILTER_H
#define QLEVER_SRC_ENGINE_FILTER_H

#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"

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
  std::string getCacheKeyImpl() const override;

 public:
  std::string getDescriptor() const override;

  std::vector<ColumnIndex> resultSortedOn() const override {
    return _subtree->resultSortedOn();
  }

  // A `Filter` preserves both the columns and the sort order of its child, so a
  // requested sort order can be pushed down to the child (e.g. an `IndexScan`
  // that can re-sort itself by changing its permutation). This avoids an
  // explicit `Sort` on top of the `Filter`.
  std::optional<std::shared_ptr<QueryExecutionTree>> makeSortedTree(
      const std::vector<ColumnIndex>& sortColumns) const override;

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
  [[nodiscard]] bool isDeterministicImpl() const override;

  std::unique_ptr<Operation> cloneImpl() const override;

  VariableToColumnMap computeVariableToColumnMap() const override {
    return _subtree->getVariableColumns();
  }

  // The method is directly invoked with the construction of this `Filter`
  // object. Its implementation retrieves <PrefilterExpression, Variable> pairs
  // from the corresponding `SparqlExpression` and uses them to call
  // `QueryExecutionTree::getUpdatedQueryExecutionTreeWithPrefilterApplied()` on
  // the `subtree_`. If necessary the `QueryExecutionTree` for this entity will
  // be updated.
  void setPrefilterExpressionForChildren();

  Result computeResult(bool requestLaziness) override;

  // Perform the actual filter operation of the data provided.
  CPP_template(int WIDTH, typename Table)(
      requires IdTableLike<
          Table>) void computeFilterImpl(IdTable& dynamicResultTable,
                                         Table&& input,
                                         std::vector<ColumnIndex> sortedBy)
      const;

  // Run `computeFilterImpl` on the provided IdTable.
  CPP_template(typename Table)(requires IdTableLike<Table>) IdTable
      filterIdTable(std::vector<ColumnIndex> sortedBy, Table&& idTable) const;
};

#endif  // QLEVER_SRC_ENGINE_FILTER_H
