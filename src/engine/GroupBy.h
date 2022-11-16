// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018      Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "gtest/gtest.h"
#include "parser/Alias.h"
#include "parser/ParsedQuery.h"

using std::string;
using std::vector;

// Forward declarations for internal member function
class IndexScan;
class Join;

class GroupBy : public Operation {
 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<Variable> _groupByVariables;
  std::vector<Alias> _aliases;

 public:
  /**
   * @brief Represents an aggregate alias in the select part of the query.
   */
  struct Aggregate {
    sparqlExpression::SparqlExpressionPimpl _expression;
    size_t _outCol;
  };

  GroupBy(QueryExecutionContext* qec, vector<Variable> groupByVariables,
          std::vector<Alias> aliases,
          std::shared_ptr<QueryExecutionTree> subtree);

 protected:
  virtual string asStringImpl(size_t indent = 0) const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override {
    _subtree->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() override {
    return _subtree->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override;

  virtual size_t getSizeEstimate() override;

  virtual size_t getCostEstimate() override;

  /**
   * @return The columns on which the input data should be sorted or an empty
   *         list if no particular order is required for the grouping.
   * @param subtree The QueryExecutionTree that contains the operations
   *                  creating the sorting operation inputs.
   */
  vector<size_t> computeSortColumns(const QueryExecutionTree* subtree);

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

 private:
  VariableToColumnMap computeVariableToColumnMap() const override;

  virtual void computeResult(ResultTable* result) override;

  template <int OUT_WIDTH>
  void processGroup(const Aggregate& expression,
                    sparqlExpression::EvaluationContext evaluationContext,
                    size_t blockStart, size_t blockEnd,
                    IdTableStatic<OUT_WIDTH>* result, size_t resultRow,
                    size_t resultColumn, ResultTable* outTable,
                    ResultTable::ResultType* resultType) const;

  template <int IN_WIDTH, int OUT_WIDTH>
  void doGroupBy(const IdTable& dynInput, const vector<size_t>& groupByCols,
                 const vector<GroupBy::Aggregate>& aggregates,
                 IdTable* dynResult, const ResultTable* inTable,
                 ResultTable* outTable) const;

  FRIEND_TEST(GroupByTest, doGroupBy);

 public:
  // TODO<joka921> use `FRIEND_TEST` here once we have converged on the set
  // of tests to write.

  // For certain combinations of `_groupByColumns`, `_aliases` and `_subtree`,
  // it is not necessary to fully materialize the `_subtree`'s result to compute
  // the GROUP BY, but the result can simply be read from the index meta data.
  // An example for such a combination is the query
  //  SELECT ((COUNT ?x) as ?cnt) WHERE {
  //    ?x <somePredicate> ?y
  //  }
  // This function checks whether such a case applies. In this case the result
  // is computed and stored in `result` and `true` is returned. If no such case
  // applies, `false` is returned and the `result` is untouched. Precondition:
  // The `result` must be empty.
  // TODO<joka921> Check whether we can simply return
  // `std::optional<ResultTable>` (Also for all the other functions).
  bool computeOptimizedAggregatesIfPossible(ResultTable* result);
  // TODO<joka921> Comment all the other functions.

  bool computeOptimizedAggregatesOnIndexScanChild(ResultTable* result,
                                                  const IndexScan* indexScan);
  struct OptimizedAggregateData {
    const QueryExecutionTree& otherSubtree_;
    Index::Permutation permutation_;
    size_t subtreeColumnIndex_;
  };
  std::optional<OptimizedAggregateData>
  checkIfOptimizedAggregateOnJoinChildIsPossible(const Join* join);
  bool computeOptimizedAggregatesOnJoinChild(ResultTable* result,
                                             const Join* ptr);

  static std::optional<Index::Permutation> getPermutationForThreeVariableTriple(
      const QueryExecutionTree* tree, const Variable& variableByWhichToSort);

  // TODO<joka921> check and resolve Hannah's engine crashes when using the
  // optimized aggregates

  // TODO<joka921> implement optimization when *additional* Variables are
  // grouped.
  // TODO<joka921> implement optimization when there are additional aggregates
  // that work on the variables that are NOT part of the three-variable-triple.

  // TODO<joka921> Also inform the query planner (via the cost estimate)
  // that the optimization can be done.

  // TODO<joka921> Throw out `subjectCardinality` etc. functions in
  // `Index(Impl)`, they are only used in one place.
};
