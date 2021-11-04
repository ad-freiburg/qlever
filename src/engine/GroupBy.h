// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../parser/ParsedQuery.h"
#include "./Operation.h"
#include "./QueryExecutionTree.h"
#include "sparqlExpressions/SparqlExpressionPimpl.h"

using std::string;
using std::vector;

class GroupBy : public Operation {
 public:
  /**
   * @brief Represents an aggregate alias in the select part of the query.
   */
  struct Aggregate {
    sparqlExpression::SparqlExpressionPimpl _expression;
    size_t _outCol;
  };

  /**
   * @brief This constructor does not take a subtree as an argument to allow
   *        for creating the GroupBy operation before the OrderBy operation
   *        that is required by this GroupBy. This prevents having to compute
   *        the order of the aggregate aliases twice and group by columns
   *        in two places. The subtree must be set by calling setSubtree
   *        before calling computeResult
   * @param qec
   * @param groupByVariables
   * @param aliases
   */
  GroupBy(QueryExecutionContext* qec, vector<string> groupByVariables,
          std::vector<ParsedQuery::Alias> aliases);

  virtual string asString(size_t indent = 0) const override;

  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<size_t> resultSortedOn() const override;

  ad_utility::HashMap<string, size_t> getVariableColumns() const override;

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
   * @brief To allow for creating a OrderBy Operation after the GroupBy
   *        the subtree is not an argument to the constructor, as it is with
   *        other operations. Instead it needs to be set using this function.
   * @param subtree The QueryExecutionTree that contains the operations creating
   *                this operations input.
   */
  void setSubtree(std::shared_ptr<QueryExecutionTree> subtree);

  /**
   * @return The columns on which the input data should be sorted or an empty
   *         list if no particular order is required for the grouping.
   * @param inputTree The QueryExecutionTree that contains the operations
   *                  creating the sorting operation inputs.
   */
  vector<pair<size_t, bool>> computeSortColumns(
      const QueryExecutionTree* inputTree);

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

 private:
  std::shared_ptr<QueryExecutionTree> _subtree;
  vector<string> _groupByVariables;
  std::vector<ParsedQuery::Alias> _aliases;
  ad_utility::HashMap<string, size_t> _varColMap;

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
};
