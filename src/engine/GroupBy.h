// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018      Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
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

  virtual vector<ColumnIndex> resultSortedOn() const override;

  virtual void setTextLimit(size_t limit) override {
    _subtree->setTextLimit(limit);
  }

  virtual bool knownEmptyResult() override {
    return _subtree->knownEmptyResult();
  }

  virtual float getMultiplicity(size_t col) override;

 private:
  uint64_t getSizeEstimateBeforeLimit() override;

 public:
  virtual size_t getCostEstimate() override;

  /**
   * @return The columns on which the input data should be sorted or an empty
   *         list if no particular order is required for the grouping.
   * @param subtree The QueryExecutionTree that contains the operations
   *                  creating the sorting operation inputs.
   */
  vector<ColumnIndex> computeSortColumns(const QueryExecutionTree* subtree);

  vector<QueryExecutionTree*> getChildren() override {
    return {_subtree.get()};
  }

 private:
  VariableToColumnMap computeVariableToColumnMap() const override;

  ResultTable computeResult() override;

  template <size_t OUT_WIDTH>
  void processGroup(const Aggregate& expression,
                    sparqlExpression::EvaluationContext& evaluationContext,
                    size_t blockStart, size_t blockEnd,
                    IdTableStatic<OUT_WIDTH>* result, size_t resultRow,
                    size_t resultColumn, LocalVocab* localVocab) const;

  template <size_t IN_WIDTH, size_t OUT_WIDTH>
  void doGroupBy(const IdTable& dynInput, const vector<size_t>& groupByCols,
                 const vector<GroupBy::Aggregate>& aggregates,
                 IdTable* dynResult, const IdTable* inTable,
                 LocalVocab* outLocalVocab) const;

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
  bool computeOptimizedGroupByIfPossible(IdTable*);

  // First, check if the query represented by this GROUP BY is of the following
  // form:
  //  SELECT (COUNT (?x) as ?cnt) WHERE {
  //    ?x <somePredicate> ?y
  //  }
  // The single triple must contain two or three variables, and the fixed value
  // in the two variable case might also be the subject or object of the triple.
  // The COUNT may be computed on any of the variables in the triple. If the
  // query has that form, the result of the query (which consists of one line)
  // is computed and stored in the `result` and `true` is returned. If not, the
  // `result` is left untouched, and `false` is returned.
  bool computeGroupByForSingleIndexScan(IdTable* result);

  // First, check if the query represented by this GROUP BY is of the following
  // form:
  //  SELECT ?x (COUNT(?x) as ?cnt) WHERE {
  //    ?x ?y ?z
  //  } GROUP BY ?x
  // The single triple must contain three variables. The grouped variable and
  // the selected variable must be the same, but may be either one of `?x, `?y`,
  // or `?z`. In the SELECT clause, both of the elements may be omitted, so in
  // the example it is possible to only select `?x` or to only select the
  // `COUNT`.
  bool computeGroupByForFullIndexScan(IdTable* result);

  // First, check if the query represented by this GROUP BY is of the following
  // form:
  //  SELECT ?x (COUNT (?x) as ?cnt) WHERE {
  //    %arbitrary graph pattern that contains `?x`, but neither `?y`, nor `?z`.
  //    ?x ?y ?z
  //  } GROUP BY ?x
  // Note that `?x` can also be the predicate or object of the three variable
  // triple, and that the COUNT may be by any of the variables `?x`, `?y`, or
  // `?z`.
  bool computeGroupByForJoinWithFullScan(IdTable* result);

  // Data to perform the AVG aggregation using the HashMap optimization.
  struct AverageAggregationData {
    bool error_ = false;
    double sum_ = 0;
    int64_t count_ = 0;
    void increment(sparqlExpression::detail::NumericValue intermediate) {
      if (const int64_t* intval = std::get_if<int64_t>(&intermediate))
        sum_ += (double)*intval;
      else if (const double* dval = std::get_if<double>(&intermediate))
        sum_ += *dval;
      else
        error_ = true;
      ++count_;
    };
    [[nodiscard]] ValueId calculateResult() const {
      if (error_)
        return ValueId::makeUndefined();
      else
        return ValueId::makeFromDouble(sum_ / static_cast<double>(count_));
    }
  };

  using KeyType = ValueId;
  template <size_t numAggregates>
  using ValueType = std::array<AverageAggregationData, numAggregates>;

  // Stores information required for substitution of an expression in an
  // expression tree.
  struct ParentAndChildIndex {
    sparqlExpression::SparqlExpression* parent_;
    size_t nThChild_;

    ParentAndChildIndex(sparqlExpression::SparqlExpression* parent,
                        size_t nThChild)
        : parent_{parent}, nThChild_{nThChild} {
      AD_CONTRACT_CHECK(parent != nullptr);
    }
  };

  // Stores information required for evaluation of an aggregate as well
  // as the alias containing it.
  struct HashMapAggregateInformation {
    // The expression of this aggregate.
    sparqlExpression::SparqlExpression* expr_;
    // The index in the `std::array` of the Hash Map where results of this
    // aggregate are stored.
    size_t hashMapIndex_;
    // The parent expression of this aggregate, and the index this expression
    // appears in the parents' children, so that it may be substituted away.
    std::optional<ParentAndChildIndex> parentAndIndex_ = std::nullopt;

    HashMapAggregateInformation(
        sparqlExpression::SparqlExpression* expr, size_t hashMapIndex,
        std::optional<ParentAndChildIndex> parentAndIndex = std::nullopt)
        : expr_{expr},
          hashMapIndex_{hashMapIndex},
          parentAndIndex_{parentAndIndex} {
      AD_CONTRACT_CHECK(expr != nullptr);
    }
  };

  // Stores alias information, especially all aggregates contained
  // in an alias.
  struct HashMapAliasInformation {
    // The expression of this alias.
    sparqlExpression::SparqlExpressionPimpl expr_;
    // The column where the result will be stored in the output.
    size_t outCol_;
    // Information about all aggregates contained in this alias.
    std::vector<HashMapAggregateInformation> aggregateInfo_;
  };

  // Required data to perform HashMap optimization.
  struct HashMapOptimizationData {
    // The index of the column that is grouped by.
    size_t subtreeColumnIndex_;
    // All aliases and the aggregates they contain.
    std::vector<HashMapAliasInformation> aggregateAliases_;
    // The total number of aggregates.
    size_t numAggregates_;
  };

  // Create result IdTable by using a HashMap mapping groups to aggregation data
  // and subsequently calling `createResultFromHashMap`.
  template <size_t OUT_WIDTH, size_t numAliases, size_t numAggregates>
  void computeGroupByForHashMapOptimization(
      IdTable* result, std::vector<HashMapAliasInformation>& aggregateAliases,
      const IdTable& subresult, size_t columnIndex, LocalVocab* localVocab);

  // Sort the HashMap by key and create result table.
  template <size_t OUT_WIDTH, size_t numAliases, size_t numAggregates>
  void createResultFromHashMap(
      IdTable* result,
      const ad_utility::HashMapWithMemoryLimit<KeyType,
                                               ValueType<numAggregates>>& map,
      std::vector<HashMapAliasInformation>& aggregateAliases,
      LocalVocab* localVocab);

  // Check if hash map optimization is applicable. This is the case when
  // the following conditions hold true:
  // - Runtime parameter is set
  // - Child operation is SORT
  // - All aggregates are AVG
  // - Maximum 5 aliases and 5 aggregates
  // - Only one grouped variable
  std::optional<HashMapOptimizationData> checkIfHashMapOptimizationPossible(
      std::vector<Aggregate>& aggregates);

  // Extract values from `expressionResult` and store them in the rows of
  // `resultTable` specified by the indices in `evaluationContext`, in column
  // `outCol`.
  static void extractValues(
      sparqlExpression::ExpressionResult&& expressionResult,
      sparqlExpression::EvaluationContext& evaluationContext,
      IdTable* resultTable, LocalVocab* localVocab, size_t outCol);

  // In case of aliases that contain an aggregate at the top-level of the
  // expression tree, e.g. "AVG(?y) as ?avg", we can directly store the values
  // calculated in our HashMap in the result table.
  template <size_t numAggregates>
  sparqlExpression::VectorWithMemoryLimit<ValueId> extractValuesDirectlyFromMap(
      size_t beginIndex, size_t endIndex,
      const ad_utility::HashMapWithMemoryLimit<KeyType,
                                               ValueType<numAggregates>>& map,
      IdTable* resultTable, size_t hashMapIndex, size_t outCol);

  // Substitute the results for all aggregates in `info`. The values of the
  // grouped variable should be at column 0 in `groupValues`.
  template <size_t numAggregates>
  void substituteAllAggregates(
      std::vector<HashMapAggregateInformation>& info,
      sparqlExpression::EvaluationContext& evaluationContext,
      const ad_utility::HashMapWithMemoryLimit<KeyType,
                                               ValueType<numAggregates>>& map,
      IdTable* resultTable);

  // Check if an expression is of a certain type.
  template <class T>
  static bool hasType(sparqlExpression::SparqlExpression* expr);

  // Check if an expression is a currently supported aggregate.
  // TODO<kcaliban> As soon as all aggregates are supported, implement and use a
  //                `isAggregate` function in SparqlExpressions instead.
  static bool isSupportedAggregate(sparqlExpression::SparqlExpression* expr);

  // Find all aggregates for expression `expr`. Return `std::nullopt`
  // if an unsupported aggregate is found.
  // TODO<kcaliban> Remove std::optional as soon as all aggregates are supported
  static std::optional<std::vector<HashMapAggregateInformation>> findAggregates(
      sparqlExpression::SparqlExpression* expr);

  // The recursive implementation of `findAggregates` (see above).
  static bool findAggregatesImpl(
      sparqlExpression::SparqlExpression* parent,
      sparqlExpression::SparqlExpression* expr, std::optional<size_t> index,
      std::vector<HashMapAggregateInformation>& info);

  // The check whether the optimization just described can be applied and its
  // actual computation are split up in two functions. This struct contains
  // information that has to be passed between the check and the computation.
  struct OptimizedGroupByData {
    // The three variable triple subtree
    const QueryExecutionTree& threeVariableTripleSubtree_;
    // The subtree of the `JOIN` operation that is *not* the three variable
    // triple.
    const QueryExecutionTree& otherSubtree_;
    // The permutation in which the three variable triple has to be sorted for
    // the JOIN, etc. `SPO` if the variable that joins the three variable triple
    // and the rest of the query body is the subject of the three variable
    // triple, `PSO` if it is the predicate, `OSP` if it is the object.
    Permutation::Enum permutation_;
    // The column index wrt the `otherSubtree_` of the variable that joins the
    // three variable triple and the rest of the query body.
    size_t subtreeColumnIndex_;
  };

  // Check if the previously described optimization can be applied. The argument
  // Must be the single subtree of this GROUP BY, properly cast to a `const
  // Join*`.
  std::optional<OptimizedGroupByData> checkIfJoinWithFullScan(const Join* join);

  // Check if the following is true: the `tree` represents a three variable
  // triple, that contains both `variableByWhichToSort` and
  // `variableThatMustBeContained`. (They might be the same). If this check
  // fails, `std::nullopt` is returned. Else the permutation corresponding to
  // `variableByWhichToSort` is returned, for example `SPO` if the
  // `variableByWhichToSort` is the subject of the triple.
  static std::optional<Permutation::Enum> getPermutationForThreeVariableTriple(
      const QueryExecutionTree& tree, const Variable& variableByWhichToSort,
      const Variable& variableThatMustBeContained);

  // If this GROUP BY has exactly one alias, and that alias is a non-distinct
  // count of a single variable, return that variable. Else return
  // `std::nullopt`.
  std::optional<Variable> getVariableForNonDistinctCountOfSingleAlias() const;

  // If this GROUP BY has exactly one alias, and that alias is a
  // count (can be distinct or not) of a single variable, return that variable
  // and the distinctness of the count. Else return `std::nullopt`.
  std::optional<
      sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>
  getVariableForCountOfSingleAlias() const;

  // TODO<joka921> implement optimization when *additional* Variables are
  // grouped.

  // TODO<joka921> implement optimization when there are additional aggregates
  // that work on the variables that are NOT part of the three-variable-triple.

  // TODO<joka921> Also inform the query planner (via the cost estimate)
  // that the optimization can be done.
};
