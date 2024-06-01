// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2018      Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//   2020-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <gtest/gtest_prod.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "engine/GroupByHashMapOptimization.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/RelationalExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "parser/Alias.h"
#include "parser/ParsedQuery.h"
#include "util/TypeIdentity.h"

using std::string;
using std::vector;

// Forward declarations for internal member function
class IndexScan;
class Join;

// Block size for when using the hash map optimization
static constexpr size_t GROUP_BY_HASH_MAP_BLOCK_SIZE = 262144;

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
  virtual string getCacheKeyImpl() const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<ColumnIndex> resultSortedOn() const override;

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

  Result computeResult([[maybe_unused]] bool requestLaziness) override;

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
  // it is not necessary to fully materialize the `_subtree`'s result, but the
  // result of the GROUP BY can be computed directly from the index meta data.
  // See the functions below for examples and details.
  //
  // This function takes an empty `result` as input and checks whether such a
  // case applies. In this case the result is computed and stored in `result`
  // and `true` is returned. If no such case applies, `false` is returned and
  // `result` remains empty.
  bool computeOptimizedGroupByIfPossible(IdTable* result);

  // Check if the query represented by this GROUP BY is of the following form:
  //
  //   SELECT (COUNT (?x) as ?count) WHERE {
  //     ?x <somePredicate> ?y
  //   }
  //
  // The single triple must contain two or three variables, and the fixed value
  // in the two variable case might also be the subject or object of the triple.
  // The COUNT may be computed on any of the variables in the triple. If the
  // query has that form, the result of the query (which consists of one line)
  // is computed and stored in the `result` and `true` is returned. If not, the
  // `result` is left untouched, and `false` is returned.
  bool computeGroupByForSingleIndexScan(IdTable* result);

  // Check if the query represented by this GROUP BY is of the following form:
  //
  //   SELECT ?y (COUNT(?y) as ?count) WHERE {
  //     ?x <somePredicate> ?y
  //   } GROUP BY ?y
  //
  // NOTE: This is exactly what we need for a context-sensitive object AC query
  // without connected triples. The GROUP BY variable can also be ommitted in
  // the SELECT clause.
  bool computeGroupByObjectWithCount(IdTable* result);

  // Check if the query represented by this GROUP BY is of the following form:
  //
  //   SELECT ?x (COUNT(?x) as ?count) WHERE {
  //     ?x ?y ?z
  //   } GROUP BY ?x
  //
  // The single triple must contain three variables. The grouped variable and
  // the selected variable must be the same, but may be either one of `?x, `?y`,
  // or `?z`. In the SELECT clause, both of the elements may be omitted, so in
  // the example it is possible to only select `?x` or to only select the
  // `COUNT`.
  bool computeGroupByForFullIndexScan(IdTable* result);

  // Check if the query represented by this GROUP BY is of the following form:
  //
  //   SELECT ?x (COUNT (?x) as ?count) WHERE {
  //     %any graph pattern that contains `?x`, but neither `?y`, nor `?z`.
  //     ?x ?y ?z
  //   } GROUP BY ?x
  //
  // Note that `?x` can also be the predicate or object of the three variable
  // triple, and that the COUNT may be by any of the variables `?x`, `?y`, or
  // `?z`.
  bool computeGroupByForJoinWithFullScan(IdTable* result);

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

  // Used to store the kind of aggregate.
  enum class HashMapAggregateType { AVG, COUNT, MIN, MAX, SUM, GROUP_CONCAT };

  // `GROUP_CONCAT` requires additional data.
  struct HashMapAggregateTypeWithData {
    HashMapAggregateType type_;
    std::optional<std::string> separator_ = std::nullopt;
  };

  // Stores information required for evaluation of an aggregate as well
  // as the alias containing it.
  struct HashMapAggregateInformation {
    // The expression of this aggregate.
    sparqlExpression::SparqlExpression* expr_;
    // The index in the vector of `HashMapAggregationData` where results of this
    // aggregate are stored.
    size_t aggregateDataIndex_;
    // The parent expression of this aggregate, and the index this expression
    // appears in the parents' children, so that it may be substituted away.
    std::optional<ParentAndChildIndex> parentAndIndex_ = std::nullopt;
    // Which kind of aggregate expression.
    HashMapAggregateTypeWithData aggregateType_;

    HashMapAggregateInformation(
        sparqlExpression::SparqlExpression* expr, size_t aggregateDataIndex,
        HashMapAggregateTypeWithData aggregateType,
        std::optional<ParentAndChildIndex> parentAndIndex = std::nullopt)
        : expr_{expr},
          aggregateDataIndex_{aggregateDataIndex},
          parentAndIndex_{parentAndIndex},
          aggregateType_{std::move(aggregateType)} {
      AD_CONTRACT_CHECK(expr != nullptr);
    }
  };

  // Determines whether the grouped by variable appears at the top of an
  // alias, e.g. `SELECT (?a as ?x) WHERE {...} GROUP BY ?a`.
  struct OccurAsRoot {};

  // Stores information required to substitute away all
  // grouped variables occurring inside an alias.
  struct HashMapGroupedVariableInformation {
    // The variable itself.
    Variable var_;
    // The column index in the final result.
    size_t resultColumnIndex_;
    // The occurrences of the grouped variable inside an alias.
    std::variant<std::vector<ParentAndChildIndex>, OccurAsRoot> occurrences_;
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
    // Information about all grouped variables contained in this alias.
    std::vector<HashMapGroupedVariableInformation> groupedVariables_;
  };

  // Required data to perform HashMap optimization.
  struct HashMapOptimizationData {
    // All aliases and the aggregates they contain.
    std::vector<HashMapAliasInformation> aggregateAliases_;
  };

  // Create result IdTable by using a HashMap mapping groups to aggregation data
  // and subsequently calling `createResultFromHashMap`.
  template <size_t NUM_GROUP_COLUMNS>
  void computeGroupByForHashMapOptimization(
      IdTable* result, std::vector<HashMapAliasInformation>& aggregateAliases,
      const IdTable& subresult, const std::vector<size_t>& columnIndices,
      LocalVocab* localVocab);

  using AggregationData =
      std::variant<AvgAggregationData, CountAggregationData, MinAggregationData,
                   MaxAggregationData, SumAggregationData,
                   GroupConcatAggregationData>;

  using AggregationDataVectors =
      ad_utility::LiftedVariant<AggregationData,
                                sparqlExpression::VectorWithMemoryLimit>;

  // Stores the map which associates Ids with vector offsets and
  // the vectors containing the aggregation data.
  template <size_t NUM_GROUP_COLUMNS>
  class HashMapAggregationData {
   public:
    template <typename A>
    using ArrayOrVector =
        std::conditional_t<NUM_GROUP_COLUMNS != 0,
                           std::array<A, NUM_GROUP_COLUMNS>, std::vector<A>>;

    HashMapAggregationData(
        const ad_utility::AllocatorWithLimit<Id>& alloc,
        const std::vector<HashMapAliasInformation>& aggregateAliases,
        size_t numOfGroupedColumns)
        : numOfGroupedColumns_{numOfGroupedColumns},
          alloc_{alloc},
          map_{alloc} {
      using enum HashMapAggregateType;
      for (const auto& alias : aggregateAliases) {
        for (const auto& aggregate : alias.aggregateInfo_) {
          using namespace ad_utility::use_type_identity;
          auto addIf = [this, &aggregate]<typename T>(
                           TI<T>, HashMapAggregateType target) {
            if (aggregate.aggregateType_.type_ == target)
              aggregationData_.emplace_back(
                  sparqlExpression::VectorWithMemoryLimit<T>{alloc_});
          };

          auto aggregationDataSize = aggregationData_.size();

          addIf(ti<AvgAggregationData>, AVG);
          addIf(ti<CountAggregationData>, COUNT);
          addIf(ti<MinAggregationData>, MIN);
          addIf(ti<MaxAggregationData>, MAX);
          addIf(ti<SumAggregationData>, SUM);
          addIf(ti<GroupConcatAggregationData>, GROUP_CONCAT);

          AD_CORRECTNESS_CHECK(aggregationData_.size() ==
                               aggregationDataSize + 1);

          aggregateTypeWithData_.emplace_back(aggregate.aggregateType_);
        }
      }
    }

    // Returns a vector containing the offsets for all ids of `groupByCols`,
    // inserting entries if necessary.
    std::vector<size_t> getHashEntries(
        const ArrayOrVector<std::span<const Id>>& groupByCols);

    // Return the index of `id`.
    [[nodiscard]] size_t getIndex(const ArrayOrVector<Id>& ids) const {
      return map_.at(ids);
    }

    // Get vector containing the aggregation data at `aggregationDataIndex`.
    AggregationDataVectors& getAggregationDataVariant(
        size_t aggregationDataIndex) {
      return aggregationData_.at(aggregationDataIndex);
    }

    // Get vector containing the aggregation data at `aggregationDataIndex`,
    // but const.
    [[nodiscard]] const AggregationDataVectors& getAggregationDataVariant(
        size_t aggregationDataIndex) const {
      return aggregationData_.at(aggregationDataIndex);
    }

    // Get the values of the grouped column in ascending order.
    [[nodiscard]] ArrayOrVector<std::vector<Id>> getSortedGroupColumns() const;

    // Returns the number of groups.
    [[nodiscard]] size_t getNumberOfGroups() const { return map_.size(); }

    // How many columns we are grouping by, important in case
    // `NUM_GROUP_COLUMNS` == 0.
    size_t numOfGroupedColumns_;

   private:
    // Allocator used for creating new vectors.
    const ad_utility::AllocatorWithLimit<Id>& alloc_;
    // Maps `Id` to vector offsets.
    ad_utility::HashMapWithMemoryLimit<ArrayOrVector<Id>, size_t> map_;
    // Stores the actual aggregation data.
    std::vector<AggregationDataVectors> aggregationData_;
    // For `GROUP_CONCAT`, we require the type information.
    std::vector<HashMapAggregateTypeWithData> aggregateTypeWithData_;
  };

  // Returns the aggregation results between `beginIndex` and `endIndex`
  // of the aggregates stored at `dataIndex`,
  // based on the groups stored in the first column of `resultTable`
  template <size_t NUM_GROUP_COLUMNS>
  sparqlExpression::VectorWithMemoryLimit<ValueId> getHashMapAggregationResults(
      IdTable* resultTable,
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      size_t dataIndex, size_t beginIndex, size_t endIndex,
      LocalVocab* localVocab);

  // Substitute away any occurrences of the grouped variable and of aggregate
  // results, if necessary, and subsequently evaluate the expression of an
  // alias
  template <size_t NUM_GROUP_COLUMNS>
  void evaluateAlias(
      HashMapAliasInformation& alias, IdTable* result,
      sparqlExpression::EvaluationContext& evaluationContext,
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      LocalVocab* localVocab);

  // Sort the HashMap by key and create result table.
  template <size_t NUM_GROUP_COLUMNS>
  void createResultFromHashMap(
      IdTable* result,
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      std::vector<HashMapAliasInformation>& aggregateAliases,
      LocalVocab* localVocab);

  // Check if hash map optimization is applicable. This is the case when
  // the following conditions hold true:
  // - Runtime parameter is set
  // - Child operation is SORT
  std::optional<HashMapOptimizationData> checkIfHashMapOptimizationPossible(
      std::vector<Aggregate>& aggregates);

  // Extract values from `expressionResult` and store them in the rows of
  // `resultTable` specified by the indices in `evaluationContext`, in column
  // `outCol`.
  static void extractValues(
      sparqlExpression::ExpressionResult&& expressionResult,
      sparqlExpression::EvaluationContext& evaluationContext,
      IdTable* resultTable, LocalVocab* localVocab, size_t outCol);

  // Substitute the group values for all occurrences of a group variable.
  void substituteGroupVariable(
      const std::vector<GroupBy::ParentAndChildIndex>& occurrences,
      IdTable* resultTable, size_t beginIndex, size_t count,
      size_t columnIndex) const;

  // Substitute the results for all aggregates in `info`. The values of the
  // grouped variable should be at column 0 in `groupValues`.
  template <size_t NUM_GROUP_COLUMNS>
  void substituteAllAggregates(
      std::vector<HashMapAggregateInformation>& info, size_t beginIndex,
      size_t endIndex,
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      IdTable* resultTable, LocalVocab* localVocab);

  // Check if an expression is of a certain type.
  template <class T>
  static std::optional<T*> hasType(sparqlExpression::SparqlExpression* expr);

  // Check if an expression is any of any type in `Exprs`
  template <typename... Exprs>
  static bool hasAnyType(const auto& expr);

  // Check if an expression is a currently supported aggregate.
  static std::optional<GroupBy::HashMapAggregateTypeWithData>
  isSupportedAggregate(sparqlExpression::SparqlExpression* expr);

  // Find all occurrences of grouped by variable for expression `expr`.
  std::variant<std::vector<ParentAndChildIndex>, OccurAsRoot>
  findGroupedVariable(sparqlExpression::SparqlExpression* expr,
                      const Variable& groupedVariable);

  // The recursive implementation of `findGroupedVariable` (see above).
  void findGroupedVariableImpl(
      sparqlExpression::SparqlExpression* expr,
      std::optional<ParentAndChildIndex> parentAndChildIndex,
      std::variant<std::vector<ParentAndChildIndex>, OccurAsRoot>&
          substitutions,
      const Variable& groupedVariable);

  // Find all aggregates for expression `expr`. Return `std::nullopt`
  // if an unsupported aggregate is found.
  // TODO<kcaliban> Remove std::optional as soon as all aggregates are
  // supported
  static std::optional<std::vector<HashMapAggregateInformation>> findAggregates(
      sparqlExpression::SparqlExpression* expr);

  // The recursive implementation of `findAggregates` (see above).
  static bool findAggregatesImpl(
      sparqlExpression::SparqlExpression* expr,
      std::optional<ParentAndChildIndex> parentAndChildIndex,
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
