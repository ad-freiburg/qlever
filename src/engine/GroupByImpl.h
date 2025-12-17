// Copyright 2018 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures.
// Authors: Florian Kramer [2018]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_GROUPBYIMPL_H
#define QLEVER_SRC_ENGINE_GROUPBYIMPL_H

#include <gtest/gtest_prod.h>

#include <boost/optional.hpp>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "engine/GroupByHashMapOptimization.h"
#include "engine/Join.h"
#include "engine/Operation.h"
#include "engine/QueryExecutionTree.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "parser/Alias.h"
#include "util/TypeIdentity.h"

// Block size for when using the hash map optimization
static constexpr size_t GROUP_BY_HASH_MAP_BLOCK_SIZE = 262144;

namespace groupBy::detail {
template <size_t IN_WIDTH, size_t OUT_WIDTH>
class LazyGroupByRange;
}

class GroupByImpl : public Operation {
 public:
  using GroupBlock = std::vector<std::pair<size_t, Id>>;

 private:
  using string = std::string;
  template <typename T>
  using vector = std::vector<T>;
  using Allocator = ad_utility::AllocatorWithLimit<Id>;

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

  GroupByImpl(QueryExecutionContext* qec, vector<Variable> groupByVariables,
              std::vector<Alias> aliases,
              std::shared_ptr<QueryExecutionTree> subtree);

 public:
  virtual string getCacheKeyImpl() const override;

 public:
  virtual string getDescriptor() const override;

  virtual size_t getResultWidth() const override;

  virtual vector<ColumnIndex> resultSortedOn() const override;

  virtual bool knownEmptyResult() override {
    // Implicit group by always returns a single row.
    return _subtree->knownEmptyResult() && !_groupByVariables.empty();
  }

  virtual float getMultiplicity(size_t col) override;

 public:
  uint64_t getSizeEstimateBeforeLimit() override;
  size_t getCostEstimate() override;

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

  // Getters for testing
  const auto& groupByVariables() const { return _groupByVariables; }
  const auto& aliases() const { return _aliases; }

  struct HashMapAliasInformation;

 public:
  VariableToColumnMap computeVariableToColumnMap() const override;

  Result computeResult(bool requestLaziness) override;

 private:
  // Helper function to create evaluation contexts in various places for the
  // GROUP BY operation.
  sparqlExpression::EvaluationContext createEvaluationContext(
      LocalVocab& localVocab, const IdTable& idTable) const;

  // Find the boundaries of blocks in a sorted `IdTable`. If these represent a
  // whole group they can be aggregated into ids afterwards. This can happen by
  // passing a proper callback as `onBlockChange`, or by using the returned
  // `size_t` which represents the start of the last block. When
  // `onBlockChanged` is invoked, it will be called with two indices
  // representing the interval [start, stop) of the passed `idTable`. Because
  // some group might be bigger than the `idTable` the end of it is not treated
  // as a boundary and thus `onBlockChange` will not be invoked. Instead this
  // function returns the starting index of the last block of this `idTable`.
  // The argument `currentGroupBlock` is used to store the values of the group
  // by columns for the current group.
  template <int COLS, typename T>
  QL_CONCEPT_OR_NOTHING(requires ranges::invocable<T, size_t, size_t>)
  size_t searchBlockBoundaries(const T& onBlockChange,
                               const IdTableView<COLS>& idTable,
                               GroupBlock& currentGroupBlock) const;

  // Helper function to process a sorted group within a single id table.
  template <size_t OUT_WIDTH>
  void processBlock(IdTableStatic<OUT_WIDTH>& output,
                    const std::vector<Aggregate>& aggregates,
                    sparqlExpression::EvaluationContext& evaluationContext,
                    size_t blockStart, size_t blockEnd, LocalVocab* localVocab,
                    const vector<size_t>& groupByCols) const;

  // Handle queries like `SELECT (COUNT(?x) AS ?c) WHERE {...}` with conditions
  // that result in an empty result set with implicit GROUP BY where we have to
  // return a single line as a result.
  template <size_t OUT_WIDTH>
  void processEmptyImplicitGroup(IdTable& resultTable,
                                 const std::vector<Aggregate>& aggregates,
                                 LocalVocab* localVocab) const;

  // Similar to `doGroupBy`, but works with a a `subresult` that is not fully
  // materialized, where the generator yields id tables in where the rows are
  // sorted. Yields the same number of id tables as the input generator,
  // skipping empty tables unless `singleIdTable` is set which causes the
  // function to yield a single id table with the complete result.
  template <size_t IN_WIDTH, size_t OUT_WIDTH>
  Result::LazyResult computeResultLazily(
      std::shared_ptr<const Result> subresult,
      std::vector<Aggregate> aggregates,
      std::vector<HashMapAliasInformation> aggregateAliases,
      std::vector<size_t> groupByCols, bool singleIdTable) const;

  template <size_t OUT_WIDTH>
  void processGroup(const Aggregate& expression,
                    sparqlExpression::EvaluationContext& evaluationContext,
                    size_t blockStart, size_t blockEnd,
                    IdTableStatic<OUT_WIDTH>* result, size_t resultRow,
                    size_t resultColumn, LocalVocab* localVocab) const;

  template <size_t IN_WIDTH, size_t OUT_WIDTH>
  IdTable doGroupBy(const IdTable& inTable, const vector<size_t>& groupByCols,
                    const vector<Aggregate>& aggregates,
                    LocalVocab* outLocalVocab) const;

  template <size_t IN_WIDTH, size_t OUT_WIDTH>
  friend class groupBy::detail::LazyGroupByRange;

  FRIEND_TEST(GroupByTest, doGroupBy);
  // Friend tests for internals used in hash-map optimization.
  FRIEND_TEST(GroupByHashMapOptimizationTest,
              MakeGroupValueSpans_SelectColumns);
  FRIEND_TEST(GroupByHashMapOptimizationTest,
              UpdateHashMapWithTable_CountSingleBlock);
  FRIEND_TEST(GroupByHashMapOptimizationTest,
              UpdateHashMapWithTable_CountTwoBlocksOverlap);
  FRIEND_TEST(GroupByHashMapOptimizationTest,
              UpdateHashMapWithTable_OnlyMatchingNonmatchingList);

 public:
  // TODO<joka921> use `FRIEND_TEST` here once we have converged on the set
  // of tests to write.

  // For certain combinations of `_groupByColumns`, `_aliases` and `_subtree`,
  // it is not necessary to fully materialize the `_subtree`'s result, but the
  // result of the GROUP BY can be computed directly from the index meta data.
  // See the functions below for examples and details.
  //
  // This function checks whether such a case applies. In this case the result
  // is computed and returned wrapped in a optional. If no such case applies the
  // optional remains empty.
  std::optional<IdTable> computeOptimizedGroupByIfPossible() const;

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
  // is computed and returned. If not, an empty optional is returned.
  std::optional<IdTable> computeGroupByForSingleIndexScan() const;

  // Check if the query represented by this GROUP BY is of the following form:
  //
  //   SELECT ?y (COUNT(?y) as ?count) WHERE {
  //     ?x <somePredicate> ?y
  //   } GROUP BY ?y
  //
  // NOTE: This is exactly what we need for a context-sensitive object AC query
  // without connected triples. The GROUP BY variable can also be omitted in
  // the SELECT clause.
  std::optional<IdTable> computeGroupByObjectWithCount() const;

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
  std::optional<IdTable> computeGroupByForFullIndexScan() const;

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
  std::optional<IdTable> computeGroupByForJoinWithFullScan() const;

  // Compute the result for a single `COUNT(*)` aggregate with a single
  // (implicit) group.
  std::optional<IdTable> computeCountStar() const;

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
  enum class HashMapAggregateType {
    AVG,
    COUNT,
    MIN,
    MAX,
    SUM,
    GROUP_CONCAT,
    SAMPLE
  };

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

    // OPTIONAL PARAMETERS, only required for hybrid fallback.
    // They are used to bundle the parameters, so that we do not have to pass
    // them around separately.
    // > `localVocabRef` is a non-owning reference to the LocalVocab used during
    // hash-map processing. It's initialized in
    // `computeGroupByForHashMapOptimization`.
    std::optional<std::vector<size_t>> columnIndices_;
    std::optional<std::vector<Aggregate>> aggregates_;
    boost::optional<LocalVocab&> localVocabRef_;

    // Default ctor: no columns or aggregates set
    HashMapOptimizationData() = default;
    // Build with just aliases
    explicit HashMapOptimizationData(
        std::vector<HashMapAliasInformation> aliases)
        : aggregateAliases_(std::move(aliases)) {}
    // Build with aliases, columns, and aggregates
    HashMapOptimizationData(std::vector<HashMapAliasInformation> aliases,
                            std::vector<size_t> cols,
                            std::vector<Aggregate> aggs)
        : aggregateAliases_(std::move(aliases)),
          columnIndices_(std::move(cols)),
          aggregates_(std::move(aggs)) {}
  };

  // Create result IdTable by using a HashMap mapping groups to aggregation data
  // and subsequently calling `createResultFromHashMap`.
  template <size_t NUM_GROUP_COLUMNS, typename SubResults>
  Result computeGroupByForHashMapOptimization(HashMapOptimizationData data,
                                              SubResults&& subresults) const;

  // Type returned by `getHashEntries(groupByCols, onlyUsePreexistingGroups)`.
  // If `onlyUsePreexistingGroups` is true, nonMatchingRows_ will only contain
  // rows that would create new groups.
  // If `onlyUsePreexistingGroups` is false, nonMatchingRows_ will be empty as
  // new groups are created for these rows.
  struct GroupLookupResult {
    // Describes the association between a single input row and the index of
    // the group that it belongs to in the hash map. Instances are created by
    // `getHashEntries` for every row whose group key already exists in the
    // map and are later iterated in ascending `rowIndex_` order to route the
    // evaluated aggregate values to the correct `groupIndex_` in the
    // aggregation data vectors.
    struct RowToGroup {
      size_t rowIndex_;
      size_t groupIndex_;

      bool operator==(const RowToGroup& other) const {
        return rowIndex_ == other.rowIndex_ && groupIndex_ == other.groupIndex_;
      }
    };

    // For each processed input row that matched an existing group (in order),
    // stores a `RowToGroup` mapping containing both the row's index in the
    // input and the index of its matching group in the hash map.
    std::vector<RowToGroup> matchedRowsToGroups_;
    // Indices of input rows for which no existing group was found.
    std::vector<size_t> nonMatchingRows_;
  };

  using AggregationData =
      std::variant<AvgAggregationData, CountAggregationData, MinAggregationData,
                   MaxAggregationData, SumAggregationData,
                   GroupConcatAggregationData, SampleAggregationData>;

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
          auto addIf = [this, &aggregate](auto ti,
                                          HashMapAggregateType target) {
            using T = typename decltype(ti)::type;
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
          addIf(ti<SampleAggregationData>, SAMPLE);

          AD_CORRECTNESS_CHECK(aggregationData_.size() ==
                               aggregationDataSize + 1);

          aggregateTypeWithData_.emplace_back(aggregate.aggregateType_);
        }
      }
    }

    // Returns the mapping of input rows (described by `groupByCols`) to
    // offsets in the internal hash map. If `onlyUsePreexistingGroups` is
    // `true`, rows whose key is not yet present in the map are not inserted
    // but their row indices are added to the `nonMatchingRows_` ("missing
    // indices") of the returned `GroupLookupResult`. If
    // `onlyUsePreexistingGroups` is `false`, such keys are inserted into the
    // hash map and no missing indices are reported for them.
    GroupLookupResult getHashEntries(
        const ArrayOrVector<ql::span<const Id>>& groupByCols,
        bool onlyUsePreexistingGroups);

    // Helper to build a single key vector from column-wise spans.
    ArrayOrVector<Id> makeKeyForHashMap(
        const ArrayOrVector<ql::span<const Id>>& spans, size_t row) const;

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
    [[nodiscard]] size_t numGroups() const { return map_.size(); }

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
  static sparqlExpression::VectorWithMemoryLimit<ValueId>
  getHashMapAggregationResults(
      IdTable* resultTable,
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      size_t dataIndex, size_t beginIndex, size_t endIndex,
      LocalVocab* localVocab, const Allocator& allocator);

  // Helper function of `evaluateAlias`.
  // 1. In the Expressions for the aliases of this GROUP BY, replace all
  //    aggregates and all occurrences of the grouped variables values that have
  //    been precomputed.
  // 2. Evaluate the (partially substituted) expressions using the
  //    `evaluationContext`, to get the final values of the aliases and store
  //    them in the `result`.
  // 3. Undo the substitution, s.t. we can reuse the expressions for additional
  //    blocks of values etc.
  //
  // Basically, perform the fallback case if no faster approach can be chosen
  // instead.
  template <size_t NUM_GROUP_COLUMNS>
  static void substituteAndEvaluate(
      HashMapAliasInformation& alias, IdTable* result,
      sparqlExpression::EvaluationContext& evaluationContext,
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      LocalVocab* localVocab, const Allocator& allocator,
      std::vector<HashMapAggregateInformation>& info,
      const std::vector<HashMapGroupedVariableInformation>& substitutions);

  // Substitute away any occurrences of the grouped variable and of aggregate
  // results, if necessary, and subsequently evaluate the expression of an
  // alias
  template <size_t NUM_GROUP_COLUMNS>
  static void evaluateAlias(
      HashMapAliasInformation& alias, IdTable* result,
      sparqlExpression::EvaluationContext& evaluationContext,
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      LocalVocab* localVocab, const Allocator& allocator);

  // Helper function to evaluate the child expression of an aggregate function.
  // Only `COUNT(*)` does not have a single child, so we make a special case for
  // it.
  static sparqlExpression::ExpressionResult
  evaluateChildExpressionOfAggregateFunction(
      const HashMapAggregateInformation& aggregate,
      sparqlExpression::EvaluationContext& evaluationContext);

  /**
   * @brief Processes all aggregate aliases for a single block of rows.
   *
   * For each alias and each aggregate inside that alias:
   * - evaluate the aggregate's inner expression on the rows in this block
   *   using the given `evaluationContext`, and
   * - send the resulting values into the matching aggregation data
   *   structures via `makeProcessGroupsVisitor`.
   */
  template <size_t NUM_GROUP_COLUMNS>
  static void processAggregateAliasesForBlock(
      const GroupLookupResult& lookupResult,
      const HashMapOptimizationData& data,
      HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      sparqlExpression::EvaluationContext& evaluationContext);

  // Helper that creates the visitor used in
  // `processAggregateAliasesForBlock` to route values from the evaluation
  // of an aggregate's child expression into the corresponding aggregation
  // data structures.
  static constexpr auto makeProcessGroupsVisitor(
      size_t blockSize,
      const sparqlExpression::EvaluationContext* evaluationContext,
      const GroupLookupResult& groupLookupResult);

  // Sort the HashMap by key and create result table.
  template <size_t NUM_GROUP_COLUMNS>
  IdTable createResultFromHashMap(
      const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      std::vector<HashMapAliasInformation>& aggregateAliases,
      LocalVocab* localVocab) const;

  // Reusable implementation of `checkIfHashMapOptimizationPossible`.
  static std::optional<HashMapOptimizationData>
  computeUnsequentialProcessingMetadata(
      std::vector<Aggregate>& aggregates,
      const std::vector<Variable>& groupByVariables);

  // Check if hash map optimization is applicable. This is the case when
  // the following conditions hold true:
  // - Runtime parameter is set
  // - Child operation is SORT
  std::optional<HashMapOptimizationData> checkIfHashMapOptimizationPossible(
      std::vector<Aggregate>& aggregates) const;

  // Extract values from `expressionResult` and store them in the rows of
  // `resultTable` specified by the indices in `evaluationContext`, in column
  // `outCol`.
  static void extractValues(
      sparqlExpression::ExpressionResult&& expressionResult,
      sparqlExpression::EvaluationContext& evaluationContext,
      IdTable* resultTable, LocalVocab* localVocab, size_t outCol);

  // Substitute the group values for all occurrences of a group variable. Return
  // a vector of the replaced `SparqlExpression`s to potentially put them pack
  // afterwards.
  static std::vector<std::unique_ptr<sparqlExpression::SparqlExpression>>
  substituteGroupVariable(const std::vector<ParentAndChildIndex>& occurrences,
                          IdTable* resultTable, size_t beginIndex, size_t count,
                          size_t columnIndex, const Allocator& allocator);

  // Substitute the results for all aggregates in `info`. The values of the
  // grouped variable should be at column 0 in `groupValues`. Return a vector of
  // the replaced `SparqlExpression`s to potentially put them pack afterwards.
  template <size_t NUM_GROUP_COLUMNS>
  std::
      vector<std::unique_ptr<sparqlExpression::SparqlExpression>> static substituteAllAggregates(
          std::vector<HashMapAggregateInformation>& info, size_t beginIndex,
          size_t endIndex,
          const HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
          IdTable* resultTable, LocalVocab* localVocab,
          const Allocator& allocator);

  // Check if an expression is a currently supported aggregate.
  static std::optional<HashMapAggregateTypeWithData> isSupportedAggregate(
      sparqlExpression::SparqlExpression* expr);

  // Find all occurrences of grouped by variable for expression `expr`.
  static std::variant<std::vector<ParentAndChildIndex>, OccurAsRoot>
  findGroupedVariable(sparqlExpression::SparqlExpression* expr,
                      const Variable& groupedVariable);

  // The recursive implementation of `findGroupedVariable` (see above).
  static void findGroupedVariableImpl(
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
  std::optional<OptimizedGroupByData> checkIfJoinWithFullScan(
      const Join& join) const;

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

  // Return true if the `variable` is part of the result of the subtree of this
  // GROUP BY. This is used by some of the optimizations above.
  bool isVariableBoundInSubtree(const Variable& variable) const;

 public:
  std::unique_ptr<Operation> cloneImpl() const override;

  // TODO<joka921> implement optimization when *additional* Variables are
  // grouped.

  // TODO<joka921> implement optimization when there are additional aggregates
  // that work on the variables that are NOT part of the three-variable-triple.

  // TODO<joka921> Also inform the query planner (via the cost estimate)
  // that the optimization can be done.

 private:
  struct HashMapTimers {
    ad_utility::Timer lookupTimer_;
    ad_utility::Timer aggregationTimer_;
  };

  // Result struct for updateHashMapWithTable
  struct UpdateResult {
    std::vector<size_t> nonMatchingRows_;
    bool thresholdExceeded_ = false;
  };

  // Load entries from the given table into the hash map, optionally only
  // matching rows. Returns both non-matching rows and whether threshold was
  // exceeded.
  template <size_t NUM_GROUP_COLUMNS>
  UpdateResult updateHashMapWithTable(
      const IdTable& table, const HashMapOptimizationData& data,
      HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
      HashMapTimers& timers, bool onlyUsePreexistingGroups = false) const;

  // Build spans for the group values in the given columns of the given table,
  // starting at `beginIdx` and spanning `blockSize` rows.
  template <size_t NUM_GROUP_COLUMNS>
  typename HashMapAggregationData<NUM_GROUP_COLUMNS>::template ArrayOrVector<
      ql::span<const Id>>
  makeGroupValueSpans(const IdTable& table, size_t beginIdx, size_t blockSize,
                      const std::vector<ColumnIndex>& cols) const;

  /** Helper function to handle the remainder of the input after the hash map
   * threshold has been exceeded.
   *
   * Hybrid approach summary:
   * - While processing input blocks we insert all rows into a hash map and
   *   maintain aggregation state for groups already present in the map.
   * - If the number of groups exceeds a runtime-configured threshold we
   *   switch to a hybrid fallback: we keep the hash map results for groups
   *   already discovered, and buffer the remaining rows (those that would
   *   create new groups) into a temporary table. The buffered rows are then
   *   processed using a sort-based grouping and merged with the hash-map
   *   results.
   *
   * @tparam NUM_GROUP_COLUMNS: number of grouping columns used by the HashMap
   *   aggregation data (compile-time constant; may be 0).
   * @tparam BlockIterator: an input iterator type that points into the range of
   *   subresults (each element is expected to be a pair/reference of the
   *   form (IdTable, LocalVocab)). This iterator is advanced from the
   *   current position to the end.
   * @tparam BlocksEnd: the corresponding end/sentinel type for the input range.
   * In many cases this is the same type as BlockIterator, but it may be a
   *   distinct sentinel type for ranges that use sentinels.
   * @param restTable A partially filled table that already contains the rows
   *   from the current block that would have created new groups in the hash
   *   map. Additional rows collected from subsequent blocks will be appended
   *   to this table inside the function before the sort-based fallback is
   *   executed.
   *
   * The function consumes the remaining input starting at `blockIt`
   * up to `blocksEnd` and returns the final `Result` consisting of the
   * hash-map entries and the grouped fallback rows.
   */
  CPP_template_def(size_t NUM_GROUP_COLUMNS, typename BlockIterator,
                   typename BlocksEnd)(
      requires ql::concepts::input_iterator<BlockIterator>&&
          ql::concepts::sentinel_for<BlocksEnd, BlockIterator>) Result
      handleRemainderUsingHybridApproach(
          HashMapOptimizationData data,
          HashMapAggregationData<NUM_GROUP_COLUMNS>& aggregationData,
          HashMapTimers& timers, BlockIterator blockIt, BlocksEnd blocksEnd,
          IdTable restTable) const;
};

// _____________________________________________________________________________
namespace groupBy::detail {
template <typename A>
CPP_concept VectorOfAggregationData =
    ad_utility::SameAsAnyTypeIn<A, GroupByImpl::AggregationDataVectors>;
}

#endif  // QLEVER_SRC_ENGINE_GROUPBYIMPL_H
