//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include "engine/GroupBy.h"

template <size_t NUM_AGGREGATED_COLS>
class LazyGroupBy {
  LocalVocab& localVocab_;
  std::vector<GroupBy::HashMapAliasInformation> aggregateAliases_;
  const std::vector<size_t>& groupByCols_;
  std::array<GroupBy::AggregationData, NUM_AGGREGATED_COLS> aggregationData_;
  std::array<std::function<ValueId()>, NUM_AGGREGATED_COLS>
      calculationFunctions_{};
  std::array<std::function<void(const std::variant<ValueId, LocalVocabEntry>&,
                                const sparqlExpression::EvaluationContext*)>,
             NUM_AGGREGATED_COLS>
      addToAggregateFunctions_{};

  // This stores the values of the group by numColumns for the current block.
  // A block ends when one of these values changes.
  std::vector<std::pair<size_t, Id>> currentGroupBlock_{};

  // Store the call to reset in a `std::function` to avoid having to expensively
  // look up type of the variant on every new group that will always be the
  // same.
  std::function<void()> resetAggregationData_;

 public:
  LazyGroupBy(LocalVocab& localVocab,
              std::vector<GroupBy::HashMapAliasInformation> aggregateAliases,
              const std::vector<size_t>& groupByCols);

  LazyGroupBy(const LazyGroupBy&) = delete;
  LazyGroupBy(LazyGroupBy&&) = delete;
  LazyGroupBy& operator=(const LazyGroupBy&) = delete;
  LazyGroupBy& operator=(LazyGroupBy&&) = delete;

  void commitRow(IdTable& resultTable,
                 sparqlExpression::EvaluationContext& evaluationContext);

  void resetAggregationData() { resetAggregationData_(); }

  void processNextBlock(sparqlExpression::EvaluationContext& evaluationContext,
                        size_t beginIndex, size_t endIndex);

  void populateGroupBlock(const IdTable& idTable, size_t row);

  bool rowMatchesCurrentBlock(const IdTable& idTable, size_t row);

  bool noEntriesProcessed() const { return currentGroupBlock_.empty(); }

  std::vector<Id> getCurrentRow(size_t outputSize) const;

 private:
  ValueId calculateAggregateResult(size_t aggregateIndex) {
    return calculationFunctions_.at(aggregateIndex)();
  }

  void addToAggregateFunction(size_t aggregateIndex,
                              const std::variant<ValueId, LocalVocabEntry>& id,
                              const sparqlExpression::EvaluationContext* ctx) {
    return addToAggregateFunctions_.at(aggregateIndex)(id, ctx);
  }

  void substituteGroupVariable(
      const std::vector<GroupBy::ParentAndChildIndex>& occurrences,
      ValueId value);

  static std::array<GroupBy::AggregationData, NUM_AGGREGATED_COLS>
  initializeAggregationData(
      const std::vector<GroupBy::HashMapAliasInformation>& aggregateAliases);

  static std::function<void()> initializeResetAggregationData(
      std::array<GroupBy::AggregationData, NUM_AGGREGATED_COLS>&
          aggregationData);
};
