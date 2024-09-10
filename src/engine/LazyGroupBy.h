//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include "engine/GroupBy.h"

template <size_t NUM_GROUP_COLUMNS>
class LazyGroupBy {
  LocalVocab& localVocab_;
  std::vector<GroupBy::HashMapAliasInformation> aggregateAliases_;
  GroupBy::HashMapAggregationData<NUM_GROUP_COLUMNS> aggregationData_;

 public:
  LazyGroupBy(LocalVocab& localVocab,
              std::vector<GroupBy::HashMapAliasInformation> aggregateAliases,
              const ad_utility::AllocatorWithLimit<Id>& allocator);

  LazyGroupBy(const LazyGroupBy&) = delete;
  LazyGroupBy(LazyGroupBy&&) = delete;
  LazyGroupBy& operator=(const LazyGroupBy&) = delete;
  LazyGroupBy& operator=(LazyGroupBy&&) = delete;

  void commitRow(IdTable& resultTable,
                 sparqlExpression::EvaluationContext& evaluationContext,
                 const std::vector<std::pair<size_t, Id>>& currentGroupBlock,
                 const GroupBy& groupBy);

  void processNextBlock(sparqlExpression::EvaluationContext& evaluationContext,
                        size_t beginIndex, size_t endIndex);

 private:
  void resetAggregationData();

  void addToAggregateFunction(size_t aggregateIndex,
                              const std::variant<ValueId, LocalVocabEntry>& id,
                              const sparqlExpression::EvaluationContext* ctx);
};
