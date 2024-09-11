//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include "engine/GroupBy.h"

class LazyGroupBy {
  LocalVocab& localVocab_;
  std::vector<GroupBy::HashMapAliasInformation> aggregateAliases_;
  GroupBy::HashMapAggregationData<0> aggregationData_;

 public:
  LazyGroupBy(LocalVocab& localVocab,
              std::vector<GroupBy::HashMapAliasInformation> aggregateAliases,
              const ad_utility::AllocatorWithLimit<Id>& allocator,
              size_t numGroupColumns);

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
};
