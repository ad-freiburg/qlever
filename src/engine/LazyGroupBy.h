//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include "engine/GroupBy.h"

// Helper class to lazily compute the result of a group by operation. It makes
// use of the hash map optimization to store the intermediate results of the
// groups.
class LazyGroupBy {
  LocalVocab& localVocab_;
  std::vector<GroupBy::HashMapAliasInformation> aggregateAliases_;
  const ad_utility::AllocatorWithLimit<Id>& allocator_;
  GroupBy::HashMapAggregationData<0> aggregationData_;

 public:
  LazyGroupBy(LocalVocab& localVocab,
              std::vector<GroupBy::HashMapAliasInformation> aggregateAliases,
              const ad_utility::AllocatorWithLimit<Id>& allocator,
              size_t numGroupColumns);

  // Delete copy and move functions to avoid unexpected behavior.
  LazyGroupBy(const LazyGroupBy&) = delete;
  LazyGroupBy(LazyGroupBy&&) = delete;
  LazyGroupBy& operator=(const LazyGroupBy&) = delete;
  LazyGroupBy& operator=(LazyGroupBy&&) = delete;

  // Commit the current group to the result table. This will write the final id
  // into the `resultTable` and reset the aggregation data for the next group.
  void commitRow(IdTable& resultTable,
                 sparqlExpression::EvaluationContext& evaluationContext,
                 const GroupBy::GroupBlock& currentGroupBlock);

  // Process the next potentially partial group as a block of rows. This
  // modifies the state of `aggregateData_`. Call `commitRow` to write the final
  // result.
  void processBlock(sparqlExpression::EvaluationContext& evaluationContext,
                    size_t beginIndex, size_t endIndex);

 private:
  // Reset the stored aggregation data. This is cheaper than recreating the
  // objects every time.
  void resetAggregationData();

  // Helper function to visit the correct variant of the aggregation data.
  void visitAggregate(const auto& visitor,
                      const GroupBy::HashMapAggregateInformation& aggregateInfo,
                      auto&&... additionalVariants);

  auto allAggregateInfoView() const {
    return aggregateAliases_ |
           std::views::transform(
               &GroupBy::HashMapAliasInformation::aggregateInfo_) |
           std::views::join;
  }

  FRIEND_TEST(LazyGroupBy, verifyGroupConcatIsCorrectlyInitialized);
};
