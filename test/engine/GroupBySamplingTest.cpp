// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <string>
#include <vector>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "GroupByStrategyHelpers.h"
#include "engine/GroupByImpl.h"
#include "engine/GroupByStrategyChooser.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/Values.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
#include "util/Log.h"

namespace {
auto I = ad_utility::testing::IntId;
using ad_utility::AllocatorWithLimit;
}  // namespace

class GroupBySamplingTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
};

// Test that an empty input table correctly results in `false`.
TEST_F(GroupBySamplingTest, edgeCaseEmptyInput) {
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table(1, allocator);
  table.resize(0);
  auto groupBy = setupGroupBy(table, qec_);
  EXPECT_FALSE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
      *groupBy, table, LogLevel::DEBUG));
}

// Test the case where all rows belong to the same group across various sizes
TEST_F(GroupBySamplingTest, edgeCaseAllSame) {
  std::vector<size_t> sizes = {1, 10, 300, 1000, 10'000, 100'000, 1'000'000};
  RuntimeParameters().set<"group-by-sample-min-table-size">(0);
  for (auto s : sizes) {
    AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
    IdTable table = createIdTable(
        s, [](size_t) { return 42; }, allocator);
    auto groupBy = setupGroupBy(table, qec_);
    if (s == 1) {
      // For size 1, we expect the hash-map grouping to be skipped
      // because the estimated number of groups is 2 and the threshold is 0.9
      EXPECT_TRUE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
          *groupBy, table, LogLevel::DEBUG));
    } else {
      EXPECT_FALSE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
          *groupBy, table, LogLevel::DEBUG));
    }
  }
}

// Test the case where every row is a unique group across various sizes
TEST_F(GroupBySamplingTest, edgeCaseAllUnique) {
  std::vector<size_t> sizes = {1, 10, 300, 1000, 10'000, 100'000, 1'000'000};
  for (auto s : sizes) {
    RuntimeParameters().set<"group-by-sample-min-table-size">(0);
    AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
    IdTable table = createIdTable(
        s, [](size_t i) { return static_cast<int64_t>(i); }, allocator);
    auto groupBy = setupGroupBy(table, qec_);
    EXPECT_TRUE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
        *groupBy, table, LogLevel::DEBUG));

    RuntimeParameters().set<"group-by-sample-min-table-size">(1000);
    if (s < 1000) {
      EXPECT_FALSE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
          *groupBy, table, LogLevel::DEBUG));
    } else {
      EXPECT_TRUE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
          *groupBy, table, LogLevel::DEBUG));
    }
  }
}

// Test a "normal" case where the estimated number of groups is clearly
// below the configured threshold.
TEST_F(GroupBySamplingTest, belowThreshold) {
  std::vector<size_t> sizes = {500, 1000, 10'000, 100'000, 1'000'000};
  RuntimeParameters().set<"group-by-sample-min-table-size">(0);
  for (auto s : sizes) {
    AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
    // Use 15% of s as the number of distinct groups
    size_t distinctGroups = static_cast<size_t>(s * 0.15);
    IdTable table = createIdTable(
        s, [distinctGroups](size_t i) { return i % distinctGroups; },
        allocator);
    auto groupBy = setupGroupBy(table, qec_);
    EXPECT_FALSE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
        *groupBy, table, LogLevel::DEBUG));
  }
}

// Test a "normal" case where the estimated number of groups is clearly
// above the configured threshold.
TEST_F(GroupBySamplingTest, aboveThreshold) {
  std::vector<size_t> sizes = {1000, 10'000, 100'000, 1'000'000};
  RuntimeParameters().set<"group-by-sample-min-table-size">(0);
  for (auto s : sizes) {
    AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
    // Use 95% of s as the number of distinct groups
    size_t distinctGroups = static_cast<size_t>(s * 0.95);
    IdTable table = createIdTable(
        s, [distinctGroups](size_t i) { return i % distinctGroups; },
        allocator);
    auto groupBy = setupGroupBy(table, qec_);
    EXPECT_TRUE(GroupByStrategyChooser::shouldSkipHashMapGrouping(
        *groupBy, table, LogLevel::DEBUG));
  }
}
