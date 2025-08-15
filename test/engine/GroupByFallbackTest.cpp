// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "GroupByStrategyHelpers.h"
#include "engine/GroupByImpl.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/idTable/IdTable.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"

namespace {
using ad_utility::AllocatorWithLimit;
using ad_utility::testing::IntId;

class GroupByFallbackTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
};

// Force fallback by setting a tiny distinct-ratio threshold
static void forceFallback() {
  // Enable hash-map grouping but disable skip-guard sampling
  RuntimeParameters().set<"group-by-hash-map-enabled">(true);
  RuntimeParameters().set<"group-by-sample-enabled">(false);
  // Set threshold to zero to trigger fallback within hash-map implementation
  RuntimeParameters().set<"group-by-sample-distinct-ratio">(0.0);
}

// Empty input should yield empty result
TEST_F(GroupByFallbackTest, EmptyInput) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table(1, allocator);
  table.resize(0);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  EXPECT_EQ(out.size(), 0);
}

// All same rows: distinct groups = 1 => fallback yields one row (value)
TEST_F(GroupByFallbackTest, AllSameRows) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table = createIdTable(
      100, [](size_t) { return 7; }, allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out(0, 0), IntId(7));
}

// Mixed values: fallback should sort+uniq
TEST_F(GroupByFallbackTest, MixedValues) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  std::vector<int64_t> vals = {2, 1, 3, 2, 0};
  IdTable table = createIdTable(
      vals.size(), [&](size_t i) { return vals[i]; }, allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  std::vector<Id> expected = {IntId(0), IntId(1), IntId(2), IntId(3)};
  ASSERT_EQ(out.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(out(i, 0), expected[i]);
  }
}

// All unique: fallback yields sorted values
TEST_F(GroupByFallbackTest, AllUniqueRows) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table = createIdTable(
      10, [](size_t i) { return static_cast<int64_t>(i); }, allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 10);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(out(i, 0), IntId(i));
  }
}

// Large uniform and unique mix to verify performance branch
TEST_F(GroupByFallbackTest, LargeMixed) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  const size_t N = 10000;
  // 5 distinct groups
  IdTable table = createIdTable(
      N, [](size_t i) { return static_cast<int64_t>(i % 5); }, allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 5);
  // Expect 0..4
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(out(i, 0), IntId(i));
  }
}

// Multi-column GROUP BY: should group on tuple
TEST_F(GroupByFallbackTest, MultiColumn) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // Table with two columns, five rows, with three distinct pairs
  std::vector<std::array<int64_t, 2>> data = {
      {1, 1}, {1, 2}, {1, 1}, {2, 2}, {2, 2}};
  IdTable table = createIdTable(
      2, data.size(),
      [&](size_t r, size_t c) { return data[r][c]; },
      allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 3);  // (1,1), (1,2), (2,2)
  EXPECT_EQ(out(0, 0), IntId(1));
  EXPECT_EQ(out(0, 1), IntId(1));
  EXPECT_EQ(out(1, 0), IntId(1));
  EXPECT_EQ(out(1, 1), IntId(2));
  EXPECT_EQ(out(2, 0), IntId(2));
  EXPECT_EQ(out(2, 1), IntId(2));
}

// Distinct-ratio guard: with high threshold no fallback
TEST_F(GroupByFallbackTest, DistinctRatioGuard) {
  // enable sampling guard
  RuntimeParameters().set<"group-by-sample-enabled">(true);
  RuntimeParameters().set<"group-by-sample-distinct-ratio">(1.0);
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table = createIdTable(
      5, [](size_t i) { return static_cast<int64_t>(i); }, allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  // fallback guard should skip hash-map, but result unchanged
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(out(i, 0), IntId(i));
  }
}

// Partial fallback: splitRowsByExistingGroups behavior
TEST_F(GroupByFallbackTest, PartialFallback) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // create 8 rows: first 2 unique, then 3 new, then repeats
  std::vector<int64_t> pattern = {1, 2, 3, 4, 3, 4, 1, 2};
  IdTable table = createIdTable(
      pattern.size(), [&](size_t i) { return pattern[i]; }, allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  // distinct values are 1,2,3,4
  ASSERT_EQ(out.size(), 4);
  std::vector<Id> expected = {IntId(1), IntId(2), IntId(3), IntId(4)};
  for (size_t i = 0; i < 4; ++i) {
    EXPECT_EQ(out(i, 0), expected[i]);
  }
}

// Test global aggregation with no GROUP BY variables: should produce one empty
// row
TEST_F(GroupByFallbackTest, ImplicitGroupOnly) {
  // Create input table with 5 rows
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // Create input table with 5 rows
  IdTable table = createIdTable(
      5, [](size_t i) { return static_cast<int64_t>(i + 1); }, allocator);
  // Build a GroupByImpl with no grouping variables and no aliases
  auto mockOp = std::make_shared<MockOperation>(qec_, table);
  auto tree = std::make_shared<QueryExecutionTree>(qec_, mockOp);
  auto gb = std::make_unique<GroupByImpl>(qec_, std::vector<Variable>{},
                                          std::vector<Alias>{}, tree);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  // Expect one row and zero columns
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out.numColumns(), 0);
}

// TODO: Tests with aggregate functions (SUM, COUNT, MIN, MAX) once alias
// construction helpers are available
}  // namespace
