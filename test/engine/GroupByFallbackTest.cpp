// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <numeric>

#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "GroupByStrategyHelpers.h"
#include "engine/GroupBy.h"
#include "engine/QueryExecutionContext.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Result.h"
#include "engine/Sort.h"
#include "engine/idTable/IdTable.h"
#include "engine/sparqlExpressions/AggregateExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
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
  RuntimeParameters().set<"group-by-hash-map-group-threshold">(0);
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
  // Single-column data
  RowData rows(100, 7);
  IdTable table = createIdTable(rows, allocator);
  auto gb = setupGroupBy(table, qec_);
  qec_->clearCacheUnpinnedOnly();
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 1);
  EXPECT_EQ(out(0, 0), IntId(7));
}

// Mixed values: fallback should sort+uniq
TEST_F(GroupByFallbackTest, MixedValues) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  RowData vals = {2, 1, 3, 2, 0};
  IdTable table = createIdTable(vals, allocator);
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
  RowData vals(10);
  for (size_t i = 0; i < vals.size(); ++i) vals[i] = i;
  IdTable table = createIdTable(vals, allocator);
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
  RowData vals(N);
  for (size_t i = 0; i < N; ++i) vals[i] = i % 5;
  IdTable table = createIdTable(vals, allocator);
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
  // Two-column row data
  TableData data = {{1, 1}, {1, 2}, {1, 1}, {2, 2}, {2, 2}};
  IdTable table = createIdTable(data, allocator);
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

// Partial fallback: behavior with existing groups
TEST_F(GroupByFallbackTest, OnlyHashMapPartialFallback) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // Partial fallback: two chunks of single-column data
  RowData ch1(500);
  std::iota(ch1.begin(), ch1.end(), 1);  // 1..500
  RowData ch2(500);
  std::iota(ch2.begin(), ch2.end(), 1);  // 1..500
  auto tables = createLazyIdTables(std::vector<RowData>{ch1, ch2}, allocator);
  auto gb = setupLazyGroupBy(std::move(tables), qec_);
  auto res = gb->computeResult(true);
  const auto& out = res.idTable();
  // Expect 500 rows, because all rows are common across both chunks
  ASSERT_EQ(out.size(), 500);
  // Checking output values
  EXPECT_EQ(out(0, 0), IntId(1));
  EXPECT_EQ(out(1, 0), IntId(2));
  EXPECT_EQ(out(2, 0), IntId(3));
  // ...
  EXPECT_EQ(out(248, 0), IntId(249));
  EXPECT_EQ(out(249, 0), IntId(250));
  EXPECT_EQ(out(250, 0), IntId(251));
  // ...
  EXPECT_EQ(out(497, 0), IntId(498));
  EXPECT_EQ(out(498, 0), IntId(499));
  EXPECT_EQ(out(499, 0), IntId(500));
}

// Partial fallback: behavior with new groups
TEST_F(GroupByFallbackTest, NewGroupsPartialFallback) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // Partial fallback: two chunks of single-column data with new groups
  RowData ch1(500);
  std::iota(ch1.begin(), ch1.end(), 1);  // 1..500
  RowData ch2(500);
  std::iota(ch2.begin(), ch2.end(), 501);  // 501..1000
  auto tables = createLazyIdTables(std::vector<RowData>{ch1, ch2}, allocator);
  auto gb = setupLazyGroupBy(std::move(tables), qec_);
  auto res = gb->computeResult(true);
  const auto& out = res.idTable();
  // Expect 1000 rows, because all rows are unique across both chunks
  ASSERT_EQ(out.size(), 1000);
  // Checking output values
  EXPECT_EQ(out(0, 0), IntId(1));
  EXPECT_EQ(out(1, 0), IntId(2));
  EXPECT_EQ(out(2, 0), IntId(3));
  // ...
  EXPECT_EQ(out(498, 0), IntId(499));
  EXPECT_EQ(out(499, 0), IntId(500));
  EXPECT_EQ(out(500, 0), IntId(501));
  // ...
  EXPECT_EQ(out(997, 0), IntId(998));
  EXPECT_EQ(out(998, 0), IntId(999));
  EXPECT_EQ(out(999, 0), IntId(1000));
}

// Partial fallback: behavior with partly overlapping groups
TEST_F(GroupByFallbackTest, MixedPartialFallback) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // Partial fallback: two chunks of single-column data
  RowData ch1(500);
  std::iota(ch1.begin(), ch1.end(), 1);  // 1..500
  RowData ch2(500);
  std::iota(ch2.begin(), ch2.end(), 251);  // 251..750
  auto tables = createLazyIdTables(std::vector<RowData>{ch1, ch2}, allocator);
  auto gb = setupLazyGroupBy(std::move(tables), qec_);
  auto res = gb->computeResult(true);
  const auto& out = res.idTable();
  // Expect 750 rows, because 251..500 are in both chunks and the rest is unique
  ASSERT_EQ(out.size(), 750);
  // Checking output values
  EXPECT_EQ(out(0, 0), IntId(1));
  EXPECT_EQ(out(1, 0), IntId(2));
  EXPECT_EQ(out(2, 0), IntId(3));
  // ...
  EXPECT_EQ(out(373, 0), IntId(374));
  EXPECT_EQ(out(374, 0), IntId(375));
  EXPECT_EQ(out(375, 0), IntId(376));
  // ...
  EXPECT_EQ(out(747, 0), IntId(748));
  EXPECT_EQ(out(748, 0), IntId(749));
  EXPECT_EQ(out(749, 0), IntId(750));
}

// Multi-column partial fallback with overlap across chunks and sorted result.
TEST_F(GroupByFallbackTest, MultiColumnMixedPartialFallback) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // Two chunks of two-column data with partial overlap
  // ch1: (1,1), (1,2), (2,2), (3,3)
  // ch2: (2,2), (3,3), (4,4)
  ChunkedTableData chunks = {TableData{{1, 1}, {1, 2}, {2, 2}, {3, 3}},
                             TableData{{2, 2}, {3, 3}, {4, 4}}};
  auto tables = createLazyIdTables(chunks, allocator);
  auto gb = setupLazyGroupBy(std::move(tables), qec_);
  auto res = gb->computeResult(true);
  const auto& out = res.idTable();
  // Expect unique, sorted pairs: (1,1), (1,2), (2,2), (3,3), (4,4)
  ASSERT_EQ(out.size(), 5);
  EXPECT_EQ(out(0, 0), IntId(1));
  EXPECT_EQ(out(0, 1), IntId(1));
  EXPECT_EQ(out(1, 0), IntId(1));
  EXPECT_EQ(out(1, 1), IntId(2));
  EXPECT_EQ(out(2, 0), IntId(2));
  EXPECT_EQ(out(2, 1), IntId(2));
  EXPECT_EQ(out(3, 0), IntId(3));
  EXPECT_EQ(out(3, 1), IntId(3));
  EXPECT_EQ(out(4, 0), IntId(4));
  EXPECT_EQ(out(4, 1), IntId(4));
}

// Hybrid fallback with a COUNT aggregate: GROUP BY ?a, COUNT(?b)
TEST_F(GroupByFallbackTest, HybridCountAggregatePartialFallback) {
  // Force fallback after first block
  forceFallback();
  // Build two chunks for variables ?a, ?b
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // ch1 rows: a,b = (1,x),(1,y),(2,z)
  TableData ch1{{1, 10}, {1, 11}, {2, 12}};
  // ch2 rows: a,b = (2,u),(3,v)
  TableData ch2{{2, 13}, {3, 14}};
  auto tables = createLazyIdTables(ChunkedTableData{ch1, ch2}, allocator);

  // Build lazy ValuesForTesting subtree with variables [?a, ?b]
  std::vector<std::optional<Variable>> vars = {Variable{"?a"}, Variable{"?b"}};
  auto valuesOp = std::make_shared<ValuesForTesting>(
      qec_, std::move(tables), vars, false, std::vector<ColumnIndex>{0});
  auto subtree = std::make_shared<QueryExecutionTree>(qec_, valuesOp);
  // Ensure explicit sort on group column to enable hash-map optimization
  subtree = std::make_shared<QueryExecutionTree>(
      qec_, std::make_shared<Sort>(qec_, subtree, std::vector<ColumnIndex>{0}));

  // Construct COUNT(?b) AS ?x
  using namespace sparqlExpression;
  auto expr = std::make_unique<CountExpression>(
      false, std::make_unique<VariableExpression>(Variable{"?b"}));
  Alias alias{SparqlExpressionPimpl{std::move(expr), "COUNT(?b)"},
              Variable{"?x"}};

  GroupBy groupBy{qec_, {Variable{"?a"}}, {std::move(alias)}, subtree};
  auto result = groupBy.getResult();
  const auto& out = result->idTable();

  // Expected counts per group a: 1 -> 2, 2 -> 2, 3 -> 1
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), 3u);
  // Sorted by ?a
  EXPECT_EQ(out(0, 0), IntId(1));
  EXPECT_EQ(out(0, 1).getInt(), 2);
  EXPECT_EQ(out(1, 0), IntId(2));
  EXPECT_EQ(out(1, 1).getInt(), 2);
  EXPECT_EQ(out(2, 0), IntId(3));
  EXPECT_EQ(out(2, 1).getInt(), 1);
}

// Verify that with a large threshold (no fallback), the pure hash-map path is
// used and results are correct.
TEST_F(GroupByFallbackTest, NoFallbackWithLargeThreshold_Count) {
  // Enable optimization, set huge threshold
  RuntimeParameters().set<"group-by-hash-map-enabled">(true);
  RuntimeParameters().set<"group-by-hash-map-group-threshold">(1'000'000);

  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  TableData ch1{{1, 10}, {1, 11}, {2, 12}};
  TableData ch2{{2, 13}, {3, 14}};
  auto tables = createLazyIdTables(ChunkedTableData{ch1, ch2}, allocator);

  std::vector<std::optional<Variable>> vars = {Variable{"?a"}, Variable{"?b"}};
  auto valuesOp = std::make_shared<ValuesForTesting>(
      qec_, std::move(tables), vars, false, std::vector<ColumnIndex>{0});
  auto subtree = std::make_shared<QueryExecutionTree>(qec_, valuesOp);
  subtree = std::make_shared<QueryExecutionTree>(
      qec_, std::make_shared<Sort>(qec_, subtree, std::vector<ColumnIndex>{0}));

  using namespace sparqlExpression;
  auto expr = std::make_unique<CountExpression>(
      false, std::make_unique<VariableExpression>(Variable{"?b"}));
  Alias alias{SparqlExpressionPimpl{std::move(expr), "COUNT(?b)"},
              Variable{"?x"}};

  GroupBy groupBy{qec_, {Variable{"?a"}}, {std::move(alias)}, subtree};
  auto result = groupBy.getResult();
  const auto& out = result->idTable();

  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), 3u);
  EXPECT_EQ(out(0, 0), IntId(1));
  EXPECT_EQ(out(0, 1).getInt(), 2);
  EXPECT_EQ(out(1, 0), IntId(2));
  EXPECT_EQ(out(1, 1).getInt(), 2);
  EXPECT_EQ(out(2, 0), IntId(3));
  EXPECT_EQ(out(2, 1).getInt(), 1);
}

// Test global aggregation with no GROUP BY variables: should produce one empty
// row
TEST_F(GroupByFallbackTest, ImplicitGroupOnly) {
  // Create input table with 5 rows
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  // Create input table with 5 rows
  // Single-column data for implicit group-only test: values 1..5
  RowData rows(5);
  for (size_t i = 0; i < rows.size(); ++i) rows[i] = i + 1;
  IdTable table = createIdTable(rows, allocator);
  // Build a GroupByImpl with no aliases using ValuesForTesting.
  // ValuesForTesting requires one optional<Variable> per input column.
  std::vector<std::optional<Variable>> varOpts = {Variable{"?x"}};
  auto valuesOp =
      std::make_shared<ValuesForTesting>(qec_, std::move(table), varOpts);
  auto tree = std::make_shared<QueryExecutionTree>(qec_, valuesOp);
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
