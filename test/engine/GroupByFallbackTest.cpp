// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>
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
#include "engine/sparqlExpressions/GroupConcatExpression.h"
#include "engine/sparqlExpressions/SampleExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "global/RuntimeParameters.h"
#include "parser/GraphPatternOperation.h"
#include "util/HashSet.h"

namespace {
using ad_utility::AllocatorWithLimit;
using ad_utility::testing::IntId;

class GroupByFallbackTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();
};

// Scenario variants for lazy GroupBy fallback across multiple chunks.
enum class ChunkOverlapScenario {
  TwoDistinct,
  TwoSubset,  // Chunk 2 contains only entries from chunk 1 (subset/duplicates)
  TwoMixed,   // Chunk 2 has both overlaps and new values
  FiveDistinct,
  FiveSubset,  // Chunks 2..5 only contain entries from chunk 1
  FiveMixed
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
      qec_, std::move(tables), vars, false, std::vector<ColumnIndex>{});
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

class GroupByFallbackTestChunkOverlap
    : public GroupByFallbackTest,
      public ::testing::WithParamInterface<ChunkOverlapScenario> {};

// Overload: Build a two-column lazy subtree from an arbitrary number of chunks.
static std::shared_ptr<QueryExecutionTree> makeSortedLazySubtreeAB(
    QueryExecutionContext* qec, const ChunkedTableData& chunks) {
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  auto tables = createLazyIdTables(chunks, allocator);
  std::vector<std::optional<Variable>> vars = {Variable{"?a"}, Variable{"?b"}};
  auto valuesOp = std::make_shared<ValuesForTesting>(
      qec, std::move(tables), vars, false, std::vector<ColumnIndex>{});
  auto subtree = std::make_shared<QueryExecutionTree>(qec, valuesOp);
  return std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<Sort>(qec, subtree, std::vector<ColumnIndex>{0}));
}

// Overload for arbitrary number of chunks with string values in column ?b.
static std::shared_ptr<QueryExecutionTree> makeSortedLazySubtreeAB_Strings(
    QueryExecutionContext* qec, const std::vector<TableData>& chunksInt,
    LocalVocab* outLocalVocab) {
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  auto makeStringId = [&](size_t x) {
    using ad_utility::triple_component::LiteralOrIri;
    auto lit = LiteralOrIri::literalWithoutQuotes(std::to_string(x));
    return Id::makeFromLocalVocabIndex(
        outLocalVocab->getIndexAndAddIfNotContained(std::move(lit)));
  };
  std::vector<IdTable> tables;
  for (const auto& in : chunksInt) {
    IdTable t{2, allocator};
    t.resize(in.size());
    for (size_t i = 0; i < in.size(); ++i) {
      t(i, 0) = IntId(in[i][0]);
      t(i, 1) = makeStringId(in[i][1]);
    }
    tables.push_back(std::move(t));
  }
  std::vector<std::optional<Variable>> vars = {Variable{"?a"}, Variable{"?b"}};
  auto valuesOp = std::make_shared<ValuesForTesting>(
      qec, std::move(tables), vars, false, std::vector<ColumnIndex>{},
      std::move(*outLocalVocab));
  auto subtree = std::make_shared<QueryExecutionTree>(qec, valuesOp);
  return std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<Sort>(qec, subtree, std::vector<ColumnIndex>{0}));
}

// Generators for parameterized scenarios (single column values)
static std::vector<RowData> generateSingleColumnChunks(ChunkOverlapScenario s) {
  auto range = [](size_t a, size_t b) {
    RowData r(b - a + 1);
    for (size_t i = a; i <= b; ++i) r[i - a] = i;
    return r;
  };
  switch (s) {
    case ChunkOverlapScenario::TwoDistinct:
      return {range(1, 500), range(501, 1000)};
    case ChunkOverlapScenario::TwoSubset:
      return {range(1, 500), range(1, 500)};
    case ChunkOverlapScenario::TwoMixed:
      return {range(1, 500), range(251, 750)};
    case ChunkOverlapScenario::FiveDistinct:
      return {range(1, 200), range(201, 400), range(401, 600), range(601, 800),
              range(801, 1000)};
    case ChunkOverlapScenario::FiveSubset:
      return {range(1, 500), range(1, 100), range(101, 200), range(201, 300),
              range(301, 500)};
    case ChunkOverlapScenario::FiveMixed:
      return {range(1, 300), range(201, 500), range(401, 700), range(601, 900),
              range(801, 1000)};
  }
  AD_FAIL();
}

// Generators for parameterized scenarios for (a,b) where expectations can be
// derived programmatically for COUNT.
static std::vector<TableData> generateABChunks(ChunkOverlapScenario s) {
  const TableData baseRows{{1, 1}, {1, 2}, {2, 1}, {2, 2}, {3, 1}};
  switch (s) {
    case ChunkOverlapScenario::TwoDistinct:
      return {baseRows, TableData{{1, 3}, {2, 3}, {4, 1}}};
    case ChunkOverlapScenario::TwoSubset:
      return {baseRows, TableData{{1, 1}, {2, 2}, {3, 1}, {1, 2}}};
    case ChunkOverlapScenario::TwoMixed:
      return {baseRows, TableData{{1, 2}, {1, 3}, {2, 1}, {3, 0}}};
    case ChunkOverlapScenario::FiveDistinct:
      return {TableData{{1, 1}}, TableData{{1, 2}}, TableData{{2, 1}},
              TableData{{2, 2}}, TableData{{3, 1}}};
    case ChunkOverlapScenario::FiveSubset:
      return {baseRows, TableData{{1, 1}, {2, 1}}, TableData{{1, 2}, {2, 1}},
              TableData{{2, 2}}, TableData{{3, 1}, {3, 1}}};
    case ChunkOverlapScenario::FiveMixed:
      return {baseRows, TableData{{1, 2}, {2, 0}, {0, 1}},
              TableData{{1, 1}, {2, 3}}, TableData{{3, 1}, {1, 4}},
              TableData{{2, 5}}};
  }
  AD_FAIL();
}

// Generic helper to build a per-group aggregation map from chunks of (a,b)
// rows. The `Update` functor is called as `update(acc, b, inserted)` for each
// (a,b). `inserted` is true if the key `a` was first created for this row.
template <typename Acc, typename Update>
static std::map<size_t, Acc> buildPerGroup(const std::vector<TableData>& chunks,
                                           Update update) {
  std::map<size_t, Acc> result;
  for (const auto& ch : chunks) {
    for (const auto& ab : ch) {
      auto [it, inserted] = result.try_emplace(ab[0]);
      update(it->second, ab[1], inserted);
    }
  }
  return result;
}

// Templated helper to construct a GroupBy with a single aggregate alias on ?b.
// The `AggExpr` is one of the aggregate expression types like SumExpression,
// MinExpression, MaxExpression, AvgExpression, SampleExpression, or
// CountExpression.
template <typename AggExpr>
std::pair<std::vector<TableData>, GroupBy> makeChunksAndGroupBy(
    QueryExecutionContext* qec, ChunkOverlapScenario scenario,
    std::string descriptor, std::string aliasName) {
  using namespace sparqlExpression;
  auto chunks = generateABChunks(scenario);
  auto subtree = makeSortedLazySubtreeAB(qec, chunks);
  auto expr = std::make_unique<AggExpr>(
      false, std::make_unique<VariableExpression>(Variable{"?b"}));
  SparqlExpressionPimpl exprPimpl{std::move(expr), std::move(descriptor)};
  Alias alias{std::move(exprPimpl), Variable{std::move(aliasName)}};
  return {
      std::move(chunks),
      GroupBy{qec, {Variable{"?a"}}, {std::move(alias)}, std::move(subtree)}};
}

// Helper to construct a GroupBy with a single alias.
static GroupBy makeGroupByABWithAlias(QueryExecutionContext* qec,
                                      std::shared_ptr<QueryExecutionTree> t,
                                      sparqlExpression::SparqlExpressionPimpl e,
                                      std::string aliasName) {
  Alias alias{std::move(e), Variable{std::move(aliasName)}};
  return GroupBy{qec, {Variable{"?a"}}, {std::move(alias)}, std::move(t)};
}

// Names for parameterized tests
struct ScenarioNamePrinter {
  std::string operator()(
      const ::testing::TestParamInfo<ChunkOverlapScenario>& info) const {
    switch (info.param) {
      case ChunkOverlapScenario::TwoDistinct:
        return "TwoDistinct";
      case ChunkOverlapScenario::TwoSubset:
        return "TwoSubset";
      case ChunkOverlapScenario::TwoMixed:
        return "TwoMixed";
      case ChunkOverlapScenario::FiveDistinct:
        return "FiveDistinct";
      case ChunkOverlapScenario::FiveSubset:
        return "FiveSubset";
      case ChunkOverlapScenario::FiveMixed:
        return "FiveMixed";
    }
    return "Unknown";
  }
};

TEST_P(GroupByFallbackTestChunkOverlap, UniqueValuesAcrossChunks) {
  forceFallback();
  auto chunks = generateSingleColumnChunks(GetParam());
  // Build tables
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  auto tables = createLazyIdTables(chunks, allocator);
  auto gb = setupLazyGroupBy(std::move(tables), qec_);
  auto res = gb->computeResult(true);
  const auto& out = res.idTable();
  // Compute expected unique count from input chunks.
  ad_utility::HashSet<size_t> unique;
  for (const auto& ch : chunks) {
    for (auto v : ch) unique.insert(v);
  }
  ASSERT_EQ(out.size(), unique.size());
  // Also check sorted ascending.
  size_t prev = std::numeric_limits<size_t>::min();
  for (size_t i = 0; i < out.size(); ++i) {
    auto cur = out(i, 0).getInt();
    ASSERT_LE(prev, cur);
    prev = cur;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, UniquePairsAcrossChunks) {
  forceFallback();
  auto chunks = generateABChunks(GetParam());
  // Build tables
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  auto tables = createLazyIdTables(chunks, allocator);
  auto gb = setupLazyGroupBy(std::move(tables), qec_);
  auto res = gb->computeResult(true);
  const auto& out = res.idTable();
  // Compute expected unique pairs and sort.
  std::set<std::pair<size_t, size_t>> uniq;
  for (const auto& ch : chunks) {
    for (const auto& p : ch) uniq.insert({p[0], p[1]});
  }
  ASSERT_EQ(out.size(), uniq.size());
  // Verify sorted lexicographically and values match
  size_t idx = 0;
  for (const auto& p : uniq) {
    EXPECT_EQ(out(idx, 0), IntId(p.first));
    EXPECT_EQ(out(idx, 1), IntId(p.second));
    ++idx;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, CountPerGroupAcrossChunks) {
  forceFallback();
  auto [chunks, gb] = makeChunksAndGroupBy<sparqlExpression::CountExpression>(
      qec_, GetParam(), "COUNT(?b)", "?x");
  auto result = gb.getResult();
  const auto& out = result->idTable();
  auto counts = buildPerGroup<size_t>(
      chunks, [](size_t& acc, size_t, bool) { acc += 1; });
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), counts.size());
  size_t idx = 0;
  for (auto& [a, c] : counts) {
    EXPECT_EQ(out(idx, 0), IntId(a));
    EXPECT_EQ(out(idx, 1).getInt(), c);
    ++idx;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, SumPerGroupAcrossChunks) {
  forceFallback();
  auto [chunks, gb] = makeChunksAndGroupBy<sparqlExpression::SumExpression>(
      qec_, GetParam(), "SUM(?b)", "?s");
  auto result = gb.getResult();
  const auto& out = result->idTable();
  auto sums = buildPerGroup<size_t>(
      chunks, [](size_t& acc, size_t b, bool) { acc += b; });
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), sums.size());
  size_t idx = 0;
  for (auto& [a, s] : sums) {
    EXPECT_EQ(out(idx, 0), IntId(a));
    EXPECT_EQ(out(idx, 1).getInt(), s);
    ++idx;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, MinPerGroupAcrossChunks) {
  forceFallback();
  auto [chunks, gb] = makeChunksAndGroupBy<sparqlExpression::MinExpression>(
      qec_, GetParam(), "MIN(?b)", "?m");
  auto result = gb.getResult();
  const auto& out = result->idTable();
  auto mins =
      buildPerGroup<size_t>(chunks, [](size_t& acc, size_t b, bool inserted) {
        acc = inserted ? b : std::min(acc, b);
      });
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), mins.size());
  size_t idx = 0;
  for (auto& [a, m] : mins) {
    EXPECT_EQ(out(idx, 0), IntId(a));
    EXPECT_EQ(out(idx, 1).getInt(), m);
    ++idx;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, MaxPerGroupAcrossChunks) {
  forceFallback();
  auto [chunks, gb] = makeChunksAndGroupBy<sparqlExpression::MaxExpression>(
      qec_, GetParam(), "MAX(?b)", "?M");
  auto result = gb.getResult();
  const auto& out = result->idTable();
  auto maxs =
      buildPerGroup<size_t>(chunks, [](size_t& acc, size_t b, bool inserted) {
        acc = inserted ? b : std::max(acc, b);
      });
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), maxs.size());
  size_t idx = 0;
  for (auto& [a, m] : maxs) {
    EXPECT_EQ(out(idx, 0), IntId(a));
    EXPECT_EQ(out(idx, 1).getInt(), m);
    ++idx;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, AvgPerGroupAcrossChunks) {
  forceFallback();
  auto [chunks, gb] = makeChunksAndGroupBy<sparqlExpression::AvgExpression>(
      qec_, GetParam(), "AVG(?b)", "?avg");
  auto result = gb.getResult();
  const auto& out = result->idTable();
  auto sumsCounts = buildPerGroup<std::pair<size_t, size_t>>(
      chunks, [](auto& acc, size_t b, bool) {
        acc.first += b;
        acc.second += 1;
      });
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), sumsCounts.size());
  size_t idx = 0;
  for (auto& [a, sc] : sumsCounts) {
    double expected = static_cast<double>(sc.first) / sc.second;
    EXPECT_EQ(out(idx, 0), IntId(a));
    // Allow tiny floating-point differences due to different aggregation orders
    // between hash-map updates and sort-based fallback merges.
    EXPECT_NEAR(out(idx, 1).getDouble(), expected, 1e-12);
    ++idx;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, SamplePerGroupAcrossChunks) {
  forceFallback();
  auto [chunks, gb] = makeChunksAndGroupBy<sparqlExpression::SampleExpression>(
      qec_, GetParam(), "SAMPLE(?b)", "?sample");
  auto result = gb.getResult();
  const auto& out = result->idTable();
  // Build value sets per group for membership check.
  auto values = buildPerGroup<std::multiset<size_t>>(
      chunks, [](auto& setRef, size_t b, bool) { setRef.insert(b); });
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), values.size());
  size_t idx = 0;
  for (auto& [a, mult] : values) {
    EXPECT_EQ(out(idx, 0), IntId(a));
    size_t v = out(idx, 1).getInt();
    // SAMPLE must be one of the seen values for that group.
    EXPECT_NE(mult.find(v), mult.end());
    ++idx;
  }
}

TEST_P(GroupByFallbackTestChunkOverlap, GroupConcatPerGroupAcrossChunks) {
  forceFallback();
  auto chunks = generateABChunks(GetParam());
  LocalVocab lv;
  auto subtree = makeSortedLazySubtreeAB_Strings(qec_, chunks, &lv);
  using namespace sparqlExpression;
  auto expr = std::make_unique<GroupConcatExpression>(
      false, std::make_unique<VariableExpression>(Variable{"?b"}), ";");
  auto gb = makeGroupByABWithAlias(
      qec_, subtree, SparqlExpressionPimpl{std::move(expr), "GROUP_CONCAT(?b)"},
      "?gc");
  auto result = gb.getResult();
  const auto& out = result->idTable();
  auto toString = [&](Id id) {
    auto idx = id.getLocalVocabIndex();
    auto s = result->localVocab().getWord(idx).toStringRepresentation();
    AD_CORRECTNESS_CHECK(s.size() >= 2);
    return s.substr(1, s.size() - 2);
  };
  // Build expected multisets of strings per group (order-independent,
  // multiplicity preserved).
  std::map<size_t, std::map<std::string, size_t>> expected;
  for (const auto& ch : chunks) {
    for (const auto& ab : ch) {
      expected[ab[0]][std::to_string(ab[1])]++;
    }
  }
  ASSERT_EQ(out.numColumns(), 2u);
  ASSERT_EQ(out.size(), expected.size());
  size_t row = 0;
  for (auto& [a, multisetMap] : expected) {
    EXPECT_EQ(out(row, 0), IntId(a));
    auto s = toString(out(row, 1));
    std::map<std::string, size_t> got;
    size_t start = 0;
    while (start <= s.size()) {
      size_t pos = s.find(';', start);
      auto token = s.substr(
          start, pos == std::string::npos ? std::string::npos : pos - start);
      if (!token.empty()) got[token]++;
      if (pos == std::string::npos) break;
      start = pos + 1;
    }
    EXPECT_EQ(got, multisetMap);
    ++row;
  }
}

INSTANTIATE_TEST_SUITE_P(Scenarios, GroupByFallbackTestChunkOverlap,
                         ::testing::Values(ChunkOverlapScenario::TwoDistinct,
                                           ChunkOverlapScenario::TwoSubset,
                                           ChunkOverlapScenario::TwoMixed,
                                           ChunkOverlapScenario::FiveDistinct,
                                           ChunkOverlapScenario::FiveSubset,
                                           ChunkOverlapScenario::FiveMixed),
                         ScenarioNamePrinter());
}  // namespace
