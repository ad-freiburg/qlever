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
using ad_utility::testing::IntId;
using ad_utility::AllocatorWithLimit;

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

// All same rows: distinct groups =1 => fallback yields one row (value)
TEST_F(GroupByFallbackTest, AllSameRows) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table = createIdTable(100,
                                [](size_t){ return 7; },
                                allocator);
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
  // pattern: 2,1,3,2,0
  IdTable table(1, allocator);
  table.resize(5);
  table(0,0) = IntId(2);
  table(1,0) = IntId(1);
  table(2,0) = IntId(3);
  table(3,0) = IntId(2);
  table(4,0) = IntId(0);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  std::vector<Id> expected = {IntId(0), IntId(1), IntId(2), IntId(3)};
  ASSERT_EQ(out.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(out(i,0), expected[i]);
  }
}

// All unique: fallback yields sorted values
TEST_F(GroupByFallbackTest, AllUniqueRows) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table = createIdTable(10,
                                [](size_t i){ return static_cast<int64_t>(i); },
                                allocator);
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 10);
  for (size_t i = 0; i < 10; ++i) {
    EXPECT_EQ(out(i,0), IntId(i));
  }
}

// Large uniform and unique mix to verify performance branch
TEST_F(GroupByFallbackTest, LargeMixed) {
  forceFallback();
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  const size_t N = 10000;
  IdTable table(1, allocator);
  table.resize(N);
  for (size_t i = 0; i < N; ++i) {
    table(i,0) = IntId((i % 5));  // 5 distinct values
  }
  auto gb = setupGroupBy(table, qec_);
  auto res = gb->computeResult(false);
  const auto& out = res.idTable();
  ASSERT_EQ(out.size(), 5);
  // Expect 0..4
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ(out(i,0), IntId(i));
  }
}
// close anonymous namespace
}
