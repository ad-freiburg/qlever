// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Benke Hargitai <hargitab@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <string>
#include <vector>

#include "../../src/engine/GroupByImpl.h"
#include "../../src/engine/QueryExecutionContext.h"
#include "../../src/engine/QueryExecutionTree.h"
#include "../../src/engine/Result.h"
#include "../../src/engine/Values.h"
#include "../../src/engine/idTable/IdTable.h"
#include "../../src/global/RuntimeParameters.h"
#include "../../src/parser/GraphPatternOperation.h"
#include "../util/AllocatorTestHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"

namespace {
auto I = ad_utility::testing::IntId;
using ad_utility::AllocatorWithLimit;

// A mock operation that returns a pre-computed result.
class MockOperation : public Operation {
 private:
  IdTable table_;

 public:
  MockOperation(QueryExecutionContext* qec, const IdTable& table)
      : Operation(qec), table_(table.clone()) {}

  // These are the pure virtual functions from `Operation` that need to be
  // implemented.

  std::string getCacheKeyImpl() const override { return "MockOperation"; }
  std::string getDescriptor() const override { return "MockOperation"; }
  size_t getResultWidth() const override { return table_.numColumns(); }
  std::vector<ColumnIndex> resultSortedOn() const override { return {}; }
  bool knownEmptyResult() override { return table_.empty(); }
  float getMultiplicity(size_t) override { return 1; }
  uint64_t getSizeEstimateBeforeLimit() override { return table_.size(); }
  uint64_t getCostEstimate() override { return table_.size(); }
  std::vector<QueryExecutionTree*> getChildren() override { return {}; }

  VariableToColumnMap computeVariableToColumnMap() const override {
    VariableToColumnMap map;
    // Single variable ?a mapped to column 0, always defined
    map[Variable{"?a"}] = ColumnIndexAndTypeInfo{0,
        ColumnIndexAndTypeInfo::UndefStatus::AlwaysDefined};
    return map;
  }

  std::unique_ptr<Operation> cloneImpl() const override {
    // We create a new instance with the same properties rather than trying to copy,
    // because `Operation` is not copyable.
    return std::make_unique<MockOperation>(getExecutionContext(), table_);
  }

 private:
  // Compute the result synchronously for testing.
  Result computeResult(bool /*requestLaziness*/) override {
    LocalVocab localVocab{};
    return Result(table_.clone(), std::vector<ColumnIndex>{}, std::move(localVocab));
  }
};
}  // namespace

// A test fixture that sets up a `GroupByImpl` operation for testing.
class GroupBySamplingTest : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = ad_utility::testing::getQec();

  // Helper to create a `GroupByImpl` operation with a simple subtree that
  // returns the given `table`.
  static std::unique_ptr<GroupByImpl> setupGroupBy(
      const IdTable& table, QueryExecutionContext* qec) {
    Variable varA{"?a"};
    auto mockOperation = std::make_shared<MockOperation>(qec, table);
    auto subtree = std::make_shared<QueryExecutionTree>(qec, mockOperation);

    // Create the GroupByImpl operation.
    return std::make_unique<GroupByImpl>(qec, std::vector{varA},
                                         std::vector<Alias>{}, subtree);
  }

  // Helper to create a simple IdTable with one column.
  static IdTable createIdTable(size_t numRows,
                               const std::function<int64_t(size_t)>& generator,
                               AllocatorWithLimit<Id>& allocator) {
    IdTable table(1, allocator);
    table.resize(numRows);
    for (size_t i = 0; i < numRows; ++i) {
      table(i, 0) = I(generator(i));
    }
    return table;
  }
};

// Test that an empty input table correctly results in `false`.
TEST_F(GroupBySamplingTest, edgeCaseEmptyInput) {
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table(1, allocator);
  table.resize(0);
  auto groupBy = setupGroupBy(table, qec_);
  EXPECT_FALSE(groupBy->shouldSkipHashMapGrouping(table));
}

// Test the case where all rows belong to the same group. The estimated number
// of groups should be 1, which is below any reasonable threshold.
TEST_F(GroupBySamplingTest, edgeCaseAllSame) {
  RuntimeParameters().set<"group-by-sample-group-threshold">(100);
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table = createIdTable(1000, [](size_t) { return 42; }, allocator);
  auto groupBy = setupGroupBy(table, qec_);
  EXPECT_FALSE(groupBy->shouldSkipHashMapGrouping(table));
}

// Test the case where every row is a unique group. The estimated number of
// groups should be high, triggering the fallback to the sort-based approach.
TEST_F(GroupBySamplingTest, edgeCaseAllUnique) {
  RuntimeParameters().set<"group-by-sample-group-threshold">(999);
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table = createIdTable(1000, [](size_t i) { return i; }, allocator);
  auto groupBy = setupGroupBy(table, qec_);
  EXPECT_TRUE(groupBy->shouldSkipHashMapGrouping(table));
}

// Test a "normal" case where the estimated number of groups is clearly
// below the configured threshold.
TEST_F(GroupBySamplingTest, belowThreshold) {
  RuntimeParameters().set<"group-by-sample-group-threshold">(500);
  // Sample 10% of rows to get accurate distinct count
  RuntimeParameters().set<"group-by-sample-percent">(0.2);
  // 1000 rows, 100 distinct groups.
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table =
      createIdTable(1000, [](size_t i) { return i % 100; }, allocator);
  auto groupBy = setupGroupBy(table, qec_);
  EXPECT_FALSE(groupBy->shouldSkipHashMapGrouping(table));
}

// Test a "normal" case where the estimated number of groups is clearly
// above the configured threshold.
TEST_F(GroupBySamplingTest, aboveThreshold) {
  RuntimeParameters().set<"group-by-sample-group-threshold">(500);
  // 1000 rows, 800 distinct groups.
  AllocatorWithLimit<Id> allocator{ad_utility::testing::makeAllocator()};
  IdTable table =
      createIdTable(1000, [](size_t i) { return i % 800; }, allocator);
  auto groupBy = setupGroupBy(table, qec_);
  EXPECT_TRUE(groupBy->shouldSkipHashMapGrouping(table));
}
