// Copyright 2023 - 2025 The QLever Authors, in particular:
//
// 2023 - 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025        Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "./util/RuntimeParametersTestHelpers.h"
#include "engine/Sort.h"
#include "engine/ValuesForTesting.h"
#include "global/RuntimeParameters.h"
#include "global/ValueIdComparators.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using namespace std::string_literals;
using namespace std::chrono_literals;
using ad_utility::source_location;

namespace {

// Create a `Sort` operation that sorts the `input` by the `sortColumns`.
Sort makeSort(IdTable input, const std::vector<ColumnIndex>& sortColumns) {
  std::vector<std::optional<Variable>> vars;
  auto qec = ad_utility::testing::getQec();
  for (ColumnIndex i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back("?"s + std::to_string(i));
  }
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      ad_utility::testing::getQec(), std::move(input), vars);
  return Sort{qec, subtree, sortColumns};
}

// Test that the `input`, when being sorted by its 0-th column as its primary
// key, its 1st column as its secondary key etc. using a `Sort` operation,
// yields the `expected` result. The test is performed for all possible
// permutations of the sort columns by also permuting `input` and `expected`
// accordingly.
void testSort(IdTable input, const IdTable& expected,
              source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto qec = ad_utility::testing::getQec();

  // Set up the vector of sort columns. Those will later be permuted.
  std::vector<ColumnIndex> sortColumns;
  for (ColumnIndex i = 0; i < input.numColumns(); ++i) {
    sortColumns.push_back(i);
  }

  // This loop runs over all possible permutations of the sort columns.
  do {
    IdTable permutedInput{input.numColumns(), qec->getAllocator()};
    IdTable permutedExpected{expected.numColumns(), qec->getAllocator()};
    permutedInput.resize(input.numRows());
    permutedExpected.resize(expected.numRows());
    // Apply the current permutation of the `sortColumns` to `expected` and
    // `input`.
    for (size_t i = 0; i < sortColumns.size(); ++i) {
      ql::ranges::copy(input.getColumn(sortColumns[i]),
                       permutedInput.getColumn(i).begin());
      ql::ranges::copy(expected.getColumn(sortColumns[i]),
                       permutedExpected.getColumn(i).begin());
    }

    for (size_t i = 0; i < 5; ++i) {
      randomShuffle(permutedInput.begin(), permutedInput.end());
      Sort s = makeSort(permutedInput.clone(), sortColumns);
      auto result = s.getResult();
      const auto& resultTable = result->idTable();
      ASSERT_EQ(resultTable, permutedExpected);
    }
  } while (std::next_permutation(sortColumns.begin(), sortColumns.end()));
}
}  // namespace

TEST(Sort, ComputeSortSingleIntColumn) {
  VectorTable input{{0},   {1},       {-1},  {3},
                    {-17}, {1230957}, {123}, {-1249867132}};
  VectorTable expected{{0},       {1},           {3},   {123},
                       {1230957}, {-1249867132}, {-17}, {-1}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  testSort(std::move(inputTable), expectedTable);
}

TEST(Sort, TwoColumnsIntAndFloat) {
  auto qec = ad_utility::testing::getQec();
  IdTable input{2, qec->getAllocator()};
  IdTable expected{2, qec->getAllocator()};

  std::vector<std::pair<int64_t, double>> intsAndFloats{
      {-3, 1.0}, {-3, 0.5}, {0, 7.0}, {0, 2.8}};
  std::vector<std::pair<int64_t, double>> intsAndFloatsExpected{
      {0, 2.8}, {0, 7}, {-3, 0.5}, {-3, 1.0}};

  AD_CONTRACT_CHECK(intsAndFloats.size() == intsAndFloatsExpected.size());
  input.resize(intsAndFloats.size());
  expected.resize(intsAndFloatsExpected.size());

  ASSERT_FALSE(Id::makeFromDouble(1.0) < Id::makeFromDouble(0.5));

  for (size_t i = 0; i < intsAndFloats.size(); ++i) {
    input(i, 0) = Id::makeFromInt(intsAndFloats[i].first);
    input(i, 1) = Id::makeFromDouble(intsAndFloats[i].second);
    expected(i, 0) = Id::makeFromInt(intsAndFloatsExpected[i].first);
    expected(i, 1) = Id::makeFromDouble(intsAndFloatsExpected[i].second);
  }

  testSort(std::move(input), expected);
}

TEST(Sort, ComputeSortThreeColumns) {
  VectorTable input{
      {-1, 12, -3}, {1, 7, 11}, {-1, 12, -4}, {1, 6, 0}, {1, 7, 11}};
  VectorTable expected{
      {1, 6, 0}, {1, 7, 11}, {1, 7, 11}, {-1, 12, -4}, {-1, 12, -3}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  testSort(std::move(inputTable), expectedTable);
}

TEST(Sort, mixedDatatypes) {
  auto I = ad_utility::testing::IntId;
  auto V = ad_utility::testing::VocabId;
  auto D = ad_utility::testing::DoubleId;
  auto U = Id::makeUndefined();

  VectorTable input{{I(13)}, {I(-7)}, {U}, {I(0)}, {D(12.3)}, {U},
                    {V(12)}, {V(0)},  {U}, {U},    {D(-2e-4)}};
  VectorTable expected{{U},     {U},       {U},        {U},    {I(0)}, {I(13)},
                       {I(-7)}, {D(12.3)}, {D(-2e-4)}, {V(0)}, {V(12)}};
  testSort(makeIdTableFromVector(input), makeIdTableFromVector(expected));
}

TEST(Sort, SimpleMemberFunctions) {
  {
    VectorTable input{{0},   {1},       {-1},  {3},
                      {-17}, {1230957}, {123}, {-1249867132}};
    auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
    Sort s = makeSort(std::move(inputTable), {0});
    EXPECT_EQ(1u, s.getResultWidth());
    EXPECT_EQ(8u, s.getSizeEstimate());
    EXPECT_EQ("Sort (internal order) on ?0", s.getDescriptor());

    EXPECT_THAT(s.getCacheKey(),
                ::testing::StartsWith("SORT(internal) on columns:asc(0) \n"));
    auto varColMap = s.getExternallyVisibleVariableColumns();
    ASSERT_EQ(1u, varColMap.size());
    ASSERT_EQ(0u, varColMap.at(Variable{"?0"}).columnIndex_);
    EXPECT_FALSE(s.knownEmptyResult());
    EXPECT_EQ(42.0, s.getMultiplicity(0));
  }

  {
    VectorTable input{{0, 1}, {0, 2}};
    auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
    Sort s = makeSort(std::move(inputTable), {1, 0});
    EXPECT_EQ(2u, s.getResultWidth());
    EXPECT_EQ(2u, s.getSizeEstimate());
    EXPECT_FALSE(s.knownEmptyResult());
    EXPECT_EQ("Sort (internal order) on ?1 ?0", s.getDescriptor());

    EXPECT_THAT(
        s.getCacheKey(),
        ::testing::StartsWith("SORT(internal) on columns:asc(1) asc(0) \n"));
    const auto& varColMap = s.getExternallyVisibleVariableColumns();
    ASSERT_EQ(2u, varColMap.size());
    ASSERT_EQ(0u, varColMap.at(Variable{"?0"}).columnIndex_);
    ASSERT_EQ(1u, varColMap.at(Variable{"?1"}).columnIndex_);
    EXPECT_FALSE(s.knownEmptyResult());
    EXPECT_EQ(42.0, s.getMultiplicity(0));
    EXPECT_EQ(84.0, s.getMultiplicity(1));
  }
}

// _____________________________________________________________________________
TEST(Sort, checkSortedCloneIsProperlyHandled) {
  {
    VectorTable input{{0},   {1},       {-1},  {3},
                      {-17}, {1230957}, {123}, {-1249867132}};
    auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
    Sort sort = makeSort(std::move(inputTable), {0});
    EXPECT_THROW(sort.makeSortedTree({0}), ad_utility::Exception);
    EXPECT_THROW(sort.makeSortedTree({}), ad_utility::Exception);
    // Check that we don't double sort
    auto operation = sort.makeSortedTree({0, 1});
    ASSERT_TRUE(operation.has_value());
    const auto& childReference =
        *operation.value()->getRootOperation()->getChildren().at(0);
    EXPECT_NE(typeid(childReference), typeid(Sort));
  }
  {
    VectorTable input{{0, 0}, {1, 1}};
    auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
    Sort sort = makeSort(std::move(inputTable), {0, 1});
    // Check that we don't double sort
    auto operation = sort.makeSortedTree({1, 0});
    ASSERT_TRUE(operation.has_value());
    const auto& childReference =
        *operation.value()->getRootOperation()->getChildren().at(0);
    EXPECT_NE(typeid(childReference), typeid(Sort));
  }
}

// _____________________________________________________________________________

TEST(Sort, verifyOperationIsPreemptivelyAbortedWithNoRemainingTime) {
  VectorTable input;
  // Make sure the estimator estimates a couple of ms to sort this
  for (int64_t i = 0; i < 1000; i++) {
    input.push_back({0, i});
  }
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  Sort sort = makeSort(std::move(inputTable), {1, 0});
  // Safe to do, because we know the underlying estimator is mutable
  const_cast<SortPerformanceEstimator&>(
      sort.getExecutionContext()->getSortPerformanceEstimator())
      .computeEstimatesExpensively(
          ad_utility::makeUnlimitedAllocator<ValueId>(), 1'000'000);

  sort.recursivelySetTimeConstraint(0ms);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      sort.getResult(true), ::testing::HasSubstr("time estimate exceeded"),
      ad_utility::CancellationException);
}

// _____________________________________________________________________________
TEST(Sort, clone) {
  Sort sort = makeSort(makeIdTableFromVector({{0, 0}}), {0});

  auto clone = sort.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(sort, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), sort.getDescriptor());
}

// Test external sorting with lazy input (multiple IdTable blocks). The test
// uses 4 blocks where block 3 exceeds the threshold, so block 4 exercises the
// "remaining blocks" loop in `computeResultExternal`.
TEST(Sort, externalSortLazyInput) {
  auto qec = ad_utility::testing::getQec();

  // Create multiple tables to simulate lazy input. Total size needs to exceed
  // the threshold. 4 batches × 2000 rows × 3 cols × 8 bytes = 192 KB.
  std::vector<IdTable> tables;
  std::vector<std::optional<Variable>> vars = {Variable{"?0"}, Variable{"?1"},
                                               Variable{"?2"}};
  for (int64_t batch = 0; batch < 4; ++batch) {
    VectorTable batchInput;
    for (int64_t i = 0; i < 2000; ++i) {
      int64_t val = batch * 2000 + i;
      batchInput.push_back({val % 10, val % 7, val});
    }
    tables.push_back(makeIdTableFromVector(batchInput, &Id::makeFromInt));
  }

  // Create a `ValuesForTesting` that produces lazy output (multiple tables).
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(tables), vars);

  // Set threshold to 100 KB so that the 192 KB input triggers external sort.
  // The threshold is exceeded after block 3 (144 KB > 100 KB), so block 4 is
  // processed by the "remaining blocks" loop.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::sortInMemoryThreshold_>(
          ad_utility::MemorySize::kilobytes(100));

  // Create the `Sort` operation and get the result.
  Sort externalSort{qec, subtree, {0, 1, 2}};
  auto result = externalSort.getResult();

  // Verify the result is sorted correctly.
  const auto& table = result->idTable();
  EXPECT_EQ(8000u, table.numRows());
  for (size_t i = 1; i < table.numRows(); ++i) {
    bool isLessOrEqual =
        std::tie(table(i - 1, 0), table(i - 1, 1), table(i - 1, 2)) <=
        std::tie(table(i, 0), table(i, 1), table(i, 2));
    EXPECT_TRUE(isLessOrEqual) << "Row " << i << " is not in order";
  }
}

// Test external sorting with fully materialized input.
TEST(Sort, externalSortMaterializedInput) {
  auto qec = ad_utility::testing::getQec();

  // Clear cache to avoid hits from previous tests.
  qec->getQueryTreeCache().clearAll();

  // Set in-memory threshold to 100 KB, and create input table large enough to
  // exceed that threshold: 5000 rows × 3 cols × 8 bytes = 120 KB.
  //
  // NOTE: `int64_t` is needed here and in the following tests because
  // `VectorTable` expects `int64_t` values.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::sortInMemoryThreshold_>(
          ad_utility::MemorySize::kilobytes(100));
  VectorTable input;
  for (int64_t i = 0; i < 5000; ++i) {
    input.push_back({i % 13, i % 11, i + 2000});
  }
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);

  // Create a `ValuesForTesting` operation with `forceFullyMaterialized = true`
  // (the last argument) to ensure the subtree returns a fully materialized
  // result even when lazy is requested.
  std::vector<std::optional<Variable>> vars = {Variable{"?0"}, Variable{"?1"},
                                               Variable{"?2"}};
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(inputTable), vars, false, std::vector<ColumnIndex>{},
      LocalVocab{}, std::nullopt, true);

  // Create the `Sort` operation and get the result.
  Sort externalSort{qec, subtree, {0, 1, 2}};
  auto result = externalSort.getResult();

  // Verify the result is sorted correctly.
  const auto& table = result->idTable();
  EXPECT_EQ(5000u, table.numRows());
  for (size_t i = 1; i < table.numRows(); ++i) {
    bool isLessOrEqual =
        std::tie(table(i - 1, 0), table(i - 1, 1), table(i - 1, 2)) <=
        std::tie(table(i, 0), table(i, 1), table(i, 2));
    EXPECT_TRUE(isLessOrEqual) << "Row " << i << " is not in order";
  }
}

// Test external sorting with lazy output.
TEST(Sort, externalSortLazyOutput) {
  auto qec = ad_utility::testing::getQec();

  // Clear cache at start to avoid hits from previous tests.
  qec->getQueryTreeCache().clearAll();

  // Create an input table large enough to exceed the second threshold of 100 KB
  // below: 5000 rows × 3 cols × 8 bytes = 120 KB.
  VectorTable input;
  for (int64_t i = 0; i < 5000; ++i) {
    input.push_back({i % 11, i % 9, i + 1000});
  }
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);

  // First compute the expected result using in-memory sort (large threshold).
  auto cleanup1 =
      setRuntimeParameterForTest<&RuntimeParameters::sortInMemoryThreshold_>(
          ad_utility::MemorySize::megabytes(10));
  Sort inMemorySort = makeSort(inputTable.clone(), {0, 1, 2});
  auto inMemoryResult = inMemorySort.getResult();
  EXPECT_EQ(inMemorySort.runtimeInfo().details_["is-external"], "false");

  // Clear cache again before external sort.
  qec->getQueryTreeCache().clearAll();

  // Set threshold to 100 KB so that the 120 KB input triggers external sort.
  auto cleanup2 =
      setRuntimeParameterForTest<&RuntimeParameters::sortInMemoryThreshold_>(
          ad_utility::MemorySize::kilobytes(100));

  // Create the `Sort` operation and get the result lazily.
  Sort externalSort = makeSort(inputTable.clone(), {0, 1, 2});
  auto lazyResult =
      externalSort.getResult(false, ComputationMode::LAZY_IF_SUPPORTED);
  EXPECT_EQ(externalSort.runtimeInfo().details_["is-external"], "true");

  // Lazy results are not fully materialized.
  EXPECT_FALSE(lazyResult->isFullyMaterialized());

  // Consume the lazy result and collect all rows.
  IdTable externalResultIdTable{3, qec->getAllocator()};
  for (auto& idTableAndLocalVocab : lazyResult->idTables()) {
    externalResultIdTable.insertAtEnd(idTableAndLocalVocab.idTable_);
  }

  // Compare with in-memory result.
  EXPECT_EQ(inMemoryResult->idTable(), externalResultIdTable);
}

// Test in-memory sorting with fully materialized input (exercises the code path
// where the subtree returns a materialized result that fits in memory).
TEST(Sort, inMemorySortMaterializedInput) {
  auto qec = ad_utility::testing::getQec();

  // Clear cache to avoid hits from previous tests.
  qec->getQueryTreeCache().clearAll();

  // Set threshold to 100 KB, and create input table small enough to stay under
  // that threshold: 100 rows × 3 cols × 8 bytes = 2.4 KB.
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::sortInMemoryThreshold_>(
          ad_utility::MemorySize::kilobytes(100));
  VectorTable input;
  for (int64_t i = 0; i < 100; ++i) {
    input.push_back({i % 7, i % 5, i});
  }
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);

  // Create a `ValuesForTesting` operation with `forceFullyMaterialized = true`
  // (the last argument) to ensure the subtree returns a fully materialized
  // result.
  std::vector<std::optional<Variable>> vars = {Variable{"?0"}, Variable{"?1"},
                                               Variable{"?2"}};
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(inputTable), vars, false, std::vector<ColumnIndex>{},
      LocalVocab{}, std::nullopt, true);

  // Create the `Sort` operation and get the result.
  Sort inMemorySort{qec, subtree, {0, 1, 2}};
  auto result = inMemorySort.getResult();

  // Verify the result is sorted correctly.
  const auto& table = result->idTable();
  EXPECT_EQ(100u, table.numRows());
  for (size_t i = 1; i < table.numRows(); ++i) {
    bool isLessOrEqual =
        std::tie(table(i - 1, 0), table(i - 1, 1), table(i - 1, 2)) <=
        std::tie(table(i, 0), table(i, 1), table(i, 2));
    EXPECT_TRUE(isLessOrEqual) << "Row " << i << " is not in order";
  }
}
