//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "engine/Sort.h"
#include "engine/ValuesForTesting.h"
#include "global/ValueIdComparators.h"
#include "util/IndexTestHelpers.h"

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
              source_location l = source_location::current()) {
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
      std::ranges::copy(input.getColumn(sortColumns[i]),
                        permutedInput.getColumn(i).begin());
      std::ranges::copy(expected.getColumn(sortColumns[i]),
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

    EXPECT_THAT(s.getSubtree()->getRootOperation()->getCacheKey(),
                ::testing::StartsWith("Values for testing with"));
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
