//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/OrderBy.h"
#include "engine/ValuesForTesting.h"
#include "global/ValueIdComparators.h"
#include "index/IdTableUtils.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

using namespace std::string_literals;
using namespace std::chrono_literals;
using ad_utility::source_location;

namespace {
// Create an `OrderBy` operation that sorts the `input` by the `sortColumns`.
OrderBy makeOrderBy(IdTable input, const OrderBy::SortIndices& sortColumns) {
  std::vector<std::optional<Variable>> vars;
  auto qec = ad_utility::testing::getQec();
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back("?"s + std::to_string(i));
  }
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      ad_utility::testing::getQec(), std::move(input), vars);
  return OrderBy{qec, std::move(subtree), sortColumns};
}

// Test that the `input`, when being sorted by its 0-th column as its primary
// key, its 1st column as its secondary key etc. using an `ORDER BY` operation,
// yields the `expected` result. The `isDescending` vector specifies, which of
// the columns of `expected` are sorted in descending order. The test is
// performed for all possible permutations of the sort columns by also permuting
// `input` and `expected` accordingly.
void testOrderBy(IdTable input, const IdTable& expected,
                 std::vector<bool> isDescending,
                 source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto qec = ad_utility::testing::getQec();

  AD_CONTRACT_CHECK(input.numColumns() == isDescending.size());
  AD_CONTRACT_CHECK(input.numColumns() == expected.numColumns());
  AD_CONTRACT_CHECK(input.numRows() == expected.numRows());
  // Set up a vector of `SortIndices`. Those will later be permuted.
  // The second element (`isDescending`) will be correctly set later.
  OrderBy::SortIndices sortColumns;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    sortColumns.emplace_back(i, false);
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
      ql::ranges::copy(input.getColumn(i),
                       permutedInput.getColumn(sortColumns[i].first).begin());
      ql::ranges::copy(
          expected.getColumn(i),
          permutedExpected.getColumn(sortColumns[i].first).begin());
      // Also put the information which columns are descending into the correct
      // column.
      sortColumns[i].second = isDescending[i];
    }

    // Randomly shuffle the input and sort.
    for (size_t i = 0; i < 5; ++i) {
      randomShuffle(permutedInput.begin(), permutedInput.end());
      OrderBy s = makeOrderBy(permutedInput.clone(), sortColumns);
      auto result = s.getResult();
      const auto& resultTable = result->idTableView();
      ASSERT_EQ(resultTable, permutedExpected);
    }
  } while (std::next_permutation(sortColumns.begin(), sortColumns.end()));
}
}  // namespace

// _____________________________________________________________________________
TEST(OrderBy, computeOrderBySingleIntColumn) {
  VectorTable input{{0},   {1},       {-1},  {3},
                    {-17}, {1230957}, {123}, {-1249867132}};
  VectorTable expectedAscending{{-1249867132}, {-17}, {-1},  {0},
                                {1},           {3},   {123}, {1230957}};
  VectorTable expectedDescending{{1230957}, {123}, {3},   {1},
                                 {0},       {-1},  {-17}, {-1249867132}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedAscendingTable =
      makeIdTableFromVector(expectedAscending, &Id::makeFromInt);
  auto expectedDescendingTable =
      makeIdTableFromVector(expectedDescending, &Id::makeFromInt);
  testOrderBy(inputTable.clone(), expectedAscendingTable, {false});
  testOrderBy(inputTable.clone(), expectedDescendingTable, {true});
}

// _____________________________________________________________________________
TEST(OrderBy, computeOrderByFloatWithNan) {
  auto nan = Id::makeFromDouble(std::numeric_limits<double>::quiet_NaN());
  VectorTable input{{0},   {1},       {nan}, {-1},  {3},
                    {-17}, {1230957}, {123}, {nan}, {-1249867132}};
  VectorTable expectedAscending{{-1249867132}, {-17}, {-1},      {0},   {1},
                                {3},           {123}, {1230957}, {nan}, {nan}};
  VectorTable expectedDescending{{nan}, {nan}, {1230957}, {123}, {3},
                                 {1},   {0},   {-1},      {-17}, {-1249867132}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromDouble);
  auto expectedAscendingTable =
      makeIdTableFromVector(expectedAscending, &Id::makeFromDouble);
  auto expectedDescendingTable =
      makeIdTableFromVector(expectedDescending, &Id::makeFromDouble);
  testOrderBy(inputTable.clone(), expectedAscendingTable, {false});
  testOrderBy(inputTable.clone(), expectedDescendingTable, {true});
}

// _____________________________________________________________________________
TEST(OrderBy, twoColumnsIntAndFloat) {
  auto qec = ad_utility::testing::getQec();
  using Vec = std::vector<std::pair<int64_t, double>>;
  Vec intsAndFloats{{-3, 1.0}, {0, 7.0}, {-3, 0.5}, {0, -2.8}};
  Vec intsAndFloatsExpectedAllAscending{
      {-3, 0.5}, {-3, 1.0}, {0, -2.8}, {0, 7.0}};
  Vec intsAndFloatsExpectedFirstDescending{
      {0, -2.8}, {0, 7.0}, {-3, 0.5}, {-3, 1.0}};
  Vec intsAndFloatsExpectedSecondDescending{
      {-3, 1.0}, {-3, 0.5}, {0, 7.0}, {0, -2.8}};
  Vec intsAndFloatsExpectedBothDescending{
      {0, 7.0}, {0, -2.8}, {-3, 1.0}, {-3, 0.5}};

  auto makeTable = [&qec](const Vec& vec) {
    IdTable table{2, qec->getAllocator()};
    table.resize(vec.size());
    for (size_t i = 0; i < vec.size(); ++i) {
      table(i, 0) = Id::makeFromInt(vec[i].first);
      table(i, 1) = Id::makeFromDouble(vec[i].second);
    }
    return table;
  };

  auto input = makeTable(intsAndFloats);
  auto expectedAllAscending = makeTable(intsAndFloatsExpectedAllAscending);
  auto firstDescending = makeTable(intsAndFloatsExpectedFirstDescending);
  auto secondDescending = makeTable(intsAndFloatsExpectedSecondDescending);
  auto bothDescending = makeTable(intsAndFloatsExpectedBothDescending);

  testOrderBy(input.clone(), expectedAllAscending, {false, false});
  testOrderBy(input.clone(), firstDescending, {true, false});
  testOrderBy(input.clone(), secondDescending, {false, true});
  testOrderBy(input.clone(), bothDescending, {true, true});
}

// _____________________________________________________________________________
TEST(OrderBy, computeOrderByThreeColumns) {
  VectorTable input{
      {-1, 12, -3}, {1, 7, 11}, {-1, 12, -4}, {1, 6, 0}, {1, 7, 11}};
  VectorTable expectedAllAscending{
      {-1, 12, -4}, {-1, 12, -3}, {1, 6, 0}, {1, 7, 11}, {1, 7, 11}};
  VectorTable expectedFirstAndThirdDescending{
      {1, 6, 0}, {1, 7, 11}, {1, 7, 11}, {-1, 12, -3}, {-1, 12, -4}};
  VectorTable expectedAllDescending{
      {1, 7, 11}, {1, 7, 11}, {1, 6, 0}, {-1, 12, -3}, {-1, 12, -4}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedTableAllAscending =
      makeIdTableFromVector(expectedAllAscending, &Id::makeFromInt);
  auto expectedTableFirstAndThirdDescending =
      makeIdTableFromVector(expectedFirstAndThirdDescending, &Id::makeFromInt);
  auto expectedTableAllDescending =
      makeIdTableFromVector(expectedAllDescending, &Id::makeFromInt);
  testOrderBy(inputTable.clone(), expectedTableAllAscending,
              {false, false, false});
  testOrderBy(inputTable.clone(), expectedTableFirstAndThirdDescending,
              {true, false, true});
  testOrderBy(inputTable.clone(), expectedTableAllDescending,
              {true, true, true});
}

TEST(OrderBy, mixedDatatypes) {
  auto I = ad_utility::testing::IntId;
  auto V = ad_utility::testing::VocabId;
  auto D = ad_utility::testing::DoubleId;
  auto U = Id::makeUndefined();

  VectorTable input{{I(13)}, {I(-7)}, {U}, {I(0)}, {D(12.3)}, {U},
                    {V(12)}, {V(0)},  {U}, {U},    {D(-2e-4)}};
  VectorTable expected{{U},    {U},       {U},     {U},    {I(-7)}, {D(-2e-4)},
                       {I(0)}, {D(12.3)}, {I(13)}, {V(0)}, {V(12)}};
  testOrderBy(makeIdTableFromVector(input), makeIdTableFromVector(expected),
              {false});

  ql::ranges::reverse(expected);
  testOrderBy(makeIdTableFromVector(input), makeIdTableFromVector(expected),
              {true});
}

namespace {
// The result of an `OrderBy` and whether it took the fast path for sorted
// numeric input.
using ResultAndFastPath = std::pair<IdTable, bool>;

// Sort the `input` by the `sortedColumns` in the internal order, and compute
// an `OrderBy` with the `sortIndices` on an input that reports being sorted by
// these columns.
ResultAndFastPath orderByOnSortedInput(
    const VectorTable& input, bool isDescending = false,
    std::vector<ColumnIndex> sortedColumns = {0},
    OrderBy::SortIndices sortIndices = {{0, false}}) {
  auto qec = ad_utility::testing::getQec();
  IdTable table = makeIdTableFromVector(input);
  IdTableUtils::sort(table, sortedColumns);
  std::vector<std::optional<Variable>> vars;
  for (size_t i = 0; i < table.numColumns(); ++i) {
    vars.emplace_back("?"s + std::to_string(i));
  }
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(table), vars, false, std::move(sortedColumns));
  sortIndices.front().second = isDescending;
  OrderBy orderBy{qec, std::move(subtree), std::move(sortIndices)};
  return {orderBy.getResult()->idTableView().clone(),
          orderBy.runtimeInfo().details_.contains("sorted-numeric-input")};
}
}  // namespace

// Test that `ORDER BY` on an integer column that is sorted in the internal
// order puts the undefined values first and the negative integers before the
// non-negative ones, without sorting.
TEST(OrderBy, sortedIntInput) {
  auto I = ad_utility::testing::IntId;
  auto U = Id::makeUndefined();
  auto min = Id::IntegerType::min();
  auto max = Id::IntegerType::max();

  // The second column shows that whole rows are moved.
  EXPECT_EQ(orderByOnSortedInput({{I(0), I(1)},
                                  {I(max), I(2)},
                                  {I(-1), I(3)},
                                  {U, I(4)},
                                  {I(min), I(5)}}),
            (ResultAndFastPath{makeIdTableFromVector({{U, I(4)},
                                                      {I(min), I(5)},
                                                      {I(-1), I(3)},
                                                      {I(0), I(1)},
                                                      {I(max), I(2)}}),
                               true}));

  // Without undefined values.
  EXPECT_EQ(
      orderByOnSortedInput({{I(3)}, {I(-2)}}),
      (ResultAndFastPath{makeIdTableFromVector({{I(-2)}, {I(3)}}), true}));
}

// Test that `ORDER BY` on a double column that is sorted in the internal order
// puts the negative doubles in reverse order before the non-negative ones and
// all `NaN`s last, in both directions and without sorting.
TEST(OrderBy, sortedDoubleInput) {
  auto D = ad_utility::testing::DoubleId;
  auto U = Id::makeUndefined();
  auto inf = std::numeric_limits<double>::infinity();
  auto nan = std::numeric_limits<double>::quiet_NaN();
  // A `NaN` with the sign bit set (e.g. the result of `0.0 / 0.0` on x86) is
  // at the very end of the internal order, after the negative doubles.
  auto negNan = std::copysign(nan, -1.0);
  VectorTable input{{D(0.0)},  {D(2.25)},   {D(-inf)}, {D(nan)},
                    {D(-3.5)}, {D(negNan)}, {U},       {D(inf)}};

  // Ascending.
  EXPECT_EQ(orderByOnSortedInput(input),
            (ResultAndFastPath{makeIdTableFromVector({{U},
                                                      {D(-inf)},
                                                      {D(-3.5)},
                                                      {D(0.0)},
                                                      {D(2.25)},
                                                      {D(inf)},
                                                      {D(nan)},
                                                      {D(negNan)}}),
                               true}));

  // Descending.
  EXPECT_EQ(orderByOnSortedInput(input, true),
            (ResultAndFastPath{makeIdTableFromVector({{D(negNan)},
                                                      {D(nan)},
                                                      {D(inf)},
                                                      {D(2.25)},
                                                      {D(0.0)},
                                                      {D(-3.5)},
                                                      {D(-inf)},
                                                      {U}}),
                               true}));
}

// Test that `ORDER BY` on a sorted input uses the regular sort if the sort
// column is not all integers or all doubles, if the input is sorted by another
// column, or if there is more than one sort column.
TEST(OrderBy, sortedInputWithoutFastPath) {
  auto I = ad_utility::testing::IntId;
  auto D = ad_utility::testing::DoubleId;
  auto V = ad_utility::testing::VocabId;
  auto U = Id::makeUndefined();

  // Integers and doubles.
  EXPECT_EQ(orderByOnSortedInput({{I(3)}, {D(-2.5)}, {I(-4)}}),
            (ResultAndFastPath{
                makeIdTableFromVector({{I(-4)}, {D(-2.5)}, {I(3)}}), false}));

  // Only vocabulary entries.
  EXPECT_EQ(
      orderByOnSortedInput({{V(4)}, {U}, {V(2)}}),
      (ResultAndFastPath{makeIdTableFromVector({{U}, {V(2)}, {V(4)}}), false}));

  // Only undefined values.
  EXPECT_EQ(orderByOnSortedInput({{U}, {U}}),
            (ResultAndFastPath{makeIdTableFromVector({{U}, {U}}), false}));

  // Sorted by the second column.
  VectorTable input{{I(3), I(1)}, {I(-2), I(0)}};
  auto expected = makeIdTableFromVector({{I(-2), I(0)}, {I(3), I(1)}});
  EXPECT_EQ(orderByOnSortedInput(input, false, {1}),
            (ResultAndFastPath{expected.clone(), false}));

  // Two sort columns.
  EXPECT_EQ(orderByOnSortedInput(input, false, {0}, {{0, false}, {1, false}}),
            (ResultAndFastPath{expected.clone(), false}));
}

// Test that the cost estimate of `ORDER BY` is linear for an input that is
// sorted by the sort column, and `n log n` otherwise.
TEST(OrderBy, costEstimateForSortedInput) {
  // Eight rows, so `log n` is 3. The cost of the input is its number of rows.
  VectorTable input;
  for (int64_t i = 0; i < 8; ++i) {
    input.push_back({i});
  }
  auto qec = ad_utility::testing::getQec();
  auto makeOrderByOnInput = [&](std::vector<ColumnIndex> sortedColumns) {
    IdTable table = makeIdTableFromVector(input);
    auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(table),
        std::vector<std::optional<Variable>>{Variable{"?x"}}, false,
        std::move(sortedColumns));
    return OrderBy{qec, std::move(subtree), {{0, false}}};
  };
  EXPECT_EQ(makeOrderByOnInput({0}).getCostEstimate(), 8 + 8);
  EXPECT_EQ(makeOrderByOnInput({}).getCostEstimate(), 8 + 8 * 3);

  // An empty input, where `log n` is not defined.
  EXPECT_EQ(makeOrderBy(IdTable{1, qec->getAllocator()}, {{0, false}})
                .getCostEstimate(),
            0);
}

// _____________________________________________________________________________
TEST(OrderBy, simpleMemberFunctions) {
  {
    VectorTable input{{0},   {1},       {-1},  {3},
                      {-17}, {1230957}, {123}, {-1249867132}};
    auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
    OrderBy s = makeOrderBy(std::move(inputTable), {{0, false}});
    EXPECT_EQ(1u, s.getResultWidth());
    EXPECT_EQ(8u, s.getSizeEstimate());
    EXPECT_EQ("OrderBy on ASC(?0)", s.getDescriptor());

    EXPECT_THAT(s.getCacheKey(),
                ::testing::StartsWith("ORDER BY on columns:asc(0) \n"));

    const auto& varColMap = s.getExternallyVisibleVariableColumns();
    ASSERT_EQ(1u, varColMap.size());
    ASSERT_EQ(0u, varColMap.at(Variable{"?0"}).columnIndex_);
    EXPECT_EQ(42.0, s.getMultiplicity(0));
  }
  {
    VectorTable input{{0, 1}, {0, 2}};
    auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
    OrderBy s = makeOrderBy(std::move(inputTable), {{1, false}, {0, true}});
    EXPECT_EQ(2u, s.getResultWidth());
    EXPECT_EQ(2u, s.getSizeEstimate());
    EXPECT_FALSE(s.knownEmptyResult());
    EXPECT_EQ("OrderBy on ASC(?1) DESC(?0)", s.getDescriptor());

    EXPECT_THAT(s.getCacheKey(),
                ::testing::StartsWith("ORDER BY on columns:asc(1) desc(0) \n"));
    auto varColMap = s.getExternallyVisibleVariableColumns();
    ASSERT_EQ(2u, varColMap.size());
    ASSERT_EQ(0u, varColMap.at(Variable{"?0"}).columnIndex_);
    ASSERT_EQ(1u, varColMap.at(Variable{"?1"}).columnIndex_);
    EXPECT_FALSE(s.knownEmptyResult());
    EXPECT_EQ(42.0, s.getMultiplicity(0));
    EXPECT_EQ(84.0, s.getMultiplicity(1));
  }
}

TEST(OrderBy, verifyOperationIsPreemptivelyAbortedWithNoRemainingTime) {
  VectorTable input;
  // Make sure the estimator estimates a couple of ms to sort this
  for (int64_t i = 0; i < 1000; i++) {
    input.push_back({0, i});
  }
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  OrderBy orderBy = makeOrderBy(std::move(inputTable), {{1, false}, {0, true}});
  // Safe to do, because we know the underlying estimator is mutable
  const_cast<SortPerformanceEstimator&>(
      orderBy.getExecutionContext()->getSortPerformanceEstimator())
      .computeEstimatesExpensively(
          ad_utility::makeUnlimitedAllocator<ValueId>(), 1'000'000);

  orderBy.recursivelySetTimeConstraint(0ms);

  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      orderBy.getResult(true), ::testing::HasSubstr("time estimate exceeded"),
      ad_utility::CancellationException);
}

// _____________________________________________________________________________
TEST(OrderBy, clone) {
  auto* qec = ad_utility::testing::getQec();
  IdTable permutedInput{2, qec->getAllocator()};

  OrderBy orderBy =
      makeOrderBy(permutedInput.clone(), OrderBy::SortIndices{{0, true}});

  auto clone = orderBy.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(orderBy, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), orderBy.getDescriptor());
}
