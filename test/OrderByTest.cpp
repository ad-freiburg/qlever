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
// Create an `OrderBy` on column 0 (descending iff `isDescending`) whose input
// is sorted by the columns `sortedColumns` in the internal order and reports
// this via `resultSortedOn`. The `input` is sorted accordingly by this
// function.
OrderBy makeOrderByOnSortedInput(IdTable input, bool isDescending,
                                 std::vector<ColumnIndex> sortedColumns = {0},
                                 OrderBy::SortIndices sortIndices = {
                                     {0, false}}) {
  auto qec = ad_utility::testing::getQec();
  IdTableUtils::sort(input, sortedColumns);
  std::vector<std::optional<Variable>> vars;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back("?"s + std::to_string(i));
  }
  auto subtree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input), vars, false, std::move(sortedColumns));
  sortIndices.front().second = isDescending;
  return OrderBy{qec, std::move(subtree), std::move(sortIndices)};
}

// Test that the `OrderBy` on the sorted `input` yields the `expected` result
// and takes the fast path for sorted numeric input iff `expectFastPath`. If
// `expected` is not given, the result of a regular (unsorted) `OrderBy` on the
// same input is expected.
void testOrderByOnSortedInput(const IdTable& input, bool isDescending,
                              bool expectFastPath,
                              std::optional<IdTable> expected = std::nullopt,
                              source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  if (!expected.has_value()) {
    expected = makeOrderBy(input.clone(), {{0, isDescending}})
                   .getResult()
                   ->idTableView()
                   .clone();
  }
  OrderBy orderBy = makeOrderByOnSortedInput(input.clone(), isDescending);
  EXPECT_EQ(orderBy.getResult()->idTableView(), expected.value());
  EXPECT_EQ(orderBy.runtimeInfo().details_.contains("sorted-numeric-input"),
            expectFastPath);
}
}  // namespace

// _____________________________________________________________________________
TEST(OrderBy, sortedIntInput) {
  auto I = ad_utility::testing::IntId;
  auto U = Id::makeUndefined();
  auto min = Id::IntegerType::min();
  auto max = Id::IntegerType::max();
  // The second column checks that complete rows are moved.
  VectorTable input{{I(0), I(1)},   {I(3), I(2)},   {I(-1), I(3)},
                    {I(max), I(4)}, {I(-17), I(5)}, {U, I(6)},
                    {I(min), I(7)}, {U, I(8)},      {I(123), I(9)}};
  VectorTable expected{{U, I(6)},      {U, I(8)},      {I(min), I(7)},
                       {I(-17), I(5)}, {I(-1), I(3)},  {I(0), I(1)},
                       {I(3), I(2)},   {I(123), I(9)}, {I(max), I(4)}};
  auto inputTable = makeIdTableFromVector(input);
  testOrderByOnSortedInput(inputTable, false, true,
                           makeIdTableFromVector(expected));
  ql::ranges::reverse(expected);
  testOrderByOnSortedInput(inputTable, true, true,
                           makeIdTableFromVector(expected));

  // Without the `Undefined` values.
  input.erase(input.begin() + 5);
  input.erase(input.begin() + 6);
  testOrderByOnSortedInput(makeIdTableFromVector(input), false, true);
  testOrderByOnSortedInput(makeIdTableFromVector(input), true, true);
}

// _____________________________________________________________________________
TEST(OrderBy, sortedDoubleInput) {
  auto D = ad_utility::testing::DoubleId;
  auto U = Id::makeUndefined();
  auto inf = std::numeric_limits<double>::infinity();
  auto nan = std::numeric_limits<double>::quiet_NaN();
  // `NaN`s with a set sign bit (e.g. the result of `0.0 / 0.0` on x86) are
  // sorted differently in the internal order than `NaN`s with an unset one.
  auto negNan = std::copysign(nan, -1.0);
  ASSERT_TRUE(std::signbit(D(negNan).getDouble()));
  VectorTable input{{D(0.0), D(1)},   {D(2.25), D(2)},   {D(-inf), D(3)},
                    {D(nan), D(4)},   {D(-0.0), D(5)},   {D(-3.5), D(6)},
                    {U, D(7)},        {D(negNan), D(8)}, {D(inf), D(9)},
                    {D(-1e-4), D(10)}};
  // All `NaN`s are greater than all other values. The order among the ties
  // (`-0.0` and `0.0`, the two `NaN`s) is the one produced by the fast path.
  VectorTable expectedAscending{
      {U, D(7)},       {D(-inf), D(3)},  {D(-3.5), D(6)}, {D(-1e-4), D(10)},
      {D(-0.0), D(5)}, {D(0.0), D(1)},   {D(2.25), D(2)}, {D(inf), D(9)},
      {D(nan), D(4)},  {D(negNan), D(8)}};
  VectorTable expectedDescending{
      {D(negNan), D(8)}, {D(nan), D(4)},  {D(inf), D(9)},    {D(2.25), D(2)},
      {D(0.0), D(1)},    {D(-0.0), D(5)}, {D(-1e-4), D(10)}, {D(-3.5), D(6)},
      {D(-inf), D(3)},   {U, D(7)}};
  auto inputTable = makeIdTableFromVector(input);
  testOrderByOnSortedInput(inputTable, false, true,
                           makeIdTableFromVector(expectedAscending));
  testOrderByOnSortedInput(inputTable, true, true,
                           makeIdTableFromVector(expectedDescending));

  // Only `NaN`s and undefined values.
  VectorTable onlyNans{{D(nan), D(1)}, {U, D(2)}, {D(negNan), D(3)}};
  VectorTable onlyNansExpected{{U, D(2)}, {D(nan), D(1)}, {D(negNan), D(3)}};
  testOrderByOnSortedInput(makeIdTableFromVector(onlyNans), false, true,
                           makeIdTableFromVector(onlyNansExpected));
}

// _____________________________________________________________________________
TEST(OrderBy, sortedInputWithoutFastPath) {
  auto I = ad_utility::testing::IntId;
  auto D = ad_utility::testing::DoubleId;
  auto V = ad_utility::testing::VocabId;
  auto U = Id::makeUndefined();
  auto B = Id::makeFromBool;

  // Mixed datatypes in the sort column, the regular sort has to be used.
  for (const VectorTable& input :
       {VectorTable{{I(3)}, {D(-2.5)}, {I(-4)}, {D(7.0)}},
        VectorTable{{I(3)}, {V(1)}, {I(-4)}, {V(0)}},
        VectorTable{{U}, {B(true)}, {I(-4)}, {I(2)}},
        VectorTable{{U}, {V(4)}, {V(2)}}, VectorTable{{U}, {U}}}) {
    auto inputTable = makeIdTableFromVector(input);
    testOrderByOnSortedInput(inputTable, false, false);
    testOrderByOnSortedInput(inputTable, true, false);
  }
  // Empty input.
  auto qec = ad_utility::testing::getQec();
  IdTable emptyTable{1, qec->getAllocator()};
  testOrderByOnSortedInput(emptyTable, false, false);
  testOrderByOnSortedInput(emptyTable, true, false);

  // The fast path is only used if the input is sorted by the sort column, and
  // if there is only a single sort column.
  VectorTable input{{I(3), I(1)}, {I(-2), I(0)}, {I(-4), I(2)}};
  VectorTable expected{{I(-4), I(2)}, {I(-2), I(0)}, {I(3), I(1)}};
  auto expectedTable = makeIdTableFromVector(expected);
  {
    OrderBy orderBy =
        makeOrderByOnSortedInput(makeIdTableFromVector(input), false, {1});
    EXPECT_EQ(orderBy.getResult()->idTableView(), expectedTable);
    EXPECT_FALSE(
        orderBy.runtimeInfo().details_.contains("sorted-numeric-input"));
  }
  {
    OrderBy orderBy = makeOrderByOnSortedInput(
        makeIdTableFromVector(input), false, {0}, {{0, false}, {1, false}});
    EXPECT_EQ(orderBy.getResult()->idTableView(), expectedTable);
    EXPECT_FALSE(
        orderBy.runtimeInfo().details_.contains("sorted-numeric-input"));
  }
}

// _____________________________________________________________________________
TEST(OrderBy, costEstimateForSortedInput) {
  VectorTable input;
  for (int64_t i = 0; i < 1000; ++i) {
    input.push_back({i, 0});
  }
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  // The cost of `ValuesForTesting` is the number of rows, the `OrderBy` on an
  // unsorted input adds `n log n`.
  OrderBy unsorted = makeOrderBy(inputTable.clone(), {{0, false}});
  EXPECT_EQ(unsorted.getCostEstimate(), 1000 + 1000 * 9);
  OrderBy sorted = makeOrderByOnSortedInput(inputTable.clone(), false);
  EXPECT_EQ(sorted.getCostEstimate(), 1000 + 1000);
  // Sorted on the wrong column.
  OrderBy sortedOnOther =
      makeOrderByOnSortedInput(inputTable.clone(), false, {1});
  EXPECT_EQ(sortedOnOther.getCostEstimate(), 1000 + 1000 * 9);
  // Two sort columns, the fast path does not apply.
  OrderBy twoSortColumns = makeOrderByOnSortedInput(
      inputTable.clone(), false, {0}, {{0, false}, {1, false}});
  EXPECT_EQ(twoSortColumns.getCostEstimate(), 1000 + 1000 * 9);
  // Empty input.
  auto qec = ad_utility::testing::getQec();
  OrderBy empty = makeOrderBy(IdTable{1, qec->getAllocator()}, {{0, false}});
  EXPECT_EQ(empty.getCostEstimate(), 0);
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
