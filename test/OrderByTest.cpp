//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "./IndexTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "engine/DummyOperation.h"
#include "engine/OrderBy.h"
#include "global/ValueIdComparators.h"

using namespace std::string_literals;
using ad_utility::source_location;

namespace {
// Create an `OrderBy` operation that sorts the `input` by the `sortColumns`.
OrderBy makeOrderBy(IdTable input, const OrderBy::SortIndices& sortColumns) {
  std::vector<Variable> vars;
  auto qec = ad_utility::testing::getQec();
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back("?"s + std::to_string(i));
  }
  auto dummy = ad_utility::makeExecutionTree<DummyOperation>(
      ad_utility::testing::getQec(), std::move(input), vars);
  return OrderBy{qec, dummy, sortColumns};
}

// Test that the `input`, when being sorted by its 0-th column as its primary
// key, its 1st column as its secondary key etc. using an `ORDER BY` operation,
// yields the `expected` result. The `isDescending` vector specifies, which of
// the columns of `expected` are sorted in descending order. The test is
// performed for all possible permutations of the sort columns by also permuting
// `input` and `expected` accordingly.
void testOrderBy(IdTable input, const IdTable& expected,
                 std::vector<bool> isDescending,
                 source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l);
  auto qec = ad_utility::testing::getQec();

  AD_CONTRACT_CHECK(input.numColumns() == isDescending.size());
  AD_CONTRACT_CHECK(input.numColumns() == expected.numColumns());
  AD_CONTRACT_CHECK(input.numRows() == expected.numRows());
  // Set up a vector of `SortIndices`. Those will later be permuted.
  // The second element (`isDescending`) will be correctly set later.
  OrderBy::SortIndices sortColumns;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    sortColumns.push_back(std::pair{i, false});
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
      std::ranges::copy(input.getColumn(i),
                        permutedInput.getColumn(sortColumns[i].first).begin());
      std::ranges::copy(
          expected.getColumn(i),
          permutedExpected.getColumn(sortColumns[i].first).begin());
      // Also put the information which columns are descending into the correct
      // column.
      sortColumns[i].second = isDescending[i];
    }

    OrderBy s = makeOrderBy(std::move(permutedInput), sortColumns);
    auto result = s.getResult();
    const auto& resultTable = result->_idTable;
    ASSERT_EQ(resultTable, permutedExpected);
  } while (std::next_permutation(sortColumns.begin(), sortColumns.end()));
}
}

TEST(OrderBy, ComputeOrderBySingleIntColumn) {
  VectorTable input{{0},   {1},       {-1},  {3},
                    {-17}, {1230957}, {123}, {-1249867132}};
  VectorTable expected{{-1249867132}, {-17}, {-1},  {0},
                       {1},           {3},   {123}, {1230957}};
  VectorTable expectedDescending{{1230957}, {123}, {3},   {1},
                                 {0},       {-1},  {-17}, {-1249867132}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  auto expectedDescendingTable =
      makeIdTableFromVector(expectedDescending, &Id::makeFromInt);
  testOrderBy(inputTable.clone(), expectedTable, {false});
  testOrderBy(inputTable.clone(), expectedDescendingTable, {true});
}

TEST(OrderBy, TwoColumnsIntAndFloat) {
  auto qec = ad_utility::testing::getQec();
  using Vec = std::vector<std::pair<int64_t, double>>;
  Vec intsAndFloats{{-3, 1.0}, {0, 7.0}, {-3, 0.5}, {0, -2.8}};
  Vec intsAndFloatsExpected{{-3, 0.5}, {-3, 1.0}, {0, -2.8}, {0, 7.0}};
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
  auto expected = makeTable(intsAndFloatsExpected);
  auto firstDescending = makeTable(intsAndFloatsExpectedFirstDescending);
  auto secondDescending = makeTable(intsAndFloatsExpectedSecondDescending);
  auto bothDescending = makeTable(intsAndFloatsExpectedBothDescending);

  testOrderBy(input.clone(), expected, {false, false});
  testOrderBy(input.clone(), firstDescending, {true, false});
  testOrderBy(input.clone(), secondDescending, {false, true});
  testOrderBy(input.clone(), bothDescending, {true, true});
}

TEST(OrderBy, ComputeOrderByThreeColumns) {
  VectorTable input{
      {-1, 12, -3}, {1, 7, 11}, {-1, 12, -4}, {1, 6, 0}, {1, 7, 11}};
  VectorTable expected{
      {-1, 12, -4}, {-1, 12, -3}, {1, 6, 0}, {1, 7, 11}, {1, 7, 11}};
  VectorTable expectedFirstAndThirdDescending{
      {1, 6, 0}, {1, 7, 11}, {1, 7, 11}, {-1, 12, -3}, {-1, 12, -4}};
  VectorTable expectedAllDescending{
      {1, 7, 11}, {1, 7, 11}, {1, 6, 0}, {-1, 12, -3}, {-1, 12, -4}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  auto expectedTableFirstAndThirdDescending =
      makeIdTableFromVector(expectedFirstAndThirdDescending, &Id::makeFromInt);
  auto expectedTableAllDescending =
      makeIdTableFromVector(expectedAllDescending, &Id::makeFromInt);
  testOrderBy(inputTable.clone(), expectedTable, {false, false, false});
  testOrderBy(inputTable.clone(), expectedTableFirstAndThirdDescending,
              {true, false, true});
  testOrderBy(inputTable.clone(), expectedTableAllDescending,
              {true, true, true});
}

TEST(OrderBy, SimpleMemberFunctions) {
  {
    VectorTable input{{0},   {1},       {-1},  {3},
                      {-17}, {1230957}, {123}, {-1249867132}};
    auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
    OrderBy s = makeOrderBy(std::move(inputTable), {{0, false}});
    EXPECT_EQ(1u, s.getResultWidth());
    EXPECT_EQ(8u, s.getSizeEstimate());
    EXPECT_EQ("OrderBy on ASC(?0)", s.getDescriptor());

    EXPECT_THAT(s.asString(),
                ::testing::StartsWith("ORDER BY on columns:asc(0) \n"));

    auto varColMap = s.getExternallyVisibleVariableColumns();
    ASSERT_EQ(1u, varColMap.size());
    ASSERT_EQ(0u, varColMap.at(Variable{"?0"}));
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

    EXPECT_THAT(s.asString(),
                ::testing::StartsWith("ORDER BY on columns:asc(1) desc(0) \n"));
    auto varColMap = s.getExternallyVisibleVariableColumns();
    ASSERT_EQ(2u, varColMap.size());
    ASSERT_EQ(0u, varColMap.at(Variable{"?0"}));
    ASSERT_EQ(1u, varColMap.at(Variable{"?1"}));
    EXPECT_FALSE(s.knownEmptyResult());
    EXPECT_EQ(42.0, s.getMultiplicity(0));
    EXPECT_EQ(84.0, s.getMultiplicity(1));
  }
}
