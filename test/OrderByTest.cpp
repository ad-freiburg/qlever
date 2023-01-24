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

OrderBy makeOrderBy(IdTable input, const std::vector<std::pair<size_t, bool>>& sortColumns) {
  std::vector<Variable> vars;
  auto qec = ad_utility::testing::getQec();
  for (size_t i = 0; i < input.numColumns(); ++i) {
    vars.emplace_back("?"s + std::to_string(i));
  }
  auto dummy = ad_utility::makeExecutionTree<DummyOperation>(
      ad_utility::testing::getQec(), std::move(input), vars);
  return OrderBy{qec, dummy, sortColumns};
}

void testOrderBy(IdTable input, const IdTable& expected,
              source_location l = source_location::current()) {
  auto trace = generateLocationTrace(l);
  auto qec = ad_utility::testing::getQec();

  std::vector<std::pair<size_t, bool>> sortColumns;
  for (size_t i = 0; i < input.numColumns(); ++i) {
    // TODO<joka921> Also test cases where the order is Ascending.
    sortColumns.push_back(std::pair{i, false});
  }
  do {
    IdTable permutedInput{input.numColumns(), qec->getAllocator()};
    IdTable permutedExpected{expected.numColumns(), qec->getAllocator()};
    permutedInput.resize(input.numRows());
    permutedExpected.resize(expected.numRows());
    for (size_t i = 0; i < sortColumns.size(); ++i) {
      std::ranges::copy(input.getColumn(i),
                        permutedInput.getColumn(sortColumns[i].first).begin());
      std::ranges::copy(expected.getColumn(i),
                        permutedExpected.getColumn(sortColumns[i].first).begin());
    }

    OrderBy s = makeOrderBy(std::move(permutedInput), sortColumns);
    // TODO<joka921> make `computeResult` accesible from tests.
    auto result = s.getResult();
    const auto& resultTable = result->_idTable;
    ASSERT_EQ(resultTable, permutedExpected);
  } while (std::next_permutation(sortColumns.begin(), sortColumns.end()));
}

TEST(OrderBy, ComputeOrderBySingleIntColumn) {
  VectorTable input{{0},   {1},       {-1},  {3},
                    {-17}, {1230957}, {123}, {-1249867132}};
  VectorTable expected{{-1249867132},       {-17},           {-1},   {0},
                       {1}, {3}, {123}, {1230957}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  testOrderBy(std::move(inputTable), expectedTable);
}

TEST(OrderBy, TwoColumnsIntAndFloat) {
  auto qec = ad_utility::testing::getQec();
  IdTable input{2, qec->getAllocator()};
  IdTable expected{2, qec->getAllocator()};

  std::vector<std::pair<int64_t, double>> intsAndFloats{
      {-3, 1.0},  {0, 7.0}, {-3, 0.5},{0, -2.8}};
  std::vector<std::pair<int64_t, double>> intsAndFloatsExpected{
      {-3, 0.5}, {-3, 1.0}, {0, -2.8}, {0, 7.0}};

  AD_CHECK(intsAndFloats.size() == intsAndFloatsExpected.size());
  input.resize(intsAndFloats.size());
  expected.resize(intsAndFloatsExpected.size());

  ASSERT_FALSE(Id::makeFromDouble(1.0) < Id::makeFromDouble(0.5));

  for (size_t i = 0; i < intsAndFloats.size(); ++i) {
    input(i, 0) = Id::makeFromInt(intsAndFloats[i].first);
    input(i, 1) = Id::makeFromDouble(intsAndFloats[i].second);
    expected(i, 0) = Id::makeFromInt(intsAndFloatsExpected[i].first);
    expected(i, 1) = Id::makeFromDouble(intsAndFloatsExpected[i].second);
  }

  testOrderBy(std::move(input), expected);
}

TEST(OrderBy, ComputeOrderByThreeColumns) {
  VectorTable input{
      {-1, 12, -3}, {1, 7, 11}, {-1, 12, -4}, {1, 6, 0}, {1, 7, 11}};
  VectorTable expected{
      {-1, 12, -4}, {-1, 12, -3}, {1, 6, 0}, {1, 7, 11}, {1, 7, 11}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  auto expectedTable = makeIdTableFromVector(expected, &Id::makeFromInt);
  testOrderBy(std::move(inputTable), expectedTable);
}

TEST(OrderBy, SimpleMemberFunctions) {
  VectorTable input{{0},   {1},       {-1},  {3},
                    {-17}, {1230957}, {123}, {-1249867132}};
  auto inputTable = makeIdTableFromVector(input, &Id::makeFromInt);
  OrderBy s = makeOrderBy(std::move(inputTable), {{0, false}});
  EXPECT_EQ(1u, s.getResultWidth());
  EXPECT_EQ("OrderBy on ASC(?0)", s.getDescriptor());

  EXPECT_THAT(s.asString(),
              ::testing::StartsWith("ORDER BY on columns:asc(0) \n"));
  // TODO<joka921> Also test with another input (with multiple columns) for
  // completeness.
}
