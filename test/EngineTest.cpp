// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Co-Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <tuple>

#include "./util/AllocatorTestHelpers.h"
#include "./util/GTestHelpers.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/Join.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"
#include "util/Forward.h"
#include "util/Random.h"
#include "util/SourceLocation.h"

using ad_utility::testing::makeAllocator;
using namespace ad_utility::testing;
namespace {
auto V = VocabId;
constexpr auto U = Id::makeUndefined();
using JoinColumns = std::vector<std::array<ColumnIndex, 2>>;
}  // namespace

TEST(EngineTest, distinctTest) {
  IdTable inp{makeIdTableFromVector(
      {{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  IdTable result{4, makeAllocator()};

  std::vector<size_t> keepIndices{{1, 2}};
  CALL_FIXED_SIZE(4, Engine::distinct, inp, keepIndices, &result);

  // For easier checking.
  IdTable expectedResult{
      makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(expectedResult, result);
}

void testOptionalJoin(const IdTable& inputA, const IdTable& inputB,
                      JoinColumns jcls, const IdTable& expectedResult) {
  // TODO<joka921> Let this use the proper constructor of OptionalJoin.
  IdTable result{inputA.numColumns() + inputB.numColumns() - jcls.size(),
                 makeAllocator()};
  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  OptionalJoin::optionalJoin(inputA, inputB, jcls, &result);
  ASSERT_EQ(expectedResult, result);
}

void testSpecialOptionalJoin(const IdTable& inputA, const IdTable& inputB,
                             JoinColumns jcls, const IdTable& expectedResult) {
  // TODO<joka921> Let this use the proper constructor of OptionalJoin.
  IdTable result{inputA.numColumns() + inputB.numColumns() - jcls.size(),
                 makeAllocator()};
  // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
  // a have to equal those of column 2 of b and vice versa).
  OptionalJoin::specialOptionalJoin(inputA, inputB, jcls, &result);
  ASSERT_EQ(expectedResult, result);
}

// TODO<joka921> This function already exists in another PR in a better version,
// merge them.
IdTable makeTable(const std::vector<std::vector<Id>>& input) {
  size_t numCols = input[0].size();
  IdTable table{numCols, makeAllocator()};
  table.reserve(input.size());
  for (const auto& row : input) {
    table.emplace_back();
    for (size_t i = 0; i < table.numColumns(); ++i) {
      table.back()[i] = row.at(i);
    }
  }
  return table;
}

// TODO<joka921> This function already exists in another PR in a better version,
// merge them.
IdTable makeTableV(
    const std::vector<std::vector<std::variant<Id, int>>>& input) {
  size_t numCols = input[0].size();
  IdTable table{numCols, makeAllocator()};
  table.reserve(input.size());
  for (const auto& row : input) {
    table.emplace_back();
    for (size_t i = 0; i < table.numColumns(); ++i) {
      if (std::holds_alternative<Id>(row.at(i))) {
        table.back()[i] = std::get<Id>(row.at(i));
      } else {
        table.back()[i] = V(std::get<int>(row.at(i)));
      }
    }
  }
  return table;
}

TEST(OptionalJoin, singleColumnRightIsEmpty) {
  auto a = makeTableV({{U}, {2}, {3}});
  IdTable b(1, makeAllocator());
  auto expected = makeTableV({{U}, {2}, {3}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoin, singleColumnLeftIsEmpty) {
  IdTable a(1, makeAllocator());
  auto b = makeTableV({{U}, {2}, {3}});
  testOptionalJoin(a, b, {{0, 0}}, a);
}

TEST(OptionalJoin, singleColumnPreexistingNulloptsLeft) {
  auto a = makeTableV({{U}, {U}, {2}, {3}, {4}});
  auto b = makeTableV({{3}, {5}});
  auto expected = makeTableV({{2}, {3}, {3}, {3}, {4}, {5}, {5}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoin, singleColumnPreexistingNulloptsRight) {
  auto a = makeTableV({{0}, {3}, {5}});
  auto b = makeTableV({{U}, {U}, {2}, {3}, {4}});
  auto expected = makeTableV({{0}, {0}, {3}, {3}, {3}, {5}, {5}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoin, singleColumnPreexistingNulloptsBoth) {
  auto a = makeTableV({{U}, {U}, {0}, {3}, {3}, {5}, {6}});
  auto b = makeTableV({{U}, {2}, {3}, {5}});
  auto expected = makeTableV({{U},
                              {U},
                              {0},
                              {2},
                              {2},
                              {3},
                              {3},
                              {3},
                              {3},
                              {3},
                              {3},
                              {5},
                              {5},
                              {5},
                              {5},
                              {6}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoin, twoColumnsPreexistingUndefLeft) {
  {
    auto a = makeTableV({{U, U}, {U, 3}, {3, U}, {3, U}});
    auto b = makeTableV({{3, 3}});
    auto expected = makeTableV({{3, 3}, {3, 3}, {3, 3}, {3, 3}});
    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }

  {
    auto a = makeTableV({{U, U},
                         {U, 2},
                         {U, 3},
                         {U, 123},
                         {0, 1},
                         {3, U},
                         {3, U},
                         {3, 7},
                         {4, U},
                         {5, 2},
                         {6, U},
                         {18, U}});
    auto b =
        makeTableV({{0, 0}, {0, 1}, {0, 1}, {3, 3}, {5, 2}, {6, 12}, {20, 3}});
    // TODO<joka921> This is the result for the case that the sorting is not
    // corrected.
    /*
    auto expected = makeTableV({{0, 0},  {0, 1},  {0, 1},   {0, 1},  {0, 1},
                                {3, 3},  {3, 3},  {3, 3},   {3, 3},  {3, 7},
                                {5, 2},  {5, 2},  {5, 2},   {6, 12}, {6, 12},
                                {20, 3}, {20, 3}, {U, 123}, {4, U},  {18, U}});
                                */
    auto expected = makeTableV({{U, 123}, {0, 0},  {0, 1},  {0, 1},  {0, 1},
                                {0, 1},   {3, 3},  {3, 3},  {3, 3},  {3, 3},
                                {3, 7},   {4, U},  {5, 2},  {5, 2},  {5, 2},
                                {6, 12},  {6, 12}, {18, U}, {20, 3}, {20, 3}});
    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }
}

TEST(OptionalJoin, twoColumnsPreexistingUndefRight) {
  {
    auto a =
        makeTableV({{0, 0}, {0, 1}, {0, 1}, {3, 3}, {5, 2}, {6, 12}, {20, 3}});
    auto b = makeTableV({{U, U},
                         {U, 2},
                         {U, 3},
                         {U, 123},
                         {0, 1},
                         {3, U},
                         {3, U},
                         {3, 7},
                         {4, U},
                         {5, 2},
                         {6, U},
                         {18, U}});
    auto expected = makeTableV({{0, 0},
                                {0, 1},
                                {0, 1},
                                {0, 1},
                                {0, 1},
                                {3, 3},
                                {3, 3},
                                {3, 3},
                                {3, 3},
                                {5, 2},
                                {5, 2},
                                {5, 2},
                                {6, 12},
                                {6, 12},
                                {20, 3},
                                {20, 3}});

    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }
}

TEST(OptionalJoin, twoColumnsPreexistingUndefBoth) {
  {
    auto a = makeTableV({{12, U}});
    auto b = makeTableV({{U, U}, {U, 3}, {U, 123}});
    auto expected = makeTableV({{12, U}, {12, 3}, {12, 123}});

    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }
  {
    auto a = makeTableV(
        {{0, 0}, {0, 1}, {0, 1}, {3, 3}, {5, U}, {6, 12}, {12, U}, {20, 3}});
    auto b = makeTableV({{U, U},
                         {U, 2},
                         {U, 3},
                         {U, 123},
                         {0, 1},
                         {3, U},
                         {3, U},
                         {3, 7},
                         {4, U},
                         {5, 2},
                         {6, U},
                         {18, U}});
    auto expected =
        makeTableV({{0, 0},  {0, 1},    {0, 1},  {0, 1},  {0, 1},  {3, 3},
                    {3, 3},  {3, 3},    {3, 3},  {5, U},  {5, 2},  {5, 2},
                    {5, 3},  {5, 123},  {6, 12}, {6, 12}, {12, U}, {12, 2},
                    {12, 3}, {12, 123}, {20, 3}, {20, 3}});

    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }
}

TEST(OptionalJoin, multipleColumnsNoUndef) {
  {
    IdTable a{makeIdTableFromVector(
        {{4, 1, 2}, {2, 1, 3}, {1, 1, 4}, {2, 2, 1}, {1, 3, 1}})};
    IdTable b{
        makeIdTableFromVector({{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}})};
    // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
    // a have to equal those of column 2 of b and vice versa).
    JoinColumns jcls{{1, 2}, {2, 1}};

    IdTable expectedResult = makeTableV(
        {{4, 1, 2, U}, {2, 1, 3, 3}, {1, 1, 4, U}, {2, 2, 1, U}, {1, 3, 1, 1}});

    testOptionalJoin(a, b, jcls, expectedResult);
  }

  {
    // Test the optional join with variable sized data.
    IdTable va{makeIdTableFromVector(
        {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5, 6}, {7, 6, 5, 4, 3, 2}})};

    IdTable vb{makeIdTableFromVector({{2, 3, 4}, {2, 3, 5}, {6, 7, 4}})};

    JoinColumns jcls{{1, 0}, {2, 1}};

    // For easier checking.
    auto expectedResult = makeTableV({{1, 2, 3, 4, 5, 6, 4},
                                      {1, 2, 3, 4, 5, 6, 5},
                                      {1, 2, 3, 7, 5, 6, 4},
                                      {1, 2, 3, 7, 5, 6, 5},
                                      {7, 6, 5, 4, 3, 2, U}});

    testOptionalJoin(va, vb, jcls, expectedResult);
  }
}

TEST(OptionalJoin, specialOptionalJoinTwoColumns) {
  {
    IdTable a{makeIdTableFromIdVector({{V(4), V(1), V(2)},
                                       {V(2), V(1), V(3)},
                                       {V(1), V(1), V(4)},
                                       {V(2), V(2), U},
                                       {V(1), V(3), V(1)}})};
    IdTable b{
        makeIdTableFromVector({{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}})};
    // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
    // a have to equal those of column 2 of b and vice versa).
    JoinColumns jcls{{1, 2}, {2, 1}};

    IdTable expectedResult = makeTableV(
        {{4, 1, 2, U}, {2, 1, 3, 3}, {1, 1, 4, U}, {2, 2, 2, 4}, {1, 3, 1, 1}});

    testSpecialOptionalJoin(a, b, jcls, expectedResult);
  }
}
