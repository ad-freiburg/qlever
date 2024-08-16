// Copyright 2015, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Co-Author: Andre Schlegel (November of 2022,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <algorithm>

#include "engine/CallFixedSize.h"
#include "engine/Engine.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/ValuesForTesting.h"
#include "engine/idTable/IdTable.h"
#include "util/AllocatorTestHelpers.h"
#include "util/Forward.h"
#include "util/GTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/Random.h"

using ad_utility::testing::makeAllocator;
using namespace ad_utility::testing;
namespace {
auto V = VocabId;
constexpr auto U = Id::makeUndefined();
using JoinColumns = std::vector<std::array<ColumnIndex, 2>>;
}  // namespace

TEST(EngineTest, distinctTest) {
  IdTable input{makeIdTableFromVector(
      {{1, 1, 3, 7}, {6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  IdTable result{4, makeAllocator()};

  std::vector<ColumnIndex> keepIndices{{1, 2}};
  CALL_FIXED_SIZE(4, Engine::distinct, input, keepIndices, &result);

  // For easier checking.
  IdTable expectedResult{
      makeIdTableFromVector({{1, 1, 3, 7}, {2, 2, 3, 5}, {3, 6, 5, 4}})};
  ASSERT_EQ(expectedResult, result);
}

TEST(EngineTest, distinctWithEmptyInput) {
  IdTable input{1, makeAllocator()};
  // Deliberately input a non-empty result to check that it is
  // overwritten by the (empty) input.
  IdTable result = makeIdTableFromVector({{3}});
  CALL_FIXED_SIZE(1, Engine::distinct, input, std::vector<ColumnIndex>{},
                  &result);
  ASSERT_EQ(input, result);
}

void testOptionalJoin(const IdTable& inputA, const IdTable& inputB,
                      JoinColumns jcls, const IdTable& expectedResult) {
  {
    auto* qec = ad_utility::testing::getQec();
    IdTable result{inputA.numColumns() + inputB.numColumns() - jcls.size(),
                   makeAllocator()};
    // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
    // a have to equal those of column 2 of b and vice versa).
    OptionalJoin{qec, idTableToExecutionTree(qec, inputA),
                 idTableToExecutionTree(qec, inputB)}
        .optionalJoin(inputA, inputB, jcls, &result);
    ASSERT_EQ(expectedResult, result);
  }

  {
    std::vector<std::optional<Variable>> varsLeft;
    for (size_t i = 0; i < inputA.numColumns(); ++i) {
      varsLeft.emplace_back(absl::StrCat("?left_", i));
    }
    std::vector<std::optional<Variable>> varsRight;
    for (size_t i = 0; i < inputB.numColumns(); ++i) {
      varsRight.emplace_back(absl::StrCat("?right_", i));
    }
    size_t idx = 0;
    for (auto [left, right] : jcls) {
      varsLeft.at(left) = Variable(absl::StrCat("?joinColumn_", idx));
      varsRight.at(right) = Variable(absl::StrCat("?joinColumn_", idx));
      ++idx;
    }
    auto qec = ad_utility::testing::getQec();
    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, inputA.clone(), varsLeft);
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, inputB.clone(), varsRight);
    OptionalJoin opt{qec, left, right};

    auto result = opt.computeResultOnlyForTesting();
    ASSERT_EQ(result.idTable(), expectedResult);
  }
}

TEST(OptionalJoin, singleColumnRightIsEmpty) {
  auto a = makeIdTableFromVector({{U}, {2}, {3}});
  IdTable b(1, makeAllocator());
  auto expected = makeIdTableFromVector({{U}, {2}, {3}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoin, singleColumnLeftIsEmpty) {
  IdTable a(1, makeAllocator());
  auto b = makeIdTableFromVector({{U}, {2}, {3}});
  testOptionalJoin(a, b, {{0, 0}}, a);
}

TEST(OptionalJoin, singleColumnPreexistingNulloptsLeft) {
  auto a = makeIdTableFromVector({{U}, {U}, {2}, {3}, {4}});
  auto b = makeIdTableFromVector({{3}, {5}});
  auto expected = makeIdTableFromVector({{2}, {3}, {3}, {3}, {4}, {5}, {5}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoin, singleColumnPreexistingNulloptsRight) {
  auto a = makeIdTableFromVector({{0}, {3}, {5}});
  auto b = makeIdTableFromVector({{U}, {U}, {2}, {3}, {4}});
  auto expected = makeIdTableFromVector({{0}, {0}, {3}, {3}, {3}, {5}, {5}});
  testOptionalJoin(a, b, {{0, 0}}, expected);
}

TEST(OptionalJoin, singleColumnPreexistingNulloptsBoth) {
  auto a = makeIdTableFromVector({{U}, {U}, {0}, {3}, {3}, {5}, {6}});
  auto b = makeIdTableFromVector({{U}, {2}, {3}, {5}});
  auto expected = makeIdTableFromVector({{U},
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
    auto a = makeIdTableFromVector({{U, U}, {U, 3}, {3, U}, {3, U}});
    auto b = makeIdTableFromVector({{3, 3}});
    auto expected = makeIdTableFromVector({{3, 3}, {3, 3}, {3, 3}, {3, 3}});
    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }

  {
    auto a = makeIdTableFromVector({{U, U},
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
    auto b = makeIdTableFromVector(
        {{0, 0}, {0, 1}, {0, 1}, {3, 3}, {5, 2}, {6, 12}, {20, 3}});
    // TODO<joka921> This is the result for the case that the sorting is not
    // corrected.
    /*
    auto expected = makeIdTableFromVector({{0, 0},  {0, 1},  {0, 1},   {0, 1},
    {0, 1}, {3, 3},  {3, 3},  {3, 3},   {3, 3},  {3, 7}, {5, 2},  {5, 2},  {5,
    2},   {6, 12}, {6, 12}, {20, 3}, {20, 3}, {U, 123}, {4, U},  {18, U}});
                                */
    auto expected = makeIdTableFromVector(
        {{U, 123}, {0, 0},  {0, 1},  {0, 1},  {0, 1},  {0, 1}, {3, 3},
         {3, 3},   {3, 3},  {3, 3},  {3, 7},  {4, U},  {5, 2}, {5, 2},
         {5, 2},   {6, 12}, {6, 12}, {18, U}, {20, 3}, {20, 3}});
    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }
}

TEST(OptionalJoin, twoColumnsPreexistingUndefRight) {
  {
    auto a = makeIdTableFromVector(
        {{0, 0}, {0, 1}, {0, 1}, {3, 3}, {5, 2}, {6, 12}, {20, 3}});
    auto b = makeIdTableFromVector({{U, U},
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
    auto expected = makeIdTableFromVector({{0, 0},
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
    auto a = makeIdTableFromVector({{12, U}});
    auto b = makeIdTableFromVector({{U, U}, {U, 3}, {U, 123}});
    auto expected = makeIdTableFromVector({{12, U}, {12, 3}, {12, 123}});

    testOptionalJoin(a, b, {{0, 0}, {1, 1}}, expected);
  }
  {
    auto a = makeIdTableFromVector(
        {{0, 0}, {0, 1}, {0, 1}, {3, 3}, {5, U}, {6, 12}, {12, U}, {20, 3}});
    auto b = makeIdTableFromVector({{U, U},
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
    auto expected = makeIdTableFromVector(
        {{0, 0},  {0, 1},    {0, 1},  {0, 1},  {0, 1},  {3, 3},
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

    IdTable expectedResult = makeIdTableFromVector(
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
    auto expectedResult = makeIdTableFromVector({{1, 2, 3, 4, 5, 6, 4},
                                                 {1, 2, 3, 4, 5, 6, 5},
                                                 {1, 2, 3, 7, 5, 6, 4},
                                                 {1, 2, 3, 7, 5, 6, 5},
                                                 {7, 6, 5, 4, 3, 2, U}});

    testOptionalJoin(va, vb, jcls, expectedResult);
  }
}

TEST(OptionalJoin, specialOptionalJoinTwoColumns) {
  {
    IdTable a{makeIdTableFromVector({{V(4), V(1), V(2)},
                                     {V(2), V(1), V(3)},
                                     {V(1), V(1), V(4)},
                                     {V(2), V(2), U},
                                     {V(1), V(3), V(1)}})};
    IdTable b{
        makeIdTableFromVector({{3, 3, 1}, {1, 8, 1}, {4, 2, 2}, {1, 1, 3}})};
    // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
    // a have to equal those of column 2 of b and vice versa).
    JoinColumns jcls{{1, 2}, {2, 1}};

    IdTable expectedResult = makeIdTableFromVector(
        {{4, 1, 2, U}, {2, 1, 3, 3}, {1, 1, 4, U}, {2, 2, 2, 4}, {1, 3, 1, 1}});

    testOptionalJoin(a, b, jcls, expectedResult);
  }
}

TEST(OptionalJoin, gallopingJoin) {
  {
    IdTable a{makeIdTableFromVector({{5}, {327}, {4938}, {100000000}})};
    IdTable expectedResult{makeIdTableFromVector(
        {{5, 17}, {327, U}, {4938, 4950}, {100000000, U}})};

    VectorTable bInput;
    for (int64_t i = 0; i < 300; ++i) {
      bInput.emplace_back(std::vector<IntOrId>{i, i + 12});
    }
    auto numElementsInLarger = static_cast<int64_t>(
        std::max(10000ul, a.numRows() * GALLOP_THRESHOLD + 1));
    for (int64_t i = 400; i < numElementsInLarger; ++i) {
      bInput.emplace_back(std::vector<IntOrId>{i, i + 12});
    }
    IdTable b{makeIdTableFromVector(bInput)};
    // Join on the first column
    JoinColumns jcls{{0, 0}};

    testOptionalJoin(a, b, jcls, expectedResult);
  }
  // Also test the case that the largest element of `a` is less than the largest
  // element of `b`.
  {
    IdTable a{makeIdTableFromVector({{5}, {327}, {328}})};
    IdTable expectedResult{
        makeIdTableFromVector({{5, 17}, {327, U}, {328, U}})};

    VectorTable bInput;
    for (int64_t i = 0; i < 300; ++i) {
      bInput.emplace_back(std::vector<IntOrId>{i, i + 12});
    }
    auto numElementsInLarger = static_cast<int64_t>(
        std::max(10000ul, a.numRows() * GALLOP_THRESHOLD + 1));
    for (int64_t i = 400; i < numElementsInLarger; ++i) {
      bInput.emplace_back(std::vector<IntOrId>{i, i + 12});
    }
    IdTable b{makeIdTableFromVector(bInput)};
    // Join on the first column
    JoinColumns jcls{{0, 0}};

    testOptionalJoin(a, b, jcls, expectedResult);
  }
}

// _____________________________________________________________________________
TEST(Engine, countDistinct) {
  auto alloc = ad_utility::testing::makeAllocator();
  IdTable t1(alloc);
  t1.setNumColumns(0);
  auto noop = []() {};
  EXPECT_EQ(0u, Engine::countDistinct(t1, noop));
  t1.setNumColumns(3);
  EXPECT_EQ(0u, Engine::countDistinct(t1, noop));

  // 0 columns, but multiple rows;
  t1.setNumColumns(0);
  t1.resize(1);
  EXPECT_EQ(1u, Engine::countDistinct(t1, noop));
  t1.resize(5);
  EXPECT_EQ(1u, Engine::countDistinct(t1, noop));

  t1 = makeIdTableFromVector(
      {{0, 0}, {0, 0}, {1, 3}, {1, 4}, {1, 4}, {4, 4}, {4, 5}, {4, 7}});
  EXPECT_EQ(6u, Engine::countDistinct(t1, noop));

  t1 = makeIdTableFromVector(
      {{0, 0}, {1, 4}, {1, 3}, {1, 4}, {1, 4}, {4, 4}, {4, 5}, {4, 7}});

  if constexpr (ad_utility::areExpensiveChecksEnabled) {
    AD_EXPECT_THROW_WITH_MESSAGE(Engine::countDistinct(t1, noop),
                                 ::testing::HasSubstr("must be sorted"));
  }
}
