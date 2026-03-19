// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/AllocatorTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "../util/TripleComponentTestHelpers.h"
#include "./LazyJoinTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/JoinHelpers.h"
#include "engine/NeutralOptional.h"
#include "engine/OptionalJoin.h"
#include "engine/QueryExecutionTree.h"
#include "engine/idTable/IdTable.h"

using ad_utility::testing::makeAllocator;
using namespace ad_utility::testing;
namespace {
auto V = VocabId;
constexpr auto U = Id::makeUndefined();
using JoinColumns = std::vector<std::array<ColumnIndex, 2>>;

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
    std::vector<ColumnIndex> leftSorted;
    std::vector<ColumnIndex> rightSorted;
    size_t idx = 0;
    for (auto [left, right] : jcls) {
      varsLeft.at(left) = Variable(absl::StrCat("?joinColumn_", idx));
      varsRight.at(right) = Variable(absl::StrCat("?joinColumn_", idx));
      leftSorted.push_back(left);
      rightSorted.push_back(right);
      ++idx;
    }
    auto qec = ad_utility::testing::getQec();
    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, inputA.clone(), varsLeft, false, std::move(leftSorted));
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, inputB.clone(), varsRight, false, std::move(rightSorted));
    OptionalJoin opt{qec, left, right};

    auto result = opt.computeResultOnlyForTesting();
    ASSERT_EQ(result.idTable(), expectedResult);
  }
}

// Helper function to test lazy join implementations.
void testLazyOptionalJoin(
    std::vector<IdTable> leftTables, std::vector<IdTable> rightTables,
    const std::vector<IdTable>& expectedResult, bool singleVar = false,
    ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) {
  auto g = generateLocationTrace(location);
  auto qec = ad_utility::testing::getQec();

  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(leftTables),
      singleVar ? std::vector<std::optional<Variable>>{Variable{"?x"}}
                : std::vector<std::optional<Variable>>{Variable{"?x"},
                                                       Variable{"?y"}},
      false, std::vector<ColumnIndex>{0});
  auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(rightTables),
      singleVar ? std::vector<std::optional<Variable>>{Variable{"?x"}}
                : std::vector<std::optional<Variable>>{Variable{"?x"},
                                                       Variable{"?z"}},
      false, std::vector<ColumnIndex>{0});
  OptionalJoin opt{qec, left, right};

  {
    qec->getQueryTreeCache().clearAll();

    auto result = opt.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    std::vector<IdTable> actualResult;

    for (auto& [idTable, _] : result.idTables()) {
      actualResult.push_back(std::move(idTable));
    }

    // Provide nicer error messages
    EXPECT_EQ(actualResult.size(), expectedResult.size());
    EXPECT_EQ(actualResult, expectedResult);
  }

  {
    qec->getQueryTreeCache().clearAll();

    auto result = opt.computeResultOnlyForTesting(false);

    ASSERT_TRUE(result.isFullyMaterialized());

    IdTable expected{opt.getResultWidth(), qec->getAllocator()};

    for (const IdTable& idTable : expectedResult) {
      expected.insertAtEnd(idTable);
    }

    EXPECT_EQ(result.idTable(), expected);
  }
}
}  // namespace

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
  {
    // Test a corner case that previously contained a bug.
    IdTable a{makeIdTableFromVector({{4, U, 2}})};
    IdTable b{makeIdTableFromVector({{3, 3, 1}})};
    // Join a and b on the column pairs 1,2 and 2,1 (entries from columns 1 of
    // a have to equal those of column 2 of b and vice versa).
    JoinColumns jcls{{1, 2}, {2, 1}};

    IdTable expectedResult = makeIdTableFromVector({{4, U, 2, U}});

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
TEST(OptionalJoin, computeOptionalJoinIndexNestedLoopJoinOptimization) {
  LocalVocabEntry entryA = LocalVocabEntry::fromStringRepresentation("\"a\"");
  LocalVocabEntry entryB = LocalVocabEntry::fromStringRepresentation("\"b\"");

  LocalVocab leftVocab;
  leftVocab.getIndexAndAddIfNotContained(entryA);
  LocalVocab rightVocab;
  rightVocab.getIndexAndAddIfNotContained(entryB);

  // From this table columns 1 and 2 will be used for the join.
  IdTable a = makeIdTableFromVector(
      {{1, 1, 2}, {4, 2, 1}, {2, 8, 1}, {3, 8, 2}, {4, 8, 2}});

  // From this table columns 2 and 1 will be used for the join.
  // This is deliberately not sorted to check the optimization that avoids
  // sorting on the right if bigger
  IdTable b = makeIdTableFromVector({{7, 2, 1, 5},
                                     {1, 3, 3, 5},
                                     {1, 8, 1, 5},
                                     {7, 2, 8, 14},
                                     {6, 2, 8, 12},
                                     {14, 15, 16, 17}});
  IdTable expected = makeIdTableFromVector({{1, 1, 2, 7, 5},
                                            {3, 8, 2, 7, 14},
                                            {4, 8, 2, 7, 14},
                                            {3, 8, 2, 6, 12},
                                            {4, 8, 2, 6, 12},
                                            {4, 2, 1, U, U},
                                            {2, 8, 1, U, U}});

  auto* qec = ad_utility::testing::getQec();
  for (bool forceFullyMaterialized : {false, true}) {
    OptionalJoin optionalJoin{
        qec,
        ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, a.clone(),
            std::vector<std::optional<Variable>>{std::nullopt, Variable{"?a"},
                                                 Variable{"?b"}},
            false, std::vector<ColumnIndex>{1, 2}, leftVocab.clone()),
        ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, b.clone(),
            std::vector<std::optional<Variable>>{std::nullopt, Variable{"?b"},
                                                 Variable{"?a"}, std::nullopt},
            false, std::vector<ColumnIndex>{}, rightVocab.clone(), std::nullopt,
            forceFullyMaterialized)};
    auto result = optionalJoin.computeResultOnlyForTesting(false);
    ASSERT_TRUE(result.isFullyMaterialized());

    EXPECT_EQ(result.idTable(), expected);
    EXPECT_THAT(result.localVocab().getAllWordsForTesting(),
                ::testing::UnorderedElementsAre(entryA, entryB));

    const auto& runtimeInfo =
        optionalJoin.getChildren().at(1)->getRootOperation()->runtimeInfo();
    EXPECT_EQ(runtimeInfo.status_, RuntimeInformation::Status::optimizedOut);
    EXPECT_EQ(runtimeInfo.numRows_, 0);
  }
}

// _____________________________________________________________________________
TEST(OptionalJoin, computeLazyOptionalJoinIndexNestedLoopJoinOptimization) {
  LocalVocabEntry entryA = LocalVocabEntry::fromStringRepresentation("\"a\"");
  LocalVocabEntry entryB = LocalVocabEntry::fromStringRepresentation("\"b\"");

  LocalVocab leftVocab;
  leftVocab.getIndexAndAddIfNotContained(entryA);
  LocalVocab rightVocab;
  rightVocab.getIndexAndAddIfNotContained(entryB);

  // From this table columns 1 and 2 will be used for the join.
  IdTable a = makeIdTableFromVector(
      {{1, 1, 2}, {4, 2, 1}, {2, 8, 1}, {3, 8, 2}, {4, 8, 2}});

  // From these tables columns 2 and 1 will be used for the join.
  // This is deliberately not sorted to check the optimization that avoids
  // sorting on the right if bigger
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector(
      {{7, 2, 1, 5}, {1, 3, 3, 5}, {1, 8, 1, 5}, {7, 2, 8, 14}}));
  rightTables.push_back(
      makeIdTableFromVector({{6, 2, 8, 12}, {14, 15, 16, 17}}));

  auto expected0 = makeIdTableFromVector(
      {{1, 1, 2, 7, 5}, {3, 8, 2, 7, 14}, {4, 8, 2, 7, 14}});
  auto expected1 = makeIdTableFromVector({{3, 8, 2, 6, 12}, {4, 8, 2, 6, 12}});
  auto expected2 = makeIdTableFromVector({{4, 2, 1, U, U}, {2, 8, 1, U, U}});

  auto* qec = ad_utility::testing::getQec();
  OptionalJoin optionalJoin{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, std::move(a),
          std::vector<std::optional<Variable>>{std::nullopt, Variable{"?a"},
                                               Variable{"?b"}},
          false, std::vector<ColumnIndex>{1, 2}, std::move(leftVocab)),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, std::move(rightTables),
          std::vector<std::optional<Variable>>{std::nullopt, Variable{"?b"},
                                               Variable{"?a"}, std::nullopt},
          false, std::vector<ColumnIndex>{}, std::move(rightVocab))};
  auto result = optionalJoin.computeResultOnlyForTesting(true);
  ASSERT_FALSE(result.isFullyMaterialized());

  std::vector<IdTable> actualTables;
  std::vector<LocalVocab> actualVocabs;
  for (auto& [idTable, localVocab] : result.idTables()) {
    actualTables.emplace_back(std::move(idTable));
    actualVocabs.emplace_back(std::move(localVocab));
  }

  using namespace ::testing;

  EXPECT_THAT(actualTables,
              ElementsAre(Eq(std::cref(expected0)), Eq(std::cref(expected1)),
                          Eq(std::cref(expected2))));
  ASSERT_EQ(actualVocabs.size(), 3);
  EXPECT_THAT(actualVocabs.at(0).getAllWordsForTesting(),
              UnorderedElementsAre(entryA, entryB));
  EXPECT_THAT(actualVocabs.at(1).getAllWordsForTesting(),
              UnorderedElementsAre(entryA, entryB));
  EXPECT_THAT(actualVocabs.at(2).getAllWordsForTesting(),
              UnorderedElementsAre(entryA));

  const auto& runtimeInfo =
      optionalJoin.getChildren().at(1)->getRootOperation()->runtimeInfo();
  EXPECT_EQ(runtimeInfo.status_, RuntimeInformation::Status::optimizedOut);
  EXPECT_EQ(runtimeInfo.numRows_, 0);
}

// _____________________________________________________________________________
TEST(OptionalJoin, clone) {
  auto qec = ad_utility::testing::getQec();
  auto a = makeIdTableFromVector({{0}});
  auto left = idTableToExecutionTree(qec, a);
  auto right = idTableToExecutionTree(qec, a);
  OptionalJoin opt{qec, left, right};

  auto clone = opt.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(opt, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), opt.getDescriptor());
}

// _____________________________________________________________________________
TEST(OptionalJoin, lazyOptionalJoin) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{V(1), V(11), U},
                                            {V(2), V(10), V(22)},
                                            {V(2), V(12), V(22)},
                                            {V(3), V(10), V(23)},
                                            {V(3), V(13), V(23)},
                                            {V(4), V(14), U},
                                            {V(5), V(15), U}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
  leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));
  leftTables.push_back(makeIdTableFromVector({{4, 14}, {5, 15}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{V(2), V(22)}}));
  rightTables.push_back(makeIdTableFromVector({{3, 23}}));

  testLazyOptionalJoin(std::move(leftTables), std::move(rightTables),
                       std::move(expected));
}

// _____________________________________________________________________________
TEST(OptionalJoin, lazyOptionalJoinWithUndefRight) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{U, V(10), V(20)},
                                            {V(1), V(11), V(20)},
                                            {V(2), V(12), V(20)},
                                            {V(2), V(10), V(22)},
                                            {V(2), V(12), V(22)},
                                            {V(3), V(13), V(20)}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
  leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{U, V(20)}, {V(2), V(22)}}));

  testLazyOptionalJoin(std::move(leftTables), std::move(rightTables),
                       std::move(expected));
}

// _____________________________________________________________________________
TEST(OptionalJoin, lazyOptionalJoinWithUndefLeft) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{V(1), V(10), V(101)},
                                            {V(1), V(11), V(101)},
                                            {V(3), V(10), V(303)},
                                            {V(3), V(12), V(303)}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(
      makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}, {V(3), V(12)}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(
      makeIdTableFromVector({{V(1), V(101)}, {V(3), V(303)}}));

  testLazyOptionalJoin(std::move(leftTables), std::move(rightTables),
                       std::move(expected));
}

// _____________________________________________________________________________
TEST(OptionalJoin, lazyOptionalJoinWithUndefLeftInSeparateTable) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{V(1), V(10), V(101)},
                                            {V(1), V(11), V(101)},
                                            {V(3), V(10), V(303)},
                                            {V(3), V(12), V(303)}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}}));
  leftTables.push_back(makeIdTableFromVector({{1, 11}, {3, 12}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{1, 101}, {3, 303}}));

  testLazyOptionalJoin(std::move(leftTables), std::move(rightTables),
                       std::move(expected));
}

// _____________________________________________________________________________
TEST(OptionalJoin, lazyOptionalJoinWithOneMaterializedTable) {
  auto qec = ad_utility::testing::getQec();

  auto expected = makeIdTableFromVector({{U, V(10), V(20)},
                                         {V(1), V(11), V(20)},
                                         {V(2), V(12), V(20)},
                                         {V(2), V(10), V(22)},
                                         {V(2), V(12), V(22)},
                                         {V(3), V(13), V(20)}});

  {
    std::vector<IdTable> rightTables;
    rightTables.push_back(makeIdTableFromVector({{U, V(20)}, {V(2), V(22)}}));

    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec,
        makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}, {2, 12}, {3, 13}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false, std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(rightTables),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::vector<ColumnIndex>{0});
    OptionalJoin opt{qec, left, right};

    qec->getQueryTreeCache().clearAll();

    auto result = opt.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    auto lazyResult = result.idTables();
    auto it = lazyResult.begin();
    ASSERT_NE(it, lazyResult.end());

    EXPECT_EQ(it->idTable_, expected);

    EXPECT_EQ(++it, lazyResult.end());
  }

  {
    std::vector<IdTable> leftTables;
    leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
    leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));

    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(leftTables),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false, std::vector<ColumnIndex>{0});
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{U, V(20)}, {V(2), V(22)}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    OptionalJoin opt{qec, left, right};

    qec->getQueryTreeCache().clearAll();

    auto result = opt.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    auto lazyResult = result.idTables();
    auto it = lazyResult.begin();
    ASSERT_NE(it, lazyResult.end());

    EXPECT_EQ(it->idTable_, expected);

    EXPECT_EQ(++it, lazyResult.end());
  }
}

// _____________________________________________________________________________
TEST(OptionalJoin, lazyOptionalJoinExceedingChunkSize) {
  std::vector<IdTable> expected;
  expected.push_back(createIdTableOfSizeWithValue(
      qlever::joinHelpers::CHUNK_SIZE, Id::makeFromInt(1)));
  expected.push_back(createIdTableOfSizeWithValue(
      qlever::joinHelpers::CHUNK_SIZE, Id::makeFromInt(2)));

  std::vector<IdTable> leftTables;
  leftTables.push_back(
      makeIdTableFromVector({{Id::makeFromInt(1)}, {Id::makeFromInt(2)}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(createIdTableOfSizeWithValue(
      qlever::joinHelpers::CHUNK_SIZE, Id::makeFromInt(1)));
  rightTables.push_back(createIdTableOfSizeWithValue(
      qlever::joinHelpers::CHUNK_SIZE, Id::makeFromInt(2)));

  testLazyOptionalJoin(std::move(leftTables), std::move(rightTables),
                       std::move(expected), true);
}

// _____________________________________________________________________________
TEST(OptionalJoin, columnOriginatesFromGraphOrUndef) {
  using ad_utility::triple_component::Iri;
  auto* qec = getQec();
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto index1 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?c"}});
  auto index2 = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Variable{"?b"}});
  auto index3 = ad_utility::makeExecutionTree<NeutralOptional>(
      qec, ad_utility::makeExecutionTree<IndexScan>(
               qec, Permutation::PSO,
               SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                                  Variable{"?c"}}));

  auto testWithTrees =
      [qec](std::shared_ptr<QueryExecutionTree> left,
            std::shared_ptr<QueryExecutionTree> right, bool a, bool b, bool c,
            ad_utility::source_location location = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(location);

        OptionalJoin optional{qec, std::move(left), std::move(right)};
        EXPECT_EQ(optional.columnOriginatesFromGraphOrUndef(Variable{"?a"}), a);
        EXPECT_EQ(optional.columnOriginatesFromGraphOrUndef(Variable{"?b"}), b);
        EXPECT_EQ(optional.columnOriginatesFromGraphOrUndef(Variable{"?c"}), c);
        EXPECT_THROW(
            optional.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
            ad_utility::Exception);
      };

  OptionalJoin optional{qec, values1, values1};
  EXPECT_FALSE(optional.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(optional.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_THROW(
      optional.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  testWithTrees(values1, values2, false, false, false);
  testWithTrees(index1, values1, true, false, true);
  testWithTrees(values1, index1, false, false, true);
  testWithTrees(index1, index2, true, true, true);
  testWithTrees(index3, index2, true, true, true);
  testWithTrees(index3, values1, false, false, true);
}

// _____________________________________________________________________________
// Test fixture for testing optionalJoinWithIndexScan with prefiltering.
class OptionalJoinWithIndexScan
    : public ::testing::TestWithParam<bool>,
      public ad_utility::testing::LazyJoinTestHelper {
 protected:
  void SetUp() override {
    // Create a small knowledge graph with controlled block structure.
    // Using 8 bytes per column gives us a single triple per block.
    std::string kg =
        "<a> <p> <A> . <a> <p> <A2> . "
        "<b> <p> <B> . <b> <p> <B2> . "
        "<c> <p> <C> . <c> <p> <C2> . "
        "<d> <p> <D> . "
        "<e> <p> <E> . ";
    setupQecWithKnowledgeGraph(kg, 8_B);
  }

  // Create a common IndexScan instance for the right side.
  std::shared_ptr<QueryExecutionTree> makeIndexScan() const {
    using Tc = TripleComponent;
    using Var = Variable;
    SparqlTripleSimple xpy{Tc{Var{"?x"}}, iri("<p>"), Tc{Var{"?y"}}};
    return ad_utility::makeExecutionTree<IndexScan>(qec_, Permutation::PSO,
                                                    xpy);
  }

  // Create a ValuesForTesting instance for the left side (single column).
  std::shared_ptr<QueryExecutionTree> makeLeftSide(
      IdTable table, std::vector<ColumnIndex> sortedColumns = {0}) const {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec_, std::move(table),
        std::vector<std::optional<Variable>>{Variable{"?x"}}, false,
        std::move(sortedColumns));
  }

  // Create a ValuesForTesting instance for the left side (two columns).
  std::shared_ptr<QueryExecutionTree> makeLeftSide2Col(
      IdTable table, std::vector<ColumnIndex> sortedColumns = {0, 1}) const {
    return ad_utility::makeExecutionTree<ValuesForTesting>(
        qec_, std::move(table),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::move(sortedColumns));
  }

  // Turn the `result` into an `IdTable`, no matter whether it was materialized
  // or not.
  IdTable materializeResult(OptionalJoin& optJoin, const Result& result,
                            bool requestLaziness) const {
    EXPECT_EQ(requestLaziness, !result.isFullyMaterialized());
    if (!result.isFullyMaterialized()) {
      IdTable lazyResult{optJoin.getResultWidth(), qec_->getAllocator()};
      for (auto& [idTable, _] : result.idTables()) {
        lazyResult.insertAtEnd(idTable);
      }
      return lazyResult;
    } else {
      return result.idTable().clone();
    }
  }
  // Helper to verify that lazy and materialized results match.
  void verifyLazyAndMaterializedMatch(OptionalJoin& optJoin,
                                      const IdTable& expected) const {
    bool requestLaziness = GetParam();
    auto result = optJoin.computeResultOnlyForTesting(requestLaziness);
    auto actual = materializeResult(optJoin, result, requestLaziness);
    EXPECT_EQ(actual, expected);
  }

  // Helper to check runtime info for prefiltering statistics.
  void checkPrefilteringStats(OptionalJoin& optJoin, size_t expectedBlocksRead,
                              size_t expectedBlocksAll) const {
    const auto& rightOp = optJoin.getChildren().at(1)->getRootOperation();
    const auto& rti = rightOp->runtimeInfo();
    EXPECT_EQ(rti.details_["num-blocks-read"], expectedBlocksRead);
    EXPECT_EQ(rti.details_["num-blocks-all"], expectedBlocksAll);
    // We have configured a single triple per block, so those numbers should be
    // equal.
    EXPECT_EQ(rti.details_["num-elements-read"], expectedBlocksRead);
  }
};

// _____________________________________________________________________________
TEST_P(OptionalJoinWithIndexScan, singleColumnBasicFiltering) {
  // Test basic prefiltered optional join with single join column.
  // Left side has entries that match some (but not all) blocks on the right.
  auto left = makeLeftSide(makeIdTable({iri("<a>"), iri("<c>"), iri("<e>")}));
  auto right = makeIndexScan();

  OptionalJoin optJoin{qec_, left, right};
  qec_->getQueryTreeCache().clearAll();

  auto expected =
      makeIdTableFromVector({{toValueId(iri("<a>")), toValueId(iri("<A>"))},
                             {toValueId(iri("<a>")), toValueId(iri("<A2>"))},
                             {toValueId(iri("<c>")), toValueId(iri("<C>"))},
                             {toValueId(iri("<c>")), toValueId(iri("<C2>"))},
                             {toValueId(iri("<e>")), toValueId(iri("<E>"))}});

  verifyLazyAndMaterializedMatch(optJoin, expected);

  // Verify prefiltering occurred: blocks read should be less than total blocks.
  // Each result is equal to one block (one triple per block in the
  // configuration), so 5 results means 5 blocks read.
  checkPrefilteringStats(optJoin, 5, 8);
}

// _____________________________________________________________________________
TEST_P(OptionalJoinWithIndexScan, singleColumnWithUndef) {
  // Test with UNDEF values on the left side.
  // Note: UNDEF values must be at the front to keep the table sorted.
  // With prefiltered optional join, UNDEFs work correctly.
  IdTable leftTable{1, makeAllocator()};
  leftTable.push_back({Id::makeUndefined()});
  leftTable.push_back({toValueId(iri("<a>"))});

  auto left = makeLeftSide(std::move(leftTable));
  auto right = makeIndexScan();

  OptionalJoin optJoin{qec_, left, right};
  qec_->getQueryTreeCache().clearAll();

  bool requestLaziness = GetParam();
  auto result = optJoin.computeResultOnlyForTesting(requestLaziness);

  IdTable actual = materializeResult(optJoin, result, requestLaziness);

  // UNDEF matches all 8 rows, and `<a>` matches 2 rows.
  EXPECT_EQ(actual.numRows(), 10);
  EXPECT_EQ(actual.numColumns(), 2);

  // All blocks are read, because undef matches everything.
  checkPrefilteringStats(optJoin, 8, 8);
}

// _____________________________________________________________________________
TEST_P(OptionalJoinWithIndexScan, singleColumnEmptyLeft) {
  // Test with empty left side.
  IdTable emptyTable{1, makeAllocator()};
  auto left = makeLeftSide(std::move(emptyTable));
  auto right = makeIndexScan();

  OptionalJoin optJoin{qec_, left, right};
  qec_->getQueryTreeCache().clearAll();

  IdTable expected{2, makeAllocator()};
  verifyLazyAndMaterializedMatch(optJoin, expected);
}

// _____________________________________________________________________________
TEST_P(OptionalJoinWithIndexScan, singleColumnAllBlocksMatch) {
  // Test when all blocks on right are needed.
  auto left = makeLeftSide(makeIdTable(
      {iri("<a>"), iri("<b>"), iri("<c>"), iri("<d>"), iri("<e>")}));
  auto right = makeIndexScan();

  OptionalJoin optJoin{qec_, left, right};
  qec_->getQueryTreeCache().clearAll();

  auto expected =
      makeIdTableFromVector({{toValueId(iri("<a>")), toValueId(iri("<A>"))},
                             {toValueId(iri("<a>")), toValueId(iri("<A2>"))},
                             {toValueId(iri("<b>")), toValueId(iri("<B>"))},
                             {toValueId(iri("<b>")), toValueId(iri("<B2>"))},
                             {toValueId(iri("<c>")), toValueId(iri("<C>"))},
                             {toValueId(iri("<c>")), toValueId(iri("<C2>"))},
                             {toValueId(iri("<d>")), toValueId(iri("<D>"))},
                             {toValueId(iri("<e>")), toValueId(iri("<E>"))}});

  verifyLazyAndMaterializedMatch(optJoin, expected);

  // All blocks should be read.
  checkPrefilteringStats(optJoin, 8, 8);
}

// _____________________________________________________________________________
TEST_P(OptionalJoinWithIndexScan, twoColumnsBasicFiltering) {
  // Test with two join columns where UNDEF is only in the last column.
  // Create knowledge graph with two-column structure.
  std::string kg2 =
      "<s0> <p> <o1> .<s1> <p> <o1> . <s1> <p> <o2> . <s2> <p> <o3> .<s3> <p> "
      "<o3>. ";
  TestIndexConfig config{kg2};
  config.blocksizePermutations = 8_B;
  auto qec2 = getQec(std::move(config));

  // Left side: two columns with UNDEF in second column.
  IdTable leftTable{2, makeAllocator()};
  auto s1 = TripleComponent{TripleComponent::Iri::fromIriref("<s1>")}
                .toValueId(qec2->getIndex().getVocab(), encodedIriManager())
                .value();
  auto s3 = TripleComponent{TripleComponent::Iri::fromIriref("<s3>")}
                .toValueId(qec2->getIndex().getVocab(), encodedIriManager())
                .value();
  auto o1 = TripleComponent{TripleComponent::Iri::fromIriref("<o1>")}
                .toValueId(qec2->getIndex().getVocab(), encodedIriManager())
                .value();

  leftTable.push_back({s1, o1});  // matches 1 row
  leftTable.push_back({s3, U});   // matches 1 row.

  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec2, std::move(leftTable),
      std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
      false, std::vector<ColumnIndex>{0, 1});

  // Right side: IndexScan with two output columns.
  SparqlTripleSimple triple{TripleComponent{Variable{"?x"}}, iri("<p>"),
                            TripleComponent{Variable{"?y"}}};
  auto right =
      ad_utility::makeExecutionTree<IndexScan>(qec2, Permutation::PSO, triple);

  OptionalJoin optJoin{qec2, left, right};
  qec2->getQueryTreeCache().clearAll();

  bool requestLaziness = GetParam();
  auto result = optJoin.computeResultOnlyForTesting(requestLaziness);

  IdTable actual = materializeResult(optJoin, result, requestLaziness);

  // Result should have 2 rows (one for each left entry matched with <p>
  // predicate).
  EXPECT_EQ(actual.numRows(), 2);
  EXPECT_EQ(actual.numColumns(), 2);
  // There is a nonmatching block for `s1, o1`, but currently we only filter on
  // the first columns.
  checkPrefilteringStats(optJoin, 3, 5);
}

// _____________________________________________________________________________
TEST_P(OptionalJoinWithIndexScan, twoColumnsMultipleMatches) {
  // Test two-column optional join with multiple matches for one subject.
  std::string kg2 = "<s0> <p> 1. <s1> <p> 2 . <s1> <p> 3 . <s2> <p> 4 .";
  TestIndexConfig config{kg2};
  config.blocksizePermutations = 8_B;
  auto qec2 = getQec(std::move(config));

  auto s1 = TripleComponent{TripleComponent::Iri::fromIriref("<s1>")}
                .toValueId(qec2->getIndex().getVocab(), encodedIriManager())
                .value();
  auto s2 = TripleComponent{TripleComponent::Iri::fromIriref("<s2>")}
                .toValueId(qec2->getIndex().getVocab(), encodedIriManager())
                .value();
  auto o1 = Id::makeFromInt(2);
  auto o3 = Id::makeFromInt(4);

  // Left side with UNDEF in second column.
  IdTable leftTable{2, makeAllocator()};
  leftTable.push_back({s1, U});
  leftTable.push_back({s1, o1});  // matches
  leftTable.push_back({s1, o3});  // doesn't match, but is added as undefined.
  leftTable.push_back({s2, U});

  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec2, std::move(leftTable),
      std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
      false, std::vector<ColumnIndex>{0, 1});

  SparqlTripleSimple triple{TripleComponent{Variable{"?x"}}, iri("<p>"),
                            TripleComponent{Variable{"?y"}}};
  auto right =
      ad_utility::makeExecutionTree<IndexScan>(qec2, Permutation::PSO, triple);

  OptionalJoin optJoin{qec2, left, right};
  qec2->getQueryTreeCache().clearAll();

  bool requestLaziness = GetParam();
  auto result = optJoin.computeResultOnlyForTesting(requestLaziness);

  IdTable actual = materializeResult(optJoin, result, requestLaziness);

  // Should have 5 rows total (s1,U) with 2 matches, (s1,o1) with 1 match,, (s1,
  // o2) with zero matches, but optional;  s2 with 1 match).
  EXPECT_EQ(actual.numRows(), 5) << actual;
  EXPECT_EQ(actual.numColumns(), 2);
  checkPrefilteringStats(optJoin, 3, 4);
}

INSTANTIATE_TEST_SUITE_P(OptionalJoinWithIndexScanSuite,
                         OptionalJoinWithIndexScan, ::testing::Bool());
