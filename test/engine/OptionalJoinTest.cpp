//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/AllocatorTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "../util/IdTableHelpers.h"
#include "../util/IdTestHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "../util/OperationTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/CallFixedSize.h"
#include "engine/JoinHelpers.h"
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

// Helper function to test lazy join implementations.
void testLazyOptionalJoin(std::vector<IdTable> leftTables,
                          std::vector<IdTable> rightTables,
                          const std::vector<IdTable>& expectedResult,
                          bool singleVar = false,
                          ad_utility::source_location location =
                              ad_utility::source_location::current()) {
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

    auto& lazyResult = result.idTables();
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

    auto& lazyResult = result.idTables();
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
      qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(1)));
  expected.push_back(createIdTableOfSizeWithValue(
      qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(2)));

  std::vector<IdTable> leftTables;
  leftTables.push_back(
      makeIdTableFromVector({{Id::makeFromInt(1)}, {Id::makeFromInt(2)}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(createIdTableOfSizeWithValue(
      qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(1)));
  rightTables.push_back(createIdTableOfSizeWithValue(
      qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(2)));

  testLazyOptionalJoin(std::move(leftTables), std::move(rightTables),
                       std::move(expected), true);
}
