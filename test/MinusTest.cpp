// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/JoinHelpers.h"
#include "engine/Minus.h"
#include "engine/ValuesForTesting.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

namespace {
auto table(size_t cols) {
  return IdTable(cols, ad_utility::testing::makeAllocator());
}
auto V = ad_utility::testing::VocabId;
constexpr auto U = Id::makeUndefined();

// Helper function to test minus implementations.
void testMinus(std::vector<IdTable> leftTables,
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
  Minus minus{qec, left, right};

  {
    qec->getQueryTreeCache().clearAll();

    auto result = minus.computeResultOnlyForTesting(true);

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

    auto result = minus.computeResultOnlyForTesting(false);

    ASSERT_TRUE(result.isFullyMaterialized());

    IdTable expected{minus.getResultWidth(), qec->getAllocator()};

    for (const IdTable& idTable : expectedResult) {
      ASSERT_EQ(idTable.numColumns(), minus.getResultWidth());
      expected.insertAtEnd(idTable);
    }

    EXPECT_EQ(result.idTable(), expected);
  }
}
}  // namespace

TEST(Minus, computeMinus) {
  using std::array;
  using std::vector;

  IdTable a = table(3);
  a.push_back({V(1), V(2), V(1)});
  a.push_back({V(2), V(1), V(4)});
  a.push_back({V(5), V(4), V(1)});
  a.push_back({V(8), V(1), V(2)});
  a.push_back({V(8), V(2), V(3)});

  IdTable b = table(4);
  b.push_back({V(1), V(2), V(7), V(5)});
  b.push_back({V(3), V(3), V(1), V(5)});
  b.push_back({V(1), V(8), V(1), V(5)});

  IdTable res = table(3);

  vector<array<ColumnIndex, 2>> jcls;
  jcls.push_back(array<ColumnIndex, 2>{{0, 1}});
  jcls.push_back(array<ColumnIndex, 2>{{1, 0}});

  IdTable wantedRes = table(3);
  wantedRes.push_back({V(1), V(2), V(1)});
  wantedRes.push_back({V(5), V(4), V(1)});
  wantedRes.push_back({V(8), V(2), V(3)});

  // Subtract b from a on the column pairs 1,2 and 2,1 (entries from columns 1
  // of a have to equal those of column 2 of b and vice versa).
  int aWidth = a.numColumns();
  int bWidth = b.numColumns();
  Minus m{Minus::OnlyForTestingTag{}};
  CALL_FIXED_SIZE((std::array{aWidth, bWidth}), &Minus::computeMinus, m, a, b,
                  jcls, &res);

  ASSERT_EQ(wantedRes.size(), res.size());

  ASSERT_EQ(wantedRes[0], res[0]);
  ASSERT_EQ(wantedRes[1], res[1]);
  ASSERT_EQ(wantedRes[2], res[2]);

  // Test subtracting without matching columns
  res.clear();
  jcls.clear();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth}), &Minus::computeMinus, m, a, b,
                  jcls, &res);
  ASSERT_EQ(a.size(), res.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], res[i]);
  }

  // Test minus with variable sized data.
  IdTable va = table(6);
  va.push_back({V(1), V(2), V(3), V(4), V(5), V(6)});
  va.push_back({V(1), V(2), V(3), V(7), V(5), V(6)});
  va.push_back({V(7), V(6), V(5), V(4), V(3), V(2)});

  IdTable vb = table(3);
  vb.push_back({V(2), V(3), V(4)});
  vb.push_back({V(2), V(3), V(5)});
  vb.push_back({V(6), V(7), V(4)});

  IdTable vres = table(6);
  jcls.clear();
  jcls.push_back({1, 0});
  jcls.push_back({2, 1});

  // The template size parameter can be at most 6 (the maximum number
  // of fixed size columns plus one).
  aWidth = va.numColumns();
  bWidth = vb.numColumns();
  CALL_FIXED_SIZE((std::array{aWidth, bWidth}), &Minus::computeMinus, m, va, vb,
                  jcls, &vres);

  wantedRes = table(6);
  wantedRes.push_back({V(7), V(6), V(5), V(4), V(3), V(2)});
  ASSERT_EQ(wantedRes.size(), vres.size());
  ASSERT_EQ(wantedRes.numColumns(), vres.numColumns());

  ASSERT_EQ(wantedRes[0], vres[0]);
}

// _____________________________________________________________________________
TEST(Minus, clone) {
  auto* qec = ad_utility::testing::getQec();
  Minus minus{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}}),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, makeIdTableFromVector({{0, 1}}),
          std::vector<std::optional<Variable>>{Variable{"?x"},
                                               Variable{"?z"}})};

  auto clone = minus.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(minus, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), minus.getDescriptor());
}

// _____________________________________________________________________________
TEST(Minus, lazyMinus) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector(
      {{U, V(10)}, {V(1), V(11)}, {V(4), V(14)}, {V(5), V(15)}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
  leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));
  leftTables.push_back(makeIdTableFromVector({{4, 14}, {5, 15}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{V(2), V(22)}}));
  rightTables.push_back(makeIdTableFromVector({{3, 23}}));

  testMinus(std::move(leftTables), std::move(rightTables), std::move(expected));
}

// _____________________________________________________________________________
TEST(Minus, repeatingMatchesDontProduceDuplicates) {
  std::vector<IdTable> expected;
  expected.push_back(
      makeIdTableFromVector({{0, 10}, {2, 13}, {2, 14}, {2, 15}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{0, 10}, {1, 11}}));
  leftTables.push_back(makeIdTableFromVector({{1, 110}}));
  leftTables.push_back(makeIdTableFromVector({{1, 111}}));
  leftTables.push_back(makeIdTableFromVector({{1, 12}, {2, 13}}));
  leftTables.push_back(makeIdTableFromVector({{2, 14}, {2, 15}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{1, 21}}));
  rightTables.push_back(makeIdTableFromVector({{1, 22}}));
  rightTables.push_back(makeIdTableFromVector({{3, 23}}));

  testMinus(std::move(leftTables), std::move(rightTables), std::move(expected));
}

// _____________________________________________________________________________
TEST(Minus, lazyMinusWithUndefRight) {
  std::vector<IdTable> expected;
  expected.push_back(
      makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}, {3, 13}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}}));
  leftTables.push_back(makeIdTableFromVector({{2, 12}, {3, 13}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{U, V(20)}, {V(2), V(22)}}));

  testMinus(std::move(leftTables), std::move(rightTables), std::move(expected));
}

// _____________________________________________________________________________
TEST(Minus, lazyMinusWithUndefLeft) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{U, V(10)}, {2, 12}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector(
      {{U, V(10)}, {V(1), V(11)}, {V(2), V(12)}, {V(3), V(13)}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(
      makeIdTableFromVector({{V(1), V(101)}, {V(3), V(303)}}));

  testMinus(std::move(leftTables), std::move(rightTables), std::move(expected));
}

// _____________________________________________________________________________
TEST(Minus, lazyMinusWithUndefLeftInSeparateTable) {
  std::vector<IdTable> expected;
  expected.push_back(makeIdTableFromVector({{U, V(10)}, {2, 12}}));

  std::vector<IdTable> leftTables;
  leftTables.push_back(makeIdTableFromVector({{U, V(10)}}));
  leftTables.push_back(makeIdTableFromVector({{1, 11}, {2, 12}, {3, 13}}));
  std::vector<IdTable> rightTables;
  rightTables.push_back(makeIdTableFromVector({{1, 101}, {3, 303}}));

  testMinus(std::move(leftTables), std::move(rightTables), std::move(expected));
}

// _____________________________________________________________________________
TEST(Minus, lazyMinusWithOneMaterializedTable) {
  auto qec = ad_utility::testing::getQec();

  auto expected = makeIdTableFromVector({{U, V(10)}, {1, 11}, {3, 13}});

  {
    std::vector<IdTable> rightTables;
    rightTables.push_back(makeIdTableFromVector({{2, 22}}));

    auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec,
        makeIdTableFromVector({{U, V(10)}, {V(1), V(11)}, {2, 12}, {3, 13}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"}},
        false, std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(rightTables),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::vector<ColumnIndex>{0});
    Minus minus{qec, left, right};

    qec->getQueryTreeCache().clearAll();

    auto result = minus.computeResultOnlyForTesting(true);

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
        qec, makeIdTableFromVector({{V(2), V(22)}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    Minus minus{qec, left, right};

    qec->getQueryTreeCache().clearAll();

    auto result = minus.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    auto& lazyResult = result.idTables();
    auto it = lazyResult.begin();
    ASSERT_NE(it, lazyResult.end());

    EXPECT_EQ(it->idTable_, expected);

    EXPECT_EQ(++it, lazyResult.end());
  }
}

// _____________________________________________________________________________
TEST(Minus, lazyMinusWithPermutedColumns) {
  auto qec = ad_utility::testing::getQec();

  auto expected = makeIdTableFromVector({{1, 11, 111}, {3, 33, 333}});

  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 11, 111}, {2, 22, 222}, {3, 33, 333}}),
      std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"},
                                           Variable{"?z"}},
      false, std::vector<ColumnIndex>{2});
  auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{2222, 222}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?z"}},
      false, std::vector<ColumnIndex>{1});
  Minus minus{qec, left, right};

  qec->getQueryTreeCache().clearAll();

  auto result = minus.computeResultOnlyForTesting(true);

  ASSERT_FALSE(result.isFullyMaterialized());

  auto& lazyResult = result.idTables();
  auto it = lazyResult.begin();
  ASSERT_NE(it, lazyResult.end());

  EXPECT_EQ(it->idTable_, expected);

  EXPECT_EQ(++it, lazyResult.end());
}

// _____________________________________________________________________________
TEST(Minus, lazyMinusExceedingChunkSize) {
  {
    std::vector<IdTable> expected;
    expected.push_back(makeIdTableFromVector({{Id::makeFromInt(3)}}));

    std::vector<IdTable> leftTables;
    leftTables.push_back(
        makeIdTableFromVector({{1}, {2}, {3}}, &Id::makeFromInt));
    std::vector<IdTable> rightTables;
    rightTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(1)));
    rightTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(2)));

    testMinus(std::move(leftTables), std::move(rightTables),
              std::move(expected), true);
  }
  {
    std::vector<IdTable> expected;
    expected.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(2)));

    std::vector<IdTable> leftTables;
    leftTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(1)));
    leftTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(2)));
    leftTables.push_back(createIdTableOfSizeWithValue(
        qlever::joinHelpers::CHUNK_SIZE + 1, Id::makeFromInt(3)));
    std::vector<IdTable> rightTables;
    rightTables.push_back(makeIdTableFromVector({{1}, {3}}, &Id::makeFromInt));

    testMinus(std::move(leftTables), std::move(rightTables),
              std::move(expected), true);
  }
}
