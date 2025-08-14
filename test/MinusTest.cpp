// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@netpun.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "./util/IdTestHelpers.h"
#include "engine/CallFixedSize.h"
#include "engine/IndexScan.h"
#include "engine/JoinHelpers.h"
#include "engine/Minus.h"
#include "engine/MinusRowHandler.h"
#include "engine/ValuesForTesting.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

namespace {
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

// _____________________________________________________________________________
TEST(Minus, computeMinus) {
  IdTable a = makeIdTableFromVector(
      {{1, 2, 1}, {2, 1, 4}, {5, 4, 1}, {8, 1, 2}, {8, 2, 3}});

  IdTable b = makeIdTableFromVector({{1, 2, 7, 5}, {3, 3, 1, 5}, {1, 8, 1, 5}});

  std::vector<std::array<ColumnIndex, 2>> jcls;
  jcls.push_back(std::array<ColumnIndex, 2>{{0, 1}});
  jcls.push_back(std::array<ColumnIndex, 2>{{1, 0}});

  // Subtract b from a on the column pairs 1,2 and 2,1 (entries from columns 1
  // of a have to equal those of column 2 of b and vice versa).
  auto* qec = ad_utility::testing::getQec();
  Minus m{qec,
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, a.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?a"}, Variable{"?b"}, std::nullopt}),
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, b.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?b"}, Variable{"?a"}, std::nullopt, std::nullopt})};
  IdTable res = m.computeMinus(a, b, jcls);

  EXPECT_EQ(res, makeIdTableFromVector({{1, 2, 1}, {5, 4, 1}, {8, 2, 3}}));

  // Test subtracting without matching columns
  res.clear();
  jcls.clear();
  res = m.computeMinus(a, b, jcls);
  EXPECT_EQ(res, a);

  // Test minus with variable sized data.
  IdTable va = makeIdTableFromVector(
      {{1, 2, 3, 4, 5, 6}, {1, 2, 3, 7, 5, 6}, {7, 6, 5, 4, 3, 2}});

  IdTable vb = makeIdTableFromVector({{2, 3, 4}, {2, 3, 5}, {6, 7, 4}});

  jcls.clear();
  jcls.push_back({1, 0});
  jcls.push_back({2, 1});

  Minus vm{qec,
           ad_utility::makeExecutionTree<ValuesForTesting>(
               qec, va.clone(),
               std::vector<std::optional<Variable>>{
                   std::nullopt, Variable{"?a"}, Variable{"?b"}, std::nullopt,
                   std::nullopt, std::nullopt}),
           ad_utility::makeExecutionTree<ValuesForTesting>(
               qec, vb.clone(),
               std::vector<std::optional<Variable>>{
                   Variable{"?a"}, Variable{"?b"}, std::nullopt})};

  IdTable vres = vm.computeMinus(va, vb, jcls);

  EXPECT_EQ(vres, makeIdTableFromVector({{7, 6, 5, 4, 3, 2}}));
}

// _____________________________________________________________________________
TEST(Minus, ensureLocalVocabFromLeftIsPassed) {
  IdTable a = makeIdTableFromVector({{0}, {1}, {2}, {3}, {4}});
  IdTable b = makeIdTableFromVector({{0}});
  LocalVocabEntry aEntry = LocalVocabEntry::fromStringRepresentation("\"a\"");
  LocalVocab vocabA;
  vocabA.getIndexAndAddIfNotContained(aEntry);
  LocalVocab vocabB;
  vocabB.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromStringRepresentation("\"b\""));

  auto* qec = ad_utility::testing::getQec();
  Minus m{qec,
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, std::move(a),
              std::vector<std::optional<Variable>>{Variable{"?a"}}, false,
              std::vector<ColumnIndex>{0}, std::move(vocabA)),
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, std::move(b),
              std::vector<std::optional<Variable>>{Variable{"?a"}}, false,
              std::vector<ColumnIndex>{0}, std::move(vocabB))};

  auto result = m.computeResultOnlyForTesting(false);
  EXPECT_THAT(result.localVocab().getAllWordsForTesting(),
              ::testing::ElementsAre(::testing::Eq(aEntry)));
}

// _____________________________________________________________________________
TEST(Minus, computeMinusIndexNestedLoopJoinOptimization) {
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
                                     {10, 11, 12, 13},
                                     {14, 15, 16, 17}});
  IdTable expected = makeIdTableFromVector({{4, 2, 1}, {2, 8, 1}});

  auto* qec = ad_utility::testing::getQec();
  for (bool forceFullyMaterialized : {false, true}) {
    Minus m{qec,
            ad_utility::makeExecutionTree<ValuesForTesting>(
                qec, a.clone(),
                std::vector<std::optional<Variable>>{
                    std::nullopt, Variable{"?a"}, Variable{"?b"}},
                false, std::vector<ColumnIndex>{1, 2}, leftVocab.clone()),
            ad_utility::makeExecutionTree<ValuesForTesting>(
                qec, b.clone(),
                std::vector<std::optional<Variable>>{
                    std::nullopt, Variable{"?b"}, Variable{"?a"}, std::nullopt},
                false, std::vector<ColumnIndex>{}, rightVocab.clone(),
                std::nullopt, forceFullyMaterialized)};
    auto result = m.computeResultOnlyForTesting(true);
    ASSERT_TRUE(result.isFullyMaterialized());
    EXPECT_EQ(result.idTable(), expected);
    EXPECT_THAT(result.localVocab().getAllWordsForTesting(),
                ::testing::UnorderedElementsAre(entryA));
    const auto& runtimeInfo =
        m.getChildren().at(1)->getRootOperation()->runtimeInfo();
    EXPECT_EQ(runtimeInfo.status_, RuntimeInformation::Status::optimizedOut);
    EXPECT_EQ(runtimeInfo.numRows_, 0);
  }
}

// _____________________________________________________________________________
TEST(Minus, computeMinusWithEmptyTables) {
  IdTable nonEmpty = makeIdTableFromVector({{1, 2}, {3, 3}, {1, 8}});
  IdTable empty = IdTable{2, nonEmpty.getAllocator()};

  std::vector<std::array<ColumnIndex, 2>> jcls;
  jcls.push_back(std::array<ColumnIndex, 2>{{0, 0}});

  auto* qec = ad_utility::testing::getQec();
  Minus m{
      qec,
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, empty.clone(),
          std::vector<std::optional<Variable>>{Variable{"?a"}, std::nullopt}),
      ad_utility::makeExecutionTree<ValuesForTesting>(
          qec, nonEmpty.clone(),
          std::vector<std::optional<Variable>>{Variable{"?a"}, std::nullopt})};

  {
    IdTable res = m.computeMinus(empty, nonEmpty, jcls);

    EXPECT_EQ(res, empty);
  }
  {
    IdTable res = m.computeMinus(nonEmpty, empty, jcls);

    EXPECT_EQ(res, nonEmpty);
  }
}

// _____________________________________________________________________________
TEST(Minus, computeMinusWithUndefined) {
  auto U = Id::makeUndefined();
  IdTable a =
      makeIdTableFromVector({{U, U, 10}, {U, 1, 11}, {1, U, 12}, {5, 4, 13}});
  IdTable b = makeIdTableFromVector({{U, U, 20}, {3, U, 21}, {1, 2, 22}});

  std::vector<std::array<ColumnIndex, 2>> jcls;
  jcls.push_back(std::array<ColumnIndex, 2>{{0, 1}});
  jcls.push_back(std::array<ColumnIndex, 2>{{1, 0}});

  auto* qec = ad_utility::testing::getQec();
  Minus m{qec,
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, a.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?a"}, Variable{"?b"}, std::nullopt}),
          ad_utility::makeExecutionTree<ValuesForTesting>(
              qec, b.clone(),
              std::vector<std::optional<Variable>>{
                  Variable{"?b"}, Variable{"?a"}, std::nullopt})};

  IdTable res = m.computeMinus(a, b, jcls);
  EXPECT_EQ(res, makeIdTableFromVector({{U, U, 10}, {1, U, 12}, {5, 4, 13}}));
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
TEST(Minus, columnOriginatesFromGraphOrUndef) {
  using ad_utility::triple_component::Iri;
  auto* qec = ad_utility::testing::getQec();
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"}});
  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?c"}});
  auto index = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::POS,
      SparqlTripleSimple{Variable{"?a"}, Iri::fromIriref("<b>"),
                         Iri::fromIriref("<c>")});

  Minus minus1{qec, values1, values1};
  EXPECT_FALSE(minus1.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(minus1.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_THROW(
      minus1.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  Minus minus2{qec, values1, values2};
  EXPECT_FALSE(minus2.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(minus2.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_THROW(minus2.columnOriginatesFromGraphOrUndef(Variable{"?c"}),
               ad_utility::Exception);
  EXPECT_THROW(
      minus2.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  Minus minus3{qec, index, values1};
  EXPECT_TRUE(minus3.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_THROW(minus3.columnOriginatesFromGraphOrUndef(Variable{"?b"}),
               ad_utility::Exception);
  EXPECT_THROW(
      minus3.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);

  Minus minus4{qec, values1, index};
  EXPECT_FALSE(minus4.columnOriginatesFromGraphOrUndef(Variable{"?a"}));
  EXPECT_FALSE(minus4.columnOriginatesFromGraphOrUndef(Variable{"?b"}));
  EXPECT_THROW(
      minus4.columnOriginatesFromGraphOrUndef(Variable{"?notExisting"}),
      ad_utility::Exception);
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
        qec, makeIdTableFromVector({{V(2), V(22)}}),
        std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?z"}},
        false, std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);
    Minus minus{qec, left, right};

    qec->getQueryTreeCache().clearAll();

    auto result = minus.computeResultOnlyForTesting(true);

    ASSERT_FALSE(result.isFullyMaterialized());

    auto lazyResult = result.idTables();
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

  auto lazyResult = result.idTables();
  auto it = lazyResult.begin();
  ASSERT_NE(it, lazyResult.end());

  EXPECT_EQ(it->idTable_, expected);

  EXPECT_EQ(++it, lazyResult.end());
}

// _____________________________________________________________________________
TEST(Minus, lazyMinusKeepsLeftLocalVocab) {
  auto qec = ad_utility::testing::getQec();

  LocalVocabEntry testLiteral{
      ad_utility::triple_component::Literal::fromStringRepresentation(
          "\"Abc\"")};

  LocalVocab leftVocab{};
  leftVocab.getIndexAndAddIfNotContained(testLiteral);
  LocalVocab rightVocab{};
  rightVocab.getIndexAndAddIfNotContained(LocalVocabEntry{
      ad_utility::triple_component::Literal::fromStringRepresentation(
          "\"Def\"")});

  auto expected = makeIdTableFromVector({{1, 11, 111}, {3, 33, 333}});

  auto left = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 11, 111}, {2, 22, 222}, {3, 33, 333}}),
      std::vector<std::optional<Variable>>{Variable{"?x"}, Variable{"?y"},
                                           Variable{"?z"}},
      false, std::vector<ColumnIndex>{2}, leftVocab.clone());
  auto right = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{2222, 222}}),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?z"}},
      false, std::vector<ColumnIndex>{1}, rightVocab.clone());
  Minus minus{qec, left, right};

  qec->getQueryTreeCache().clearAll();

  auto result = minus.computeResultOnlyForTesting(true);

  ASSERT_FALSE(result.isFullyMaterialized());

  auto lazyResult = result.idTables();
  auto it = lazyResult.begin();
  ASSERT_NE(it, lazyResult.end());

  EXPECT_EQ(it->idTable_, expected);
  ASSERT_EQ(it->localVocab_.size(), 1);
  EXPECT_THAT(it->localVocab_.getAllWordsForTesting(),
              ::testing::ElementsAre(testLiteral));

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

// _____________________________________________________________________________
struct Wrapper {
  IdTableView<0> table_;
  const LocalVocab& localVocab_;
  operator IdTableView<0>() const { return table_; }
  const LocalVocab& getLocalVocab() const { return localVocab_; }
};

// _____________________________________________________________________________
TEST(Minus, MinusRowHandlerKeepsLeftLocalVocabAfterFlush) {
  auto qec = ad_utility::testing::getQec();

  LocalVocabEntry testLiteral{
      ad_utility::triple_component::Literal::fromStringRepresentation(
          "\"Abc\"")};

  LocalVocab leftVocab{};
  leftVocab.getIndexAndAddIfNotContained(testLiteral);

  std::vector<IdTable> resultTables;

  ad_utility::MinusRowHandler handler{
      1, IdTable{1, qec->getAllocator()},
      std::make_shared<ad_utility::CancellationHandle<>>(),
      [&resultTables](IdTable& table, LocalVocab&) {
        resultTables.push_back(std::move(table));
      }};

  auto input = makeIdTableFromVector({{1}});

  Wrapper wrapper{input.asStaticView<0>(), leftVocab};

  handler.setOnlyLeftInputForOptionalJoin(wrapper);
  handler.addOptionalRow(0);

  handler.flush();

  EXPECT_THAT(resultTables,
              ::testing::ElementsAre(::testing::Eq(std::cref(input))));
  EXPECT_EQ(handler.localVocab().size(), 1);
  EXPECT_THAT(handler.localVocab().getAllWordsForTesting(),
              ::testing::ElementsAre(testLiteral));
  EXPECT_TRUE(std::move(handler).resultTable().empty());
}
