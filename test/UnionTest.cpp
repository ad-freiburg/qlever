// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <vector>

#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/IndexScan.h"
#include "engine/NeutralElementOperation.h"
#include "engine/Sort.h"
#include "engine/Union.h"
#include "engine/ValuesForTesting.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "global/Id.h"
#include "util/IndexTestHelpers.h"
#include "util/OperationTestHelpers.h"

namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;
}  // namespace

// A simple test for computing a union.
TEST(Union, computeUnion) {
  auto* qec = ad_utility::testing::getQec();
  IdTable left = makeIdTableFromVector({{V(1)}, {V(2)}, {V(3)}});
  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, left.clone(), Vars{Variable{"?x"}});

  IdTable right = makeIdTableFromVector({{V(4), V(5)}, {V(6), V(7)}});
  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, right.clone(), Vars{Variable{"?u"}, Variable{"?x"}});

  Union u{qec, leftT, rightT};
  auto resultTable = u.computeResultOnlyForTesting();
  const auto& result = resultTable.idTable();

  auto U = Id::makeUndefined();
  auto expected = makeIdTableFromVector(
      {{V(1), U}, {V(2), U}, {V(3), U}, {V(5), V(4)}, {V(7), V(6)}});
  ASSERT_EQ(result, expected);
}

// A test with large inputs to test the chunked writing that is caused by the
// timeout checks.
TEST(Union, computeUnionLarge) {
  auto* qec = ad_utility::testing::getQec();
  VectorTable leftInput, rightInput, expected;
  size_t numInputsL = 1'500'000u;
  size_t numInputsR = 5;
  size_t numInputs = numInputsL + numInputsR;
  auto U = Id::makeUndefined();
  leftInput.reserve(numInputsL);
  expected.reserve(numInputs);
  for (size_t i = 0; i < numInputsL; ++i) {
    leftInput.push_back(std::vector<IntOrId>{V(i)});
    expected.push_back(std::vector<IntOrId>{V(i), U});
  }
  for (size_t i = 0; i < numInputsR; ++i) {
    rightInput.push_back(std::vector<IntOrId>{V(i + 425)});
    expected.push_back(std::vector<IntOrId>{U, V(i + 425)});
  }
  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector(leftInput), Vars{Variable{"?x"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector(rightInput), Vars{Variable{"?u"}});

  Union u{qec, leftT, rightT};
  auto resultTable = u.computeResultOnlyForTesting();
  const auto& result = resultTable.idTable();

  ASSERT_EQ(result, makeIdTableFromVector(expected));
}

// _____________________________________________________________________________
TEST(Union, computeUnionLazy) {
  auto runTest = [](bool nonLazyChildren, bool invisibleSubtreeColumns,
                    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
    auto l = generateLocationTrace(loc);
    auto* qec = ad_utility::testing::getQec();
    qec->getQueryTreeCache().clearAll();
    auto leftT = [&]() {
      if (!invisibleSubtreeColumns) {
        IdTable left = makeIdTableFromVector({{V(1)}, {V(2)}, {V(3)}});
        return ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, std::move(left), Vars{Variable{"?x"}}, false,
            std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt,
            nonLazyChildren);
      } else {
        // With the `invisibleSubtreeColumns==true` case we test the case, that
        // the input contains variables that are not visible because of a
        // subquery. This case was previously buggy and triggered an assertion.
        IdTable left = makeIdTableFromVector(
            {{V(1), V(3)}, {V(2), V(27)}, {V(3), V(123)}});
        auto tree = ad_utility::makeExecutionTree<ValuesForTesting>(
            qec, std::move(left), Vars{Variable{"?x"}, Variable{"?invisible"}},
            false, std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt,
            nonLazyChildren);
        tree->getRootOperation()->setSelectedVariablesForSubquery(
            {Variable{"?x"}});
        return tree;
      }
    }();

    IdTable right = makeIdTableFromVector({{V(4), V(5)}, {V(6), V(7)}});
    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(right), Vars{Variable{"?u"}, Variable{"?x"}}, false,
        std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt,
        nonLazyChildren);

    Union u{qec, std::move(leftT), std::move(rightT)};
    auto resultTable = u.computeResultOnlyForTesting(true);
    ASSERT_FALSE(resultTable.isFullyMaterialized());
    auto result = resultTable.idTables();

    auto U = Id::makeUndefined();
    auto expected1 = makeIdTableFromVector({{V(1), U}, {V(2), U}, {V(3), U}});
    auto expected2 = makeIdTableFromVector({{V(5), V(4)}, {V(7), V(6)}});

    auto iterator = result.begin();
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected2);

    ASSERT_EQ(++iterator, result.end());
  };

  runTest(false, false);
  runTest(false, true);
  runTest(true, false);
  runTest(true, true);
}

// _____________________________________________________________________________
TEST(Union, ensurePermutationIsAppliedCorrectly) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2, 3, 4, 5}}),
      Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}, Var{"?d"}, Var{"?e"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{6, 7, 8}}),
      Vars{Var{"?b"}, Var{"?a"}, Var{"?e"}});

  Union u{qec, std::move(leftT), std::move(rightT)};

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting(true);
    ASSERT_FALSE(resultTable.isFullyMaterialized());
    auto result = resultTable.idTables();

    auto U = Id::makeUndefined();
    auto expected1 = makeIdTableFromVector({{1, 2, 3, 4, 5}});
    auto expected2 = makeIdTableFromVector({{V(7), V(6), U, U, V(8)}});

    auto iterator = result.begin();
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected2);

    ASSERT_EQ(++iterator, result.end());
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting();
    ASSERT_TRUE(resultTable.isFullyMaterialized());

    auto U = Id::makeUndefined();
    auto expected =
        makeIdTableFromVector({{1, 2, 3, 4, 5}, {V(7), V(6), U, U, V(8)}});
    EXPECT_EQ(resultTable.idTable(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, inputWithZeroColumns) {
  auto* qec = ad_utility::testing::getQec();
  auto leftT = ad_utility::makeExecutionTree<NeutralElementOperation>(qec);
  auto rightT = ad_utility::makeExecutionTree<NeutralElementOperation>(qec);

  Union u{qec, std::move(leftT), std::move(rightT)};

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting(true);
    ASSERT_FALSE(resultTable.isFullyMaterialized());
    auto result = resultTable.idTables();

    auto expected1 = makeIdTableFromVector({{}});

    auto iterator = result.begin();
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ASSERT_EQ(++iterator, result.end());
  }

  {
    qec->getQueryTreeCache().clearAll();
    auto resultTable = u.computeResultOnlyForTesting();
    ASSERT_TRUE(resultTable.isFullyMaterialized());

    auto expected = makeIdTableFromVector({{}, {}});
    EXPECT_EQ(resultTable.idTable(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, clone) {
  auto* qec = ad_utility::testing::getQec();

  Union unionOperation{
      qec, ad_utility::makeExecutionTree<NeutralElementOperation>(qec),
      ad_utility::makeExecutionTree<NeutralElementOperation>(qec)};

  auto clone = unionOperation.clone();
  ASSERT_TRUE(clone);
  EXPECT_THAT(unionOperation, IsDeepCopy(*clone));
  EXPECT_EQ(clone->getDescriptor(), unionOperation.getDescriptor());
}

// _____________________________________________________________________________
TEST(Union, cheapMergeIfOrderNotImportant) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2}}), Vars{Var{"?a"}, Var{"?b"}}, false,
      std::vector<ColumnIndex>{0, 1});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 0}, {2, 4}}), Vars{Var{"?a"}, Var{"?b"}},
      false, std::vector<ColumnIndex>{0, 1});
  Union unionOperation{qec, std::move(leftT), std::move(rightT), {}};
  EXPECT_TRUE(unionOperation.resultSortedOn().empty());
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    EXPECT_FALSE(result->isFullyMaterialized());
    auto idTables = result->idTables();
    auto expected1 = makeIdTableFromVector({{1, 2}});
    auto expected2 = makeIdTableFromVector({{0, 0}, {2, 4}});

    auto iterator = idTables.begin();
    ASSERT_NE(iterator, idTables.end());
    ASSERT_EQ(iterator->idTable_, expected1);

    ++iterator;
    ASSERT_NE(iterator, idTables.end());
    ASSERT_EQ(iterator->idTable_, expected2);

    ASSERT_EQ(++iterator, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Union, sortedMerge) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2, 4}}),
      Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}}, false,
      std::vector<ColumnIndex>{0, 1, 2});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{4, 1}, {8, 2}}), Vars{Var{"?c"}, Var{"?a"}},
      false, std::vector<ColumnIndex>{1, 0});
  Union unionOperation{qec, std::move(leftT), std::move(rightT), {0, 1, 2}};
  EXPECT_EQ(unionOperation.resultSortedOn(),
            (std::vector<ColumnIndex>{0, 1, 2}));
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, U, 4}, {1, 2, 4}, {2, U, 8}});
    EXPECT_EQ(result->idTable(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    auto expected = makeIdTableFromVector({{1, U, 4}, {1, 2, 4}, {2, U, 8}});
    auto idTables = result->idTables();
    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, expected);

    ASSERT_EQ(++it, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Union, sortedMergeWithOneSideNonLazy) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{2}}), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0}, LocalVocab{}, std::nullopt, true);

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}, {1}}), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0});
  Union unionOperation{qec, std::move(leftT), std::move(rightT), {0}};
  auto expected = makeIdTableFromVector({{0}, {1}, {2}});
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    EXPECT_EQ(result->idTable(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    auto idTables = result->idTables();
    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{0}, {1}}));

    ++it;
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{2}}));

    ASSERT_EQ(++it, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Union, sortedMergeWithLocalVocab) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  LocalVocab vocab1;
  vocab1.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromStringRepresentation("\"Test1\""));

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}, {2}, {4}}), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0}, vocab1.clone());

  LocalVocab vocab2;
  vocab2.getIndexAndAddIfNotContained(
      LocalVocabEntry::fromStringRepresentation("\"Test2\""));
  std::vector<IdTable> tables;
  tables.push_back(makeIdTableFromVector({{0}}));
  tables.push_back(makeIdTableFromVector({{3}}));
  tables.push_back(makeIdTableFromVector({{5}}));

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(tables), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0}, vocab2.clone());
  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, leftT, rightT, {0}};
    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{0}, {1}, {2}, {3}, {4}, {5}});
    EXPECT_EQ(result->idTable(), expected);
    EXPECT_THAT(result->localVocab().getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab1.getAllWordsForTesting()));
    EXPECT_THAT(result->localVocab().getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab2.getAllWordsForTesting()));
  }
  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, std::move(leftT), std::move(rightT), {0}};
    auto result =
        unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
    auto idTables = result->idTables();

    auto it = idTables.begin();
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{0}, {1}, {2}, {3}, {4}}));
    EXPECT_THAT(it->localVocab_.getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab1.getAllWordsForTesting()));
    EXPECT_THAT(it->localVocab_.getAllWordsForTesting(),
                ::testing::IsSupersetOf(vocab2.getAllWordsForTesting()));

    ++it;
    ASSERT_NE(it, idTables.end());
    EXPECT_EQ(it->idTable_, makeIdTableFromVector({{5}}));
    EXPECT_EQ(it->localVocab_.getAllWordsForTesting(),
              vocab2.getAllWordsForTesting());

    ASSERT_EQ(++it, idTables.end());
  }
}

// _____________________________________________________________________________
TEST(Union, cacheKeyDiffersForDifferentOrdering) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 4}}), Vars{Var{"?a"}, Var{"?b"}}, false,
      std::vector<ColumnIndex>{0, 1});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 8}}), Vars{Var{"?a"}, Var{"?c"}}, false,
      std::vector<ColumnIndex>{0, 1});
  Union unionOperation1{qec, leftT, rightT, {0, 1, 2}};
  Union unionOperation2{qec, std::move(leftT), std::move(rightT), {0, 2, 1}};

  EXPECT_NE(unionOperation1.getCacheKey(), unionOperation2.getCacheKey());
  EXPECT_EQ(unionOperation1.getChildren().at(0)->getCacheKey(),
            unionOperation2.getChildren().at(0)->getCacheKey());
  EXPECT_EQ(unionOperation1.getChildren().at(1)->getCacheKey(),
            unionOperation2.getChildren().at(1)->getCacheKey());

  qec->getQueryTreeCache().clearAll();
  {
    auto result =
        unionOperation1.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, U, 8}, {1, 4, U}});
    EXPECT_EQ(result->idTable(), expected);
  }
  {
    auto result =
        unionOperation2.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, 4, U}, {1, U, 8}});
    EXPECT_EQ(result->idTable(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, cacheKeyPreventsAmbiguity) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  // Construct the following two operations (for the check that follows):
  //
  // { VALUES ?a { 1 } INTERNAL SORT BY ?a } UNION { VALUES ?a { 1 } }
  //
  // { VALUES ?a { 1 } } UNION { VALUES ?a { 1 } } INTERNAL SORT BY ?a
  //
  auto values1 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}});

  auto values2 = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}});
  auto sort = ad_utility::makeExecutionTree<Sort>(qec, values1->clone(),
                                                  std::vector<ColumnIndex>{0});
  Union operation1{qec, std::move(sort), values2};
  Sort operation2{qec,
                  ad_utility::makeExecutionTree<Union>(qec, std::move(values1),
                                                       std::move(values2)),
                  std::vector<ColumnIndex>{0}};

  // Check that the two cache keys are different (which was not the case before
  // #1933).
  EXPECT_NE(operation1.getCacheKey(), operation2.getCacheKey());
}

// _____________________________________________________________________________
TEST(Union, cacheKeyStoresColumnMapping) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  {
    auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1, 4}}), Vars{Var{"?a"}, Var{"?b"}});

    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{2, 8}}), Vars{Var{"?a"}, Var{"?c"}});

    auto rightTHidden = rightT->clone();
    rightTHidden->getRootOperation()->setSelectedVariablesForSubquery(
        {Variable{"?a"}});

    Union unionOperation1{qec, leftT, std::move(rightT)};
    Union unionOperation2{qec, std::move(leftT), std::move(rightTHidden)};

    EXPECT_NE(unionOperation1.getCacheKey(), unionOperation2.getCacheKey());
  }

  {
    auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{1, 4}}), Vars{Var{"?a"}, Var{"?b"}});

    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{2, 8}}), Vars{Var{"?a"}, Var{"?b"}});

    auto rightTSwapped = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{2, 8}}), Vars{Var{"?b"}, Var{"?a"}});

    Union unionOperation1{qec, leftT, std::move(rightT)};
    Union unionOperation2{qec, std::move(leftT), std::move(rightTSwapped)};

    EXPECT_NE(unionOperation1.getCacheKey(), unionOperation2.getCacheKey());
  }
}

// _____________________________________________________________________________
// We use a trick to merge two children where the first sort column is not
// present in both children. This test checks that the trick works correctly.
TEST(Union, testEfficientMerge) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{2}}), Vars{Var{"?b"}}, false,
      std::vector<ColumnIndex>{0});

  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, leftT, rightT, {0, 1}};
    // Check if children were swapped
    EXPECT_EQ(unionOperation.getChildren().at(0)->getCacheKey(),
              rightT->getCacheKey());
    EXPECT_EQ(unionOperation.getChildren().at(1)->getCacheKey(),
              leftT->getCacheKey());

    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{U, 2}, {1, U}});
    EXPECT_EQ(result->idTable(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    Union unionOperation{qec, leftT, rightT, {1, 0}};
    // Ensure children were not swapped
    EXPECT_EQ(unionOperation.getChildren().at(0)->getCacheKey(),
              leftT->getCacheKey());
    EXPECT_EQ(unionOperation.getChildren().at(1)->getCacheKey(),
              rightT->getCacheKey());

    auto result =
        unionOperation.getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected = makeIdTableFromVector({{1, U}, {U, 2}});
    EXPECT_EQ(result->idTable(), expected);
  }
}

// _____________________________________________________________________________
TEST(Union, createSortedVariantWorksProperly) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  auto U = Id::makeUndefined();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 2, 4}}),
      Vars{Var{"?a"}, Var{"?b"}, Var{"?c"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1, 4}, {2, 8}}), Vars{Var{"?a"}, Var{"?d"}});
  Union unionOperation{qec, std::move(leftT), std::move(rightT), {}};
  EXPECT_TRUE(unionOperation.resultSortedOn().empty());

  {
    qec->getQueryTreeCache().clearAll();
    auto tree = unionOperation.makeSortedTree({0, 1, 2, 3});
    ASSERT_TRUE(tree.has_value());
    auto variant = tree.value()->getRootOperation();
    EXPECT_EQ(variant->getResultSortedOn(),
              (std::vector<ColumnIndex>{0, 1, 2, 3}));
    EXPECT_EQ(
        variant->getChildren().at(0)->getRootOperation()->getResultSortedOn(),
        (std::vector<ColumnIndex>{0, 1, 2}));
    EXPECT_EQ(
        variant->getChildren().at(1)->getRootOperation()->getResultSortedOn(),
        (std::vector<ColumnIndex>{0, 1}));
    auto result = variant->getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected =
        makeIdTableFromVector({{1, U, U, 4}, {1, 2, 4, U}, {2, U, U, 8}});
    EXPECT_EQ(result->idTable(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    auto tree = unionOperation.makeSortedTree({0, 3, 1, 2});
    ASSERT_TRUE(tree.has_value());
    auto variant = tree.value()->getRootOperation();
    EXPECT_EQ(variant->getResultSortedOn(),
              (std::vector<ColumnIndex>{0, 3, 1, 2}));
    EXPECT_EQ(
        variant->getChildren().at(0)->getRootOperation()->getResultSortedOn(),
        (std::vector<ColumnIndex>{0, 1, 2}));
    EXPECT_EQ(
        variant->getChildren().at(1)->getRootOperation()->getResultSortedOn(),
        (std::vector<ColumnIndex>{0, 1}));
    auto result = variant->getResult(true, ComputationMode::FULLY_MATERIALIZED);
    auto expected =
        makeIdTableFromVector({{1, 2, 4, U}, {1, U, U, 4}, {2, U, U, 8}});
    EXPECT_EQ(result->idTable(), expected);
  }
  {
    qec->getQueryTreeCache().clearAll();
    EXPECT_THROW(unionOperation.makeSortedTree({}), ad_utility::Exception);
  }
}

// _____________________________________________________________________________
TEST(Union, checkChunkSizeSplitsProperly) {
  using Var = Variable;
  using ::testing::Each;
  auto* qec = ad_utility::testing::getQec();

  IdTable reference{1, qec->getAllocator()};
  reference.resize(Union::chunkSize + (Union::chunkSize / 2) + 1);
  ql::ranges::fill(reference.getColumn(0), Id::makeFromInt(42));
  // Make sure we compute the expensive way
  reference.getColumn(0).back() = Id::makeFromInt(1337);

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, reference.clone(), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(reference), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0});

  Union unionOperation{qec, std::move(leftT), std::move(rightT), {0}};

  qec->getQueryTreeCache().clearAll();
  auto result =
      unionOperation.getResult(true, ComputationMode::LAZY_IF_SUPPORTED);
  auto idTables = result->idTables();

  auto it = idTables.begin();
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), Union::chunkSize);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(42)));

  ++it;
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), Union::chunkSize);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(42)));

  ++it;
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), Union::chunkSize);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(42)));

  ++it;
  ASSERT_NE(it, idTables.end());
  EXPECT_EQ(it->idTable_.size(), 2);
  EXPECT_THAT(it->idTable_.getColumn(0), Each(Id::makeFromInt(1337)));

  ++it;
  EXPECT_EQ(it, idTables.end());
}

// _____________________________________________________________________________
TEST(Union, columnOriginatesFromGraphOrUndef) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();

  IdTable reference{2, qec->getAllocator()};

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, reference.clone(), Vars{Var{"?a"}, Var{"?d"}}, false,
      std::vector<ColumnIndex>{0, 1});

  auto index = ad_utility::makeExecutionTree<IndexScan>(
      qec, Permutation::PSO,
      SparqlTripleSimple{Variable{"?a"}, Variable{"?b"}, Variable{"?c"}});

  Union union1{qec, values, values};
  EXPECT_FALSE(union1.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_FALSE(union1.columnOriginatesFromGraphOrUndef(Var{"?d"}));
  EXPECT_THROW(union1.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  Union union2{qec, values, index};
  EXPECT_FALSE(union2.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_TRUE(union2.columnOriginatesFromGraphOrUndef(Var{"?b"}));
  EXPECT_TRUE(union2.columnOriginatesFromGraphOrUndef(Var{"?c"}));
  EXPECT_FALSE(union2.columnOriginatesFromGraphOrUndef(Var{"?d"}));
  EXPECT_THROW(union2.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  Union union3{qec, index, values};
  EXPECT_FALSE(union3.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_TRUE(union3.columnOriginatesFromGraphOrUndef(Var{"?b"}));
  EXPECT_TRUE(union3.columnOriginatesFromGraphOrUndef(Var{"?c"}));
  EXPECT_FALSE(union3.columnOriginatesFromGraphOrUndef(Var{"?d"}));
  EXPECT_THROW(union3.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);

  Union union4{qec, index, index};
  EXPECT_TRUE(union4.columnOriginatesFromGraphOrUndef(Var{"?a"}));
  EXPECT_TRUE(union4.columnOriginatesFromGraphOrUndef(Var{"?b"}));
  EXPECT_TRUE(union4.columnOriginatesFromGraphOrUndef(Var{"?c"}));
  EXPECT_THROW(union4.columnOriginatesFromGraphOrUndef(Var{"?notExisting"}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(Union, getCostEstimate) {
  using Var = Variable;
  auto* qec = ad_utility::testing::getQec();
  IdTable oneColumn{1, qec->getAllocator()};
  oneColumn.resize(100);
  ql::ranges::fill(oneColumn.getColumn(0), Id::makeUndefined());
  IdTable twoColumns{2, qec->getAllocator()};
  twoColumns.resize(100);
  ql::ranges::fill(twoColumns.getColumn(0), Id::makeUndefined());
  ql::ranges::fill(twoColumns.getColumn(1), Id::makeUndefined());

  auto valuesA = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, oneColumn.clone(), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0});

  auto valuesB = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, oneColumn.clone(), Vars{Var{"?b"}}, false,
      std::vector<ColumnIndex>{0});

  auto valuesAb = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, twoColumns.clone(), Vars{Var{"?a"}, Var{"?b"}}, false,
      std::vector<ColumnIndex>{0, 1});

  Union sortedUnionWithUndefCol{qec, valuesB->clone(), valuesAb->clone(), {1}};
  Union sortedUnionWithWrongUndefCol{
      qec, valuesA->clone(), valuesAb->clone(), {0}};

  // Second option is way cheaper because the faster implementation can be used.
  EXPECT_LT(sortedUnionWithUndefCol.getCostEstimate(),
            sortedUnionWithWrongUndefCol.getCostEstimate());

  Union unsortedUnionWithSingleVar{qec, valuesA->clone(), valuesA->clone()};
  Union unsortedUnionWithDifferentVars{qec, valuesA->clone(), valuesB->clone()};
  Union sortedUnionWithSingleVar{qec, valuesA->clone(), valuesA->clone(), {0}};
  Union sortedUnionWithTwoVars{
      qec, valuesAb->clone(), valuesAb->clone(), {0, 1}};

  EXPECT_LT(unsortedUnionWithSingleVar.getCostEstimate(),
            unsortedUnionWithDifferentVars.getCostEstimate());
  EXPECT_LT(unsortedUnionWithSingleVar.getCostEstimate(),
            sortedUnionWithSingleVar.getCostEstimate());
  EXPECT_LT(sortedUnionWithSingleVar.getCostEstimate(),
            sortedUnionWithTwoVars.getCostEstimate());
  sortedUnionWithSingleVar.getCostEstimate();
  sortedUnionWithTwoVars.getCostEstimate();

  IdTable oneColumnSmall{1, qec->getAllocator()};
  oneColumn.resize(2);
  ql::ranges::fill(oneColumn.getColumn(0), Id::makeUndefined());

  auto valuesSmall = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, oneColumn.clone(), Vars{Var{"?a"}}, false,
      std::vector<ColumnIndex>{0});
  Union unsortedUnionSmall{qec, valuesSmall->clone(), valuesSmall->clone()};
  // Union should never be free.
  EXPECT_GT(unsortedUnionSmall.getCostEstimate(),
            valuesSmall->getCostEstimate() * 2);
}
