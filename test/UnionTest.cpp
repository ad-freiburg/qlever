// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <vector>

#include "./engine/ValuesForTesting.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/NeutralElementOperation.h"
#include "engine/Union.h"
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
                    ad_utility::source_location loc =
                        ad_utility::source_location::current()) {
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
    auto& result = resultTable.idTables();

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
    auto& result = resultTable.idTables();

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
