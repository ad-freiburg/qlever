// Copyright 2018, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#include <gtest/gtest.h>

#include <array>
#include <vector>

#include "./engine/ValuesForTesting.h"
#include "./util/IdTableHelpers.h"
#include "./util/IdTestHelpers.h"
#include "engine/Union.h"
#include "global/Id.h"
#include "util/IndexTestHelpers.h"

namespace {
auto V = ad_utility::testing::VocabId;
using Vars = std::vector<std::optional<Variable>>;
}  // namespace

// A simple test for computing a union.
TEST(UnionTest, computeUnion) {
  auto* qec = ad_utility::testing::getQec();
  IdTable left = makeIdTableFromVector({{V(1)}, {V(2)}, {V(3)}});
  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, left.clone(), Vars{Variable{"?x"}});

  IdTable right = makeIdTableFromVector({{V(4), V(5)}, {V(6), V(7)}});
  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, right.clone(), Vars{Variable{"?u"}, Variable{"?x"}});

  Union u{ad_utility::testing::getQec(), leftT, rightT};
  auto resultTable = u.computeResultOnlyForTesting();
  const auto& result = resultTable.idTable();

  auto U = Id::makeUndefined();
  auto expected = makeIdTableFromVector(
      {{V(1), U}, {V(2), U}, {V(3), U}, {V(5), V(4)}, {V(7), V(6)}});
  ASSERT_EQ(result, expected);
}

// A test with large inputs to test the chunked writing that is caused by the
// timeout checks.
TEST(UnionTest, computeUnionLarge) {
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

  Union u{ad_utility::testing::getQec(), leftT, rightT};
  auto resultTable = u.computeResultOnlyForTesting();
  const auto& result = resultTable.idTable();

  ASSERT_EQ(result, makeIdTableFromVector(expected));
}

// _____________________________________________________________________________
TEST(UnionTest, computeUnionLazy) {
  auto runTest = [](bool nonLazyChilds,
                    ad_utility::source_location loc =
                        ad_utility::source_location::current()) {
    auto l = generateLocationTrace(loc);
    auto* qec = ad_utility::testing::getQec();
    IdTable left = makeIdTableFromVector({{V(1)}, {V(2)}, {V(3)}});
    auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(left), Vars{Variable{"?x"}}, false,
        std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt, nonLazyChilds);

    IdTable right = makeIdTableFromVector({{V(4), V(5)}, {V(6), V(7)}});
    auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, std::move(right), Vars{Variable{"?u"}, Variable{"?x"}}, false,
        std::vector<ColumnIndex>{}, LocalVocab{}, std::nullopt, nonLazyChilds);

    Union u{ad_utility::testing::getQec(), std::move(leftT), std::move(rightT)};
    auto resultTable = u.computeResultOnlyForTesting(true);
    ASSERT_FALSE(resultTable.isFullyMaterialized());
    auto& result = resultTable.idTables();

    auto U = Id::makeUndefined();
    auto expected1 = makeIdTableFromVector({{V(1), U}, {V(2), U}, {V(3), U}});
    auto expected2 = makeIdTableFromVector({{V(5), V(4)}, {V(7), V(6)}});

    auto iterator = result.begin();
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(*iterator, expected1);

    ++iterator;
    ASSERT_NE(iterator, result.end());
    ASSERT_EQ(*iterator, expected2);

    ASSERT_EQ(++iterator, result.end());
  };

  runTest(false);
  runTest(true);
}
