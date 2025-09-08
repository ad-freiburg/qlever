// Copyright 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/QueryExecutionTree.h"
#include "engine/Sort.h"
#include "engine/Union.h"

using namespace ad_utility::testing;

// _____________________________________________________________________________
TEST(QueryExecutionTree, getVariableColumn) {
  auto qec = getQec();
  auto x = Variable{"?x"};
  auto y = Variable{"?y"};
  auto qet = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{3}}),
      std::vector<std::optional<Variable>>{x});
  EXPECT_EQ(qet->getVariableColumn(x), 0u);
  EXPECT_THAT(qet->getVariableColumnOrNullopt(x), ::testing::Optional(0u));
  EXPECT_EQ(qet->getVariableColumnOrNullopt(y), std::nullopt);
  EXPECT_ANY_THROW(qet->getVariableColumn(y));
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, sortedUnionSpecialCase) {
  using Var = Variable;
  using Vars = std::vector<std::optional<Variable>>;
  auto* qec = ad_utility::testing::getQec();

  auto leftT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{1}}), Vars{Var{"?a"}});

  auto rightT = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0}}), Vars{Var{"?a"}});

  auto sortedTree = QueryExecutionTree::createSortedTree(
      ad_utility::makeExecutionTree<Union>(qec, leftT, rightT), {0});

  // Ensure no `Sort` is added on top
  EXPECT_TRUE(std::dynamic_pointer_cast<Union>(sortedTree->getRootOperation()));

  qec->getQueryTreeCache().clearAll();
  auto result = sortedTree->getResult(false);
  EXPECT_EQ(result->idTable(), makeIdTableFromVector({{0}, {1}}));
}

// _____________________________________________________________________________
TEST(QueryExecutionTree, createSortedTreeAnyPermutation) {
  using Vars = std::vector<std::optional<Variable>>;
  Vars vars{std::nullopt, std::nullopt, std::nullopt};
  using SC = std::vector<ColumnIndex>;
  auto* qec = getQec();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, makeIdTableFromVector({{0, 1, 2}}), vars, false, SC{0, 1, 2});

  EXPECT_EQ(QueryExecutionTree::createSortedTreeAnyPermutation(values, {0, 1}),
            values);

  EXPECT_EQ(
      QueryExecutionTree::createSortedTreeAnyPermutation(values, {1, 0, 2}),
      values);

  {
    auto sortedTree =
        QueryExecutionTree::createSortedTreeAnyPermutation(values, {2});
    EXPECT_NE(sortedTree, values);
    auto castTree =
        std::dynamic_pointer_cast<Sort>(sortedTree->getRootOperation());
    ASSERT_TRUE(castTree);
    EXPECT_EQ(castTree->getResultSortedOn(), (SC{2}));
  }

  {
    auto valuesNotSorted = ad_utility::makeExecutionTree<ValuesForTesting>(
        qec, makeIdTableFromVector({{0, 1, 2}}), vars);
    auto sortedTree = QueryExecutionTree::createSortedTreeAnyPermutation(
        valuesNotSorted, {0, 1});
    EXPECT_NE(sortedTree, valuesNotSorted);
    auto castTree =
        std::dynamic_pointer_cast<Sort>(sortedTree->getRootOperation());
    ASSERT_TRUE(castTree);
    EXPECT_EQ(castTree->getResultSortedOn(), (SC{0, 1}));
  }
}
