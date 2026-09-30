// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../src/engine/Distinct.h"
#include "../src/engine/QueryExecutionTree.h"
#include "../src/util/ColumnStrippingHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

TEST(VarsRequiredFromSubtree, add) {
  Variable nameVar("?name");
  Variable ageVar("?age");
  Variable streetVar("?street");
  std::set<Variable> varSet = {nameVar, ageVar, streetVar};
  VarsRequiredFromSubtree helper(&varSet);

  // Add another Variable that has already been added to the helper via
  // constructor
  helper.add(nameVar);
  EXPECT_TRUE(
      ad_utility::contains(*(helper.varsRequiredFromSubtree_), nameVar));
  EXPECT_EQ((*(helper.varsRequiredFromSubtree_)).size(), 3);

  // Add another Variable that is not yet part of the helper
  Variable cityVar("?city");
  EXPECT_FALSE(
      ad_utility::contains(*(helper.varsRequiredFromSubtree_), cityVar));
  helper.add(cityVar);
  EXPECT_TRUE(
      ad_utility::contains(*(helper.varsRequiredFromSubtree_), cityVar));
  EXPECT_EQ((*(helper.varsRequiredFromSubtree_)).size(), 4);
}

TEST(VarsRequiredFromSubtree, get) {
  // check return value if constructor received empty set
  std::set<Variable> varSet = {};
  VarsRequiredFromSubtree helper(&varSet);
  EXPECT_EQ(varSet, helper.get());

  // check return value if add() is never called
  std::set<Variable> varSet_1 = {Variable("?city")};
  VarsRequiredFromSubtree helper_1(&varSet_1);
  EXPECT_EQ(varSet_1, helper_1.get());

  // check return value after add() has been called
  helper_1.add(Variable("?age"));
  auto returnSet = helper_1.get();
  EXPECT_EQ(returnSet.size(), 2);
  EXPECT_TRUE(ad_utility::contains(returnSet, Variable("?age")));
  EXPECT_TRUE(ad_utility::contains(returnSet, Variable("?city")));

  // check return value after add() has been called twice
  helper_1.add(Variable("?person"));
  returnSet = helper_1.get();
  EXPECT_EQ(returnSet.size(), 3);
}

TEST(makeTreeWithOptionalStripOperation, basic) {
  IdTable input{makeIdTableFromVector(
      {{6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};

  auto qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"},
                                           Variable{"?c"}, Variable{"?d"}});

  // TODO case that subtree with operation as root is returned
  {
    Distinct distinct_B(qec, values, {1});

    std::set<Variable> variablesRequestedFromParent = {Variable{"?a"},
                                                       Variable{"?b"}};

    VarsRequiredFromSubtree helper(&variablesRequestedFromParent);

    // Collect all the variables that are required from the subtree.
    const std::set<Variable>& varsRequiredFromSubtree = helper.get();

    QueryExecutionTree subtree_A(qec, std::make_shared<Distinct>(distinct_B));

    // Continue with the recursion and strip columns of subtree.
    auto subtree_A_new = QueryExecutionTree::makeTreeWithStrippedColumns(
        std::make_shared<QueryExecutionTree>(subtree_A),
        varsRequiredFromSubtree);

    // Create query execution tree with Distinct-Operation as root-Operation and
    // add additional stripColumns-Operation if needed.
    const Variable varB = Variable{"?b"};
    auto tree = makeTreeWithOptionalStripOperation<Distinct>(
        qec, variablesRequestedFromParent, std::vector<const Variable*>{&varB},
        std::move(subtree_A_new), std::vector<ColumnIndex>{1});

    ASSERT_TRUE(tree.has_value());

    auto qet = *tree;

    // TODO: check whether root-operation is a distinct-operation as expected
    EXPECT_NE(dynamic_cast<Distinct*>(qet->getRootOperation().get()),
              nullptr);

    auto var2colMap = qet->getVariableColumns();
    EXPECT_EQ(var2colMap.size(), 2);
    EXPECT_TRUE(var2colMap.contains(Variable{"?a"}));
    EXPECT_TRUE(var2colMap.contains(Variable{"?b"}));
  }

  // TODO case that StripColumns as root is returned
  {}
}
