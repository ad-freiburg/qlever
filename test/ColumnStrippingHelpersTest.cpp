// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Anna Kaiser <anna.kaiser@uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "engine/Distinct.h"
#include "engine/QueryExecutionTree.h"
#include "util/ColumnStrippingHelpers.h"
#include "util/IdTableHelpers.h"
#include "util/IndexTestHelpers.h"

// _______________________________________________________________________________________
TEST(VarsRequiredFromSubtree, add) {
  Variable nameVar("?name");
  Variable ageVar("?age");
  Variable streetVar("?street");
  std::set<Variable> varSet = {nameVar, ageVar, streetVar};
  VarsRequiredFromSubtree helper(&varSet);

  // Add another variable that has already been added to the helper via the
  // constructor
  helper.add(nameVar);
  EXPECT_TRUE(
      ad_utility::contains(*(helper.varsRequiredFromSubtree_), nameVar));
  EXPECT_EQ((helper.varsRequiredFromSubtree_)->size(), 3);

  // Add another variable that is not yet part of the helper.
  Variable cityVar("?city");
  EXPECT_FALSE(
      ad_utility::contains(*(helper.varsRequiredFromSubtree_), cityVar));
  helper.add(cityVar);
  EXPECT_TRUE(
      ad_utility::contains(*(helper.varsRequiredFromSubtree_), cityVar));
  EXPECT_EQ((helper.varsRequiredFromSubtree_)->size(), 4);
}

// _______________________________________________________________________________________
TEST(VarsRequiredFromSubtree, get) {
  // Check the return value if the constructor received an empty set.
  std::set<Variable> varSet;
  VarsRequiredFromSubtree helper(&varSet);
  EXPECT_EQ(varSet, helper.get());

  // Check return value if `add` function is never called.
  std::set<Variable> varSet_1 = {Variable("?city")};
  VarsRequiredFromSubtree helper_1(&varSet_1);
  EXPECT_EQ(varSet_1, helper_1.get());

  // Check return value after `add` function has been called.
  helper_1.add(Variable("?age"));
  auto returnSet = helper_1.get();
  EXPECT_EQ(returnSet.size(), 2);
  EXPECT_TRUE(ad_utility::contains(returnSet, Variable("?age")));
  EXPECT_TRUE(ad_utility::contains(returnSet, Variable("?city")));

  // Check return value after `add` function has been called twice.
  helper_1.add(Variable("?person"));
  returnSet = helper_1.get();
  EXPECT_EQ(returnSet.size(), 3);
}

// _______________________________________________________________________________________
TEST(makeTreeWithOptionalStripOperation, basic) {
  // Create an operation (in this test case the `Distinct` operation).
  IdTable input{makeIdTableFromVector(
      {{6, 1, 3, 6}, {2, 2, 3, 5}, {3, 6, 5, 4}, {1, 6, 5, 1}})};
  auto qec = ad_utility::testing::getQec();
  qec->getQueryTreeCache().clearAll();

  auto values = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(input),
      std::vector<std::optional<Variable>>{Variable{"?a"}, Variable{"?b"},
                                           Variable{"?c"}, Variable{"?d"}});
  Distinct distinct(qec, values, {1});

  // Check the case in which an additional `StripColumns` operation is added.
  {
    // Use helper and create subtree of the `Distinct` operation.
    std::set<Variable> variablesRequestedFromParent = {Variable{"?a"}};
    VarsRequiredFromSubtree helper(&variablesRequestedFromParent);
    helper.add(Variable{"?b"});
    const std::set<Variable>& varsRequiredFromSubtree = helper.get();
    QueryExecutionTree subtreeWithDistinctRoot(
        qec, std::make_shared<Distinct>(distinct));

    auto subtreeWithStrippedColumns =
        QueryExecutionTree::makeTreeWithStrippedColumns(
            std::make_shared<QueryExecutionTree>(subtreeWithDistinctRoot),
            varsRequiredFromSubtree);

    auto tree =
        columnStrippingHelpers::makeTreeWithOptionalStripOperation<Distinct>(
            qec, variablesRequestedFromParent,
            std::move(subtreeWithStrippedColumns), std::vector<ColumnIndex>{1});
    ASSERT_TRUE(tree.has_value());
    auto qet = *tree;

    // Check whether root operation is a `StripColumns` operation with the
    // expected variables.
    auto stripColumnsOperation =
        dynamic_cast<StripColumns*>(qet->getRootOperation().get());
    EXPECT_NE(stripColumnsOperation, nullptr);
    auto var2colMapStripCols = qet->getVariableColumns();
    EXPECT_EQ(var2colMapStripCols.size(), 1);
    EXPECT_TRUE(var2colMapStripCols.contains(Variable{"?a"}));

    // Check whether child of `StripColumns` operation is `Distinct` operation.
    auto strColSubtree = stripColumnsOperation->getChildren();
    ASSERT_NE(strColSubtree.at(0), nullptr);
    auto distinctOp = strColSubtree.at(0)->getRootOperation();
    Distinct* distinctOperation = dynamic_cast<Distinct*>(distinctOp.get());
    EXPECT_NE(distinctOperation, nullptr);
    auto var2colMapDistinct = strColSubtree.at(0)->getVariableColumns();
    EXPECT_EQ(var2colMapDistinct.size(), 2);
    EXPECT_TRUE(var2colMapDistinct.contains(Variable{"?a"}));
    EXPECT_TRUE(var2colMapDistinct.contains(Variable{"?b"}));
  }

  // Check the case in which no additional `StripColumns` operation is added.
  {
    // Use helper and create subtree of the `Distinct` operation.
    std::set<Variable> variablesRequestedFromParent = {Variable{"?a"},
                                                       Variable{"?b"}};
    VarsRequiredFromSubtree helper(&variablesRequestedFromParent);
    helper.add(Variable{"?b"});
    const std::set<Variable>& varsRequiredFromSubtree = helper.get();
    QueryExecutionTree subtreeWithDistinctRoot(
        qec, std::make_shared<Distinct>(distinct));

    auto subtreeWithStrippedColumns =
        QueryExecutionTree::makeTreeWithStrippedColumns(
            std::make_shared<QueryExecutionTree>(subtreeWithDistinctRoot),
            varsRequiredFromSubtree);

    auto tree =
        columnStrippingHelpers::makeTreeWithOptionalStripOperation<Distinct>(
            qec, variablesRequestedFromParent,
            std::move(subtreeWithStrippedColumns), std::vector<ColumnIndex>{1});
    ASSERT_TRUE(tree.has_value());
    auto qet = *tree;

    // Check whether root operation is a `Distinct` operation with the expected
    // variables.
    EXPECT_NE(dynamic_cast<Distinct*>(qet->getRootOperation().get()), nullptr);
    auto var2colMap = qet->getVariableColumns();
    EXPECT_EQ(var2colMap.size(), 2);
    EXPECT_TRUE(var2colMap.contains(Variable{"?a"}));
    EXPECT_TRUE(var2colMap.contains(Variable{"?b"}));
  }
}
