//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/PreconditionAction.h"
#include "engine/QueryExecutionTree.h"

// _____________________________________________________________________________
TEST(PreconditionAction, basicFunctionality) {
  EXPECT_TRUE(PreconditionAction::IMPLICITLY_SATISFIED.isImplicitlySatisfied());
  EXPECT_FALSE(
      PreconditionAction::IMPLICITLY_SATISFIED.mustBeSatisfiedExternally());
  EXPECT_THROW(
      PreconditionAction{PreconditionAction::IMPLICITLY_SATISFIED}.getTree(),
      ad_utility::Exception);
  EXPECT_TRUE(
      PreconditionAction::SATISFY_EXTERNALLY.mustBeSatisfiedExternally());
  EXPECT_FALSE(PreconditionAction::SATISFY_EXTERNALLY.isImplicitlySatisfied());
  EXPECT_THROW(
      PreconditionAction{PreconditionAction::SATISFY_EXTERNALLY}.getTree(),
      ad_utility::Exception);
  auto qec = ad_utility::testing::getQec();
  auto tree = std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<ValuesForTesting>(
               qec, IdTable{0, qec->getAllocator()},
               std::vector<std::optional<Variable>>{}));
  PreconditionAction action{tree};
  EXPECT_FALSE(action.mustBeSatisfiedExternally());
  EXPECT_FALSE(action.isImplicitlySatisfied());
  EXPECT_EQ(tree, std::move(action).getTree());
}
