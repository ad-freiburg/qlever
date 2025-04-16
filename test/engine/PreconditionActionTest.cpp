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
  EXPECT_TRUE(PreconditionAction::ALREADY_SATISFIED.isAlreadySatisfied());
  EXPECT_FALSE(PreconditionAction::ALREADY_SATISFIED.isNotSatisfiable());
  EXPECT_THROW(
      PreconditionAction{PreconditionAction::ALREADY_SATISFIED}.getTree(),
      ad_utility::Exception);
  EXPECT_TRUE(PreconditionAction::NOT_SATISFIABLE.isNotSatisfiable());
  EXPECT_FALSE(PreconditionAction::NOT_SATISFIABLE.isAlreadySatisfied());
  EXPECT_THROW(
      PreconditionAction{PreconditionAction::NOT_SATISFIABLE}.getTree(),
      ad_utility::Exception);
  auto qec = ad_utility::testing::getQec();
  auto tree = std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<ValuesForTesting>(
               qec, IdTable{0, qec->getAllocator()},
               std::vector<std::optional<Variable>>{}));
  PreconditionAction action{tree};
  EXPECT_FALSE(action.isNotSatisfiable());
  EXPECT_FALSE(action.isAlreadySatisfied());
  EXPECT_EQ(tree, std::move(action).getTree());
}
