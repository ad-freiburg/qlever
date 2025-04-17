//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/PreconditionAction.h"
#include "engine/QueryExecutionTree.h"

// _____________________________________________________________________________
TEST(PreconditionAction, basicFunctionality) {
  EXPECT_EQ(
      PreconditionAction{PreconditionAction::IMPLICITLY_SATISFIED}.getTree(),
      std::nullopt);
  EXPECT_EQ(
      PreconditionAction{PreconditionAction::SATISFY_EXTERNALLY}.getTree(),
      std::nullopt);
  EXPECT_EQ(PreconditionAction{PreconditionAction::IMPLICITLY_SATISFIED}
                .handle([]() {
                  ADD_FAILURE() << "This should not be called";
                  return nullptr;
                })
                .getTree(),
            std::nullopt);

  auto qec = ad_utility::testing::getQec();
  auto tree = std::make_shared<QueryExecutionTree>(
      qec, std::make_shared<ValuesForTesting>(
               qec, IdTable{0, qec->getAllocator()},
               std::vector<std::optional<Variable>>{}));

  using namespace ::testing;

  EXPECT_THAT(PreconditionAction{PreconditionAction::SATISFY_EXTERNALLY}
                  .handle([&tree]() { return tree; })
                  .getTree(),
              Optional(Eq(tree)));
  EXPECT_THAT(PreconditionAction{tree}.getTree(), Optional(Eq(tree)));
  EXPECT_THAT(PreconditionAction{tree}
                  .handle([]() {
                    ADD_FAILURE() << "This should not be called";
                    return nullptr;
                  })
                  .getTree(),
              Optional(Eq(tree)));
}
