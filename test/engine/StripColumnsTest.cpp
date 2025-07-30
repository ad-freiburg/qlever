// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR/QL
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// QL =  QLeverize AG

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/IdTableHelpers.h"
#include "../util/IndexTestHelpers.h"
#include "./ValuesForTesting.h"
#include "engine/QueryExecutionTree.h"
#include "engine/StripColumns.h"

namespace {
using Vars = std::vector<std::optional<Variable>>;
using namespace ad_utility::testing;
using namespace ::testing;

// _____________________________________________________________________________
StripColumns makeStrip(QueryExecutionContext* qec, IdTable idTable,
                       const std::set<Variable>& varsToKeep) {
  auto valuesTree = ad_utility::makeExecutionTree<ValuesForTesting>(
      qec, std::move(idTable),
      Vars{Variable{"?a"}, Variable{"?b"}, Variable{"?c"}});
  return {qec, std::move(valuesTree), varsToKeep};
}

// _____________________________________________________________________________
TEST(StripColumns, basicMembers) {
  auto strip =
      makeStrip(getQec(), makeIdTableFromVector({{1, 2, 3}}),
                {Variable{"?b"}, Variable{"?notFoundInChild"} Variable{"?c"}});

  EXPECT_EQ(strip.getDescriptor(), "Strip Columns");
  EXPECT_THAT(strip.getChildren(),
              ElementsAre(Pointee(AD_PROPERTY(
                  QueryExecutionTree, getRootOperation,
                  Pointee(WhenDynamicCastTo<const ValuesForTesting&>(_))))));
  EXPECT_THAT(strip.getCacheKey(),
              AllOf(HasSubstr("StripColumns"), HasSubstr("(1,2"),
                    HasSubstr("Values for testing")));

  EXPECT_EQ(strip.getResultWidth(), 2);
  EXPECT_EQ(strip.getCostEstimate(), strip.getChildren()[0]->getCostEstimate());
  EXPECT_EQ(strip.getSizeEstimate(), strip.getChildren()[0]->getSizeEstimate());

  EXPECT_EQ(strip.getMultiplicity(25), 1.0f);
  EXPECT_EQ(strip.getMultiplicity(0),
            strip.getChildren()[0]->getMultiplicity(1));
  EXPECT_FALSE(strip.knownEmptyResult());

  // TODO<joka921> Add tests for `clone`.
  // TODO<joka921> better tests for `resultSortedOn`.
  EXPECT_TRUE(strip.getResultSortedOn().empty());

  // TODO<joka921> Better tests.
  EXPECT_EQ(strip.getExternallyVisibleVariableColumns().size(), 2);
}

// _____________________________________________________________________________
TEST(StripColumns, computeResult) {
  // TODO<joka921> Write tests.
}

}  // namespace
