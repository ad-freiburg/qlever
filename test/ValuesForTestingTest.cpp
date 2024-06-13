//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "./util/IdTableHelpers.h"
#include "engine/ValuesForTesting.h"
#include "gtest/gtest.h"
#include "util/IndexTestHelpers.h"

using namespace ad_utility::testing;
TEST(ValuesForTesting, valuesForTesting) {
  auto table = makeIdTableFromVector({{3, 4}, {12, 2}, {1, 63}});

  // Too few variables;
  ASSERT_ANY_THROW(
      (ValuesForTesting{getQec(), table.clone(), {Variable{"?x"}}}));

  ValuesForTesting v{
      getQec(), table.clone(), {Variable{"?x"}, {Variable{"?y"}}}};
  // The following line has no effect. TODO<joka921> provide default
  // implementations for such boilerplate methods in the `Operation` base class.
  ASSERT_EQ(v.getResultWidth(), 2u);
  ASSERT_EQ(v.getSizeEstimate(), 3u);
  ASSERT_EQ(v.getCostEstimate(), 3u);
  ASSERT_EQ(v.getMultiplicity(0), 42.0);
  ASSERT_EQ(v.getMultiplicity(1), 84.0);

  ASSERT_THAT(
      v.getCacheKey(),
      ::testing::StartsWith("Values for testing with 2 columns. V:3 V:12"));
  ASSERT_THAT(v.getCacheKey(), ::testing::EndsWith("Supports limit: 0"));
  ASSERT_EQ(v.getDescriptor(), "explicit values for testing");
  ASSERT_TRUE(v.resultSortedOn().empty());
  ASSERT_TRUE(v.getChildren().empty());

  auto result = v.getResult();
  ASSERT_EQ(result->idTable(), table);
}
