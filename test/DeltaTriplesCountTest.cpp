// Copyright 2025, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock.h>

#include "index/DeltaTriples.h"

// _____________________________________________________________________________
TEST(DeltaTriplesCountTest, toJson) {
  constexpr DeltaTriplesCount count{5, 3};
  const nlohmann::json expected = {
      {"inserted", 5}, {"deleted", 3}, {"total", 8}};
  nlohmann::json actual;
  to_json(actual, count);
  EXPECT_THAT(actual, testing::Eq(expected));
}

// _____________________________________________________________________________
TEST(DeltaTriplesCountTest, subtractOperator) {
  constexpr DeltaTriplesCount count1{10, 5};
  constexpr DeltaTriplesCount count2{3, 2};
  constexpr DeltaTriplesCount expected{7, 3};
  const DeltaTriplesCount actual = count1 - count2;
  EXPECT_THAT(actual, testing::Eq(expected));
}
