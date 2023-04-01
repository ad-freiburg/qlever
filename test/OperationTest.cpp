// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <kalmbach@cs.uni-freiburg.de>

#include "./IndexTestHelpers.h"
#include "engine/NeutralElementOperation.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

TEST(OperationTest, limitIsRepresentedInCacheKey) {
  NeutralElementOperation n{ad_utility::testing::getQec()};
  EXPECT_THAT(n.asString(), testing::Not(testing::HasSubstr("LIMIT 20")));
  LimitOffsetClause l;
  l._limit = 20;
  n.setLimit(l);
  EXPECT_THAT(n.asString(), testing::HasSubstr("LIMIT 20"));
  EXPECT_THAT(n.asString(), testing::Not(testing::HasSubstr("OFFSET 34")));

  l._offset = 34;
  n.setLimit(l);
  EXPECT_THAT(n.asString(), testing::HasSubstr("OFFSET 34"));
}
