// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (joka921) <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "parser/data/LimitOffsetClause.h"
#include "util/Random.h"

// _____________________________________________________________________________
TEST(LimitOffsetClause, actualOffset) {
  LimitOffsetClause l;
  l._offset = 42;
  ASSERT_EQ(l.actualOffset(500), 42u);
  ASSERT_EQ(l.actualOffset(42), 42u);
  ASSERT_EQ(l.actualOffset(38), 38u);
  ASSERT_EQ(l.actualOffset(0), 0u);
}

// _____________________________________________________________________________
TEST(LimitOffsetClause, upperBound) {
  LimitOffsetClause l;
  l._offset = 42;
  auto max = std::numeric_limits<uint64_t>::max();
  // Offset, but no limit.
  ASSERT_EQ(l.upperBound(500), 500u);
  ASSERT_EQ(l.upperBound(max), max);
  ASSERT_EQ(l.upperBound(38), 38u);
  ASSERT_EQ(l.upperBound(0), 0u);

  // Offset and limit
  l._limit = 10;
  ASSERT_EQ(l.upperBound(500), 52u);
  ASSERT_EQ(l.upperBound(max), 52u);
  ASSERT_EQ(l.upperBound(50), 50u);
  ASSERT_EQ(l.upperBound(38), 38u);
  ASSERT_EQ(l.upperBound(0), 0u);

  // Offset + limit would overflow.
  l._limit = max - 20;
  ASSERT_EQ(l.upperBound(500), 500u);
  ASSERT_EQ(l.upperBound(max), max);
  ASSERT_EQ(l.upperBound(50), 50u);
  ASSERT_EQ(l.upperBound(38), 38u);
  ASSERT_EQ(l.upperBound(0), 0u);
}

// _____________________________________________________________________________
TEST(LimitOffsetClause, randomTestingOfInvariants) {
  ad_utility::FastRandomIntGenerator<uint64_t> r;
  for (size_t i = 0; i < 10'000; ++i) {
    LimitOffsetClause l;
    l._limit = r();
    l._offset = r();
    auto input = r();
    ASSERT_EQ(l.actualSize(input), l.upperBound(input) - l.actualOffset(input));
    ASSERT_LE(l.upperBound(input), input);
    ASSERT_LE(l.actualSize(input), input);
    ASSERT_LE(l.actualOffset(input), input);
    ASSERT_LE(l.actualOffset(input), l.upperBound(input));
  }
}

// _____________________________________________________________________________
TEST(LimitOffsetClause, mergeLimitAndOffset) {
  LimitOffsetClause l;

  l.mergeLimitAndOffset({std::nullopt, 1});
  EXPECT_EQ(l, LimitOffsetClause(std::nullopt, 1));

  l.mergeLimitAndOffset({20, 2});
  EXPECT_EQ(l, LimitOffsetClause(20, 3));

  l.mergeLimitAndOffset({std::nullopt, 4});
  EXPECT_EQ(l, LimitOffsetClause(20, 7));

  l.mergeLimitAndOffset({10, 8});
  EXPECT_EQ(l, LimitOffsetClause(10, 15));

  // Make sure everything that's not LIMIT/OFFSET is ignored.
  l.mergeLimitAndOffset({std::nullopt, 0, 100, 200});
  EXPECT_EQ(l, LimitOffsetClause(10, 15));
}
