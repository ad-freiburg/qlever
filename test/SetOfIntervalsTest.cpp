// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>
#include "../src/parser/BooleanRangeExpressions.h"

using namespace setOfIntervals;

TEST(SetOfIntervals, SortAndCheckInvariants) {
  // Sorted and valid set
  Set s{{0, 2}, {2, 5}, {6, 12}};
  auto t = SortAndCheckInvariants(s);
  ASSERT_EQ(s, t);
  // Unsorted  and valid set
  Set u{{6, 12}, {0, 2}, {2, 5}};
  ASSERT_EQ(s, SortAndCheckInvariants(u));

  // The empty set is valid
  Set empty{};
  ASSERT_EQ(empty, SortAndCheckInvariants(empty));

  // Invalid set with empty interval
  Set emptyInterval{ {4, 5}, {2,2}};
  ASSERT_THROW(SortAndCheckInvariants(emptyInterval), ad_semsearch::Exception);

  // Invalid set with overlapping intervals
  Set overlapping{{4, 6}, {2, 5}};
  ASSERT_THROW(SortAndCheckInvariants(overlapping), ad_semsearch::Exception);
}

TEST(SetOfIntervals, Union) {
  Set l {{4,6}, {0, 2}, {10, 380}};
  Set empty{};
  // Union with empty set leaves input unchanged
  ASSERT_EQ(Union{}(l, empty), SortAndCheckInvariants(l));
  ASSERT_EQ(Union{}(empty, l), SortAndCheckInvariants(l));

  Set noOverlap {{2, 3}, {7, 10}, {400, 401}};
  Set expected { {0, 3}, {4, 6}, {7, 380}, {400, 401}};
  ASSERT_EQ(Union{}(l, noOverlap), expected);
  ASSERT_EQ(Union{}(noOverlap, l), expected);

  {
    // Complete enclosing of two
    Set a {{2, 3}, {4, 5}, {7, 9}};
    Set b {{0, 6}, {8, 9}};
    Set c {{0, 6}, {7, 9}};
    ASSERT_EQ(Union{}(a, b), c);
  }
  {
    // Complete enclosing of three
    Set a {{2, 3}, {4, 5}, {7, 8}};
    Set b {{0, 9}};
    ASSERT_EQ(Union{}(a, b), b);
  }

  {
    // Partial overlap
    Set a {{2, 3}, {4, 6}, {7, 10}};
    Set b {{0, 5}, {8, 11}};
    Set c {{0, 6}, {7, 11}};
    ASSERT_EQ(Union{}(a, b), c);
  }
}

