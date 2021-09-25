// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gtest/gtest.h>

#include <unordered_set>

#include "../src/parser/SetOfIntervals.h"
#include "../src/util/Random.h"

using namespace ad_utility;

TEST(SetOfIntervals, SortAndCheckDisjointAndNonempty) {
  // Sorted and valid set.
  SetOfIntervals s{{{0, 2}, {2, 5}, {6, 12}}};
  auto t = SortAndCheckDisjointAndNonempty(s);
  ASSERT_EQ(s, t);
  // Unsorted  and valid set.
  SetOfIntervals u{{{6, 12}, {0, 2}, {2, 5}}};
  ASSERT_EQ(s, SortAndCheckDisjointAndNonempty(u));

  // The empty set is valid.
  SetOfIntervals empty{};
  ASSERT_EQ(empty, SortAndCheckDisjointAndNonempty(empty));

  // Invalid set with empty interval.
  SetOfIntervals emptyInterval{{{4, 5}, {2, 2}}};
  ASSERT_THROW(SortAndCheckDisjointAndNonempty(emptyInterval),
               ad_semsearch::Exception);

  // Invalid set with overlapping intervals
  SetOfIntervals overlapping{{{4, 6}, {2, 5}}};
  ASSERT_THROW(SortAndCheckDisjointAndNonempty(overlapping),
               ad_semsearch::Exception);
}

TEST(SetOfIntervals, CheckSortedAndDisjointAndSimplify) {
  SetOfIntervals nonOverlapping{{{0, 2}, {3, 5}, {6, 8}}};
  ASSERT_EQ(nonOverlapping, CheckSortedAndDisjointAndSimplify(nonOverlapping));
  SetOfIntervals overlapping{{{0, 2}, {2, 5}, {5, 8}}};
  SetOfIntervals expected{{{0, 8}}};
  ASSERT_EQ(expected, CheckSortedAndDisjointAndSimplify(overlapping));

  {
    SetOfIntervals partiallyOverlapping{{{0, 2}, {3, 5}, {5, 7}}};
    SetOfIntervals expected2{{{0, 2}, {3, 7}}};
    ASSERT_EQ(expected2,
              CheckSortedAndDisjointAndSimplify(partiallyOverlapping));
  }

  SetOfIntervals unsorted{{{3, 5}, {0, 2}}};
  ASSERT_THROW(CheckSortedAndDisjointAndSimplify(unsorted),
               ad_semsearch::Exception);
}

TEST(SetOfIntervals, Union) {
  SetOfIntervals s{{{4, 6}, {0, 2}, {10, 380}}};
  SetOfIntervals empty{};
  // Union with empty set leaves input unchanged
  ASSERT_EQ(Union{}(s, empty), SortAndCheckDisjointAndNonempty(s));
  ASSERT_EQ(Union{}(empty, s), SortAndCheckDisjointAndNonempty(s));

  SetOfIntervals nonOverlapping{{{2, 3}, {7, 10}, {400, 401}}};
  SetOfIntervals expected{{{0, 3}, {4, 6}, {7, 380}, {400, 401}}};
  ASSERT_EQ(Union{}(s, nonOverlapping), expected);
  ASSERT_EQ(Union{}(nonOverlapping, s), expected);

  {
    // Complete enclosing of two intervals.
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 9}}};
    SetOfIntervals b{{{0, 6}, {8, 9}}};
    SetOfIntervals c{{{0, 6}, {7, 9}}};
    ASSERT_EQ(Union{}(a, b), c);
  }
  {
    // Complete enclosing of three
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 8}}};
    SetOfIntervals b{{{0, 9}}};
    ASSERT_EQ(Union{}(a, b), b);
  }

  {
    // Partial overlap
    SetOfIntervals a{{{2, 3}, {4, 6}, {7, 10}}};
    SetOfIntervals b{{{0, 5}, {8, 11}}};
    SetOfIntervals c{{{0, 6}, {7, 11}}};
    ASSERT_EQ(Union{}(a, b), c);
  }
}

TEST(SetOfIntervals, Intersection) {
  SetOfIntervals s{{{4, 6}, {0, 2}, {10, 380}}};
  SetOfIntervals empty{};
  // Union with empty set leaves input unchanged
  ASSERT_EQ(Intersection{}(s, empty), empty);
  ASSERT_EQ(Intersection{}(empty, s), empty);

  SetOfIntervals noOverlap{{{2, 3}, {7, 10}, {400, 401}}};
  ASSERT_EQ(Intersection{}(s, noOverlap), empty);
  ASSERT_EQ(Intersection{}(noOverlap, s), empty);
  {
    // Complete enclosing of two
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 9}}};
    SetOfIntervals b{{{0, 6}, {8, 10}}};
    SetOfIntervals c{{{2, 3}, {4, 5}, {8, 9}}};
    ASSERT_EQ(Intersection{}(a, b), c);
  }
  {
    // Complete enclosing of three
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 8}}};
    SetOfIntervals b{{{0, 9}}};
    ASSERT_EQ(Intersection{}(a, b), a);
  }

  {
    // Partial overlap
    SetOfIntervals a{{{2, 3}, {4, 6}, {7, 10}}};
    SetOfIntervals b{{{0, 5}, {8, 11}}};
    SetOfIntervals c{{{2, 3}, {4, 5}, {8, 10}}};
    ASSERT_EQ(Intersection{}(a, b), c);
  }
}

TEST(SetOfIntervals, toBitContainer) {
  SetOfIntervals a{{{2, 3}, {4, 6}, {7, 10}}};
  std::unordered_set<size_t> elements{2, 4, 5, 7, 8, 9};
  auto expanded = toBitVector(a, 200);
  ASSERT_EQ(200ul, expanded.size());
  for (size_t i = 0; i < expanded.size(); ++i) {
    ASSERT_EQ(elements.contains(i), expanded[i]);
  }
}
