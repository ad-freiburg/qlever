// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach <johannes.kalmbach@gmail.com>

#include <gmock/gmock.h>

#include <unordered_set>

#include "./util/GTestHelpers.h"
#include "engine/sparqlExpressions/SetOfIntervals.h"
#include "util/Random.h"

using namespace ad_utility;

// _____________________________________________________________________________
TEST(SetOfIntervals, Constructor) {
  SetOfIntervals s{{{0, 2}, {6, 12}}, 12};
  EXPECT_EQ(s.size(), 12);
  EXPECT_EQ(s._intervals, (SetOfIntervals::Vec{{0, 2}, {6, 12}}));
  // The empty set is valid for every size.
  EXPECT_EQ(SetOfIntervals({}, 0).size(), 0);
  EXPECT_EQ(SetOfIntervals({}, 7).size(), 7);
  // An interval must not end after the size of the set.
  AD_EXPECT_THROW_WITH_MESSAGE(
      SetOfIntervals({{0, 2}, {6, 13}}, 12),
      ::testing::HasSubstr("ends after the size of the set"));
}

// _____________________________________________________________________________
TEST(SetOfIntervals, Equality) {
  // Sets with the same intervals, but different sizes are different.
  EXPECT_EQ(SetOfIntervals({{0, 2}}, 4), SetOfIntervals({{0, 2}}, 4));
  EXPECT_NE(SetOfIntervals({{0, 2}}, 4), SetOfIntervals({{0, 2}}, 5));
  EXPECT_NE(SetOfIntervals({{0, 2}}, 4), SetOfIntervals({{0, 3}}, 4));
}

// _____________________________________________________________________________
TEST(SetOfIntervals, SortAndCheckDisjointAndNonempty) {
  // Sorted and valid set.
  SetOfIntervals s{{{0, 2}, {2, 5}, {6, 12}}, 12};
  auto t = SetOfIntervals::SortAndCheckDisjointAndNonempty(s);
  ASSERT_EQ(s, t);
  // Unsorted  and valid set.
  SetOfIntervals u{{{6, 12}, {0, 2}, {2, 5}}, 12};
  ASSERT_EQ(s, SetOfIntervals::SortAndCheckDisjointAndNonempty(u));

  // The empty set is valid.
  SetOfIntervals empty{{}, 12};
  ASSERT_EQ(empty, SetOfIntervals::SortAndCheckDisjointAndNonempty(empty));

  // Invalid set with empty interval.
  SetOfIntervals emptyInterval{{{4, 5}, {2, 2}}, 12};
  ASSERT_THROW(SetOfIntervals::SortAndCheckDisjointAndNonempty(emptyInterval),
               ad_utility::Exception);

  // Invalid set with overlapping intervals
  SetOfIntervals overlapping{{{4, 6}, {2, 5}}, 12};
  ASSERT_THROW(SetOfIntervals::SortAndCheckDisjointAndNonempty(overlapping),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(SetOfIntervals, CheckSortedAndDisjointAndSimplify) {
  SetOfIntervals nonOverlapping{{{0, 2}, {3, 5}, {6, 8}}, 10};
  ASSERT_EQ(nonOverlapping,
            SetOfIntervals::CheckSortedAndDisjointAndSimplify(nonOverlapping));
  SetOfIntervals overlapping{{{0, 2}, {2, 5}, {5, 8}}, 10};
  SetOfIntervals expected{{{0, 8}}, 10};
  ASSERT_EQ(expected,
            SetOfIntervals::CheckSortedAndDisjointAndSimplify(overlapping));

  {
    SetOfIntervals partiallyOverlapping{{{0, 2}, {3, 5}, {5, 7}}, 10};
    SetOfIntervals expected2{{{0, 2}, {3, 7}}, 10};
    ASSERT_EQ(expected2, SetOfIntervals::CheckSortedAndDisjointAndSimplify(
                             partiallyOverlapping));
  }

  SetOfIntervals unsorted{{{3, 5}, {0, 2}}, 10};

  // The size is kept, also for the empty set.
  SetOfIntervals empty{{}, 10};
  ASSERT_EQ(empty, SetOfIntervals::CheckSortedAndDisjointAndSimplify(empty));
  ASSERT_THROW(SetOfIntervals::CheckSortedAndDisjointAndSimplify(unsorted),
               ad_utility::Exception);
}

// _____________________________________________________________________________
using Union = SetOfIntervals::Union;
TEST(SetOfIntervals, Union) {
  SetOfIntervals s{{{4, 6}, {0, 2}, {10, 380}}, 500};
  SetOfIntervals empty{{}, 500};
  // Union with empty set leaves input unchanged
  ASSERT_EQ(Union{}(s, empty),
            SetOfIntervals::SortAndCheckDisjointAndNonempty(s));
  ASSERT_EQ(Union{}(empty, s),
            SetOfIntervals::SortAndCheckDisjointAndNonempty(s));

  SetOfIntervals nonOverlapping{{{2, 3}, {7, 10}, {400, 401}}, 500};
  SetOfIntervals expected{{{0, 3}, {4, 6}, {7, 380}, {400, 401}}, 500};
  ASSERT_EQ(Union{}(s, nonOverlapping), expected);
  ASSERT_EQ(Union{}(nonOverlapping, s), expected);

  {
    // Complete enclosing of two intervals.
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 9}}, 12};
    SetOfIntervals b{{{0, 6}, {8, 9}}, 12};
    SetOfIntervals c{{{0, 6}, {7, 9}}, 12};
    ASSERT_EQ(Union{}(a, b), c);
  }
  {
    // Complete enclosing of three
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 8}}, 12};
    SetOfIntervals b{{{0, 9}}, 12};
    ASSERT_EQ(Union{}(a, b), b);
  }

  {
    // Partial overlap
    SetOfIntervals a{{{2, 3}, {4, 6}, {7, 10}}, 12};
    SetOfIntervals b{{{0, 5}, {8, 11}}, 12};
    SetOfIntervals c{{{0, 6}, {7, 11}}, 12};
    ASSERT_EQ(Union{}(a, b), c);
  }

  // Both sets must refer to the same range.
  ASSERT_THROW(Union{}(SetOfIntervals{{{0, 2}}, 4}, SetOfIntervals{{}, 5}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
using Intersection = SetOfIntervals::Intersection;
TEST(SetOfIntervals, Intersection) {
  SetOfIntervals s{{{4, 6}, {0, 2}, {10, 380}}, 500};
  SetOfIntervals empty{{}, 500};
  // Union with empty set leaves input unchanged
  ASSERT_EQ(Intersection{}(s, empty), empty);
  ASSERT_EQ(Intersection{}(empty, s), empty);

  SetOfIntervals noOverlap{{{2, 3}, {7, 10}, {400, 401}}, 500};
  ASSERT_EQ(Intersection{}(s, noOverlap), empty);
  ASSERT_EQ(Intersection{}(noOverlap, s), empty);
  {
    // Complete enclosing of two
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 9}}, 12};
    SetOfIntervals b{{{0, 6}, {8, 10}}, 12};
    SetOfIntervals c{{{2, 3}, {4, 5}, {8, 9}}, 12};
    ASSERT_EQ(Intersection{}(a, b), c);
  }
  {
    // Complete enclosing of three
    SetOfIntervals a{{{2, 3}, {4, 5}, {7, 8}}, 12};
    SetOfIntervals b{{{0, 9}}, 12};
    ASSERT_EQ(Intersection{}(a, b), a);
  }

  {
    // Partial overlap
    SetOfIntervals a{{{2, 3}, {4, 6}, {7, 10}}, 12};
    SetOfIntervals b{{{0, 5}, {8, 11}}, 12};
    SetOfIntervals c{{{2, 3}, {4, 5}, {8, 10}}, 12};
    ASSERT_EQ(Intersection{}(a, b), c);
  }

  // Both sets must refer to the same range.
  ASSERT_THROW(
      Intersection{}(SetOfIntervals{{{0, 2}}, 4}, SetOfIntervals{{}, 5}),
      ad_utility::Exception);
}

// _____________________________________________________________________________
using Complement = SetOfIntervals::Complement;
TEST(SetOfIntervals, Complement) {
  SetOfIntervals a{{{2, 3}, {4, 6}, {7, 10}}, 12};
  SetOfIntervals expected{{{0, 2}, {3, 4}, {6, 7}, {10, 12}}, 12};
  ASSERT_EQ(Complement{}(a), expected);

  SetOfIntervals b{{{2, 3}, {3, 6}, {6, 10}}, 12};
  SetOfIntervals expected2{{{0, 2}, {10, 12}}, 12};
  ASSERT_EQ(Complement{}(b), expected2);

  // The complement lies within the range of the set, in particular the
  // complement of the empty set is the complete range (this was the cause of
  // https://github.com/ad-freiburg/qlever/issues/3559), and the complement of
  // the complete range is empty.
  ASSERT_EQ(Complement{}(SetOfIntervals{{}, 5}), (SetOfIntervals{{{0, 5}}, 5}));
  ASSERT_EQ(Complement{}(SetOfIntervals{{{0, 5}}, 5}), (SetOfIntervals{{}, 5}));
  ASSERT_EQ(Complement{}(SetOfIntervals{{}, 0}), (SetOfIntervals{{}, 0}));
  ASSERT_EQ(Complement{}(Complement{}(a)), a);
}

// _____________________________________________________________________________
TEST(SetOfIntervals, toBitVector) {
  SetOfIntervals a{{{2, 3}, {4, 6}, {7, 10}}, 200};
  std::unordered_set<size_t> elements{2, 4, 5, 7, 8, 9};
  auto expanded = SetOfIntervals::toBitVector(a);
  ASSERT_EQ(200ul, expanded.size());
  for (size_t i = 0; i < expanded.size(); ++i) {
    ASSERT_EQ(elements.contains(i), expanded[i]);
  }
}

// _____________________________________________________________________________
TEST(SetOfIntervals, toIdVector) {
  auto allocator = makeUnlimitedAllocator<Id>();

  SetOfIntervals intervals{{{1, 3}, {5, 6}}, 8};

  auto result = SetOfIntervals::toIdVector(intervals, allocator);

  VectorWithMemoryLimit<Id> expected{
      {Id::makeFromBool(false), Id::makeFromBool(true), Id::makeFromBool(true),
       Id::makeFromBool(false), Id::makeFromBool(false), Id::makeFromBool(true),
       Id::makeFromBool(false), Id::makeFromBool(false)},
      allocator};

  ASSERT_EQ(result, expected);
}
