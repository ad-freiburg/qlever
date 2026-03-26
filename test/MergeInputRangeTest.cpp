// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "util/MergeInputRange.h"

using ad_utility::mergeInputRange;
using testing::ElementsAre;
using testing::IsEmpty;

namespace {

// Helper: collect all elements from a MergeInputRange into a vector.
template <typename Range>
auto toVector(Range&& r) {
  std::vector<std::decay_t<ql::ranges::range_reference_t<Range>>> result;
  for (auto& el : r) {
    result.push_back(el);
  }
  return result;
}

}  // namespace

// Both ranges empty.
TEST(MergeInputRange, BothEmpty) {
  std::vector<int> a{}, b{};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), IsEmpty());
}

// First range empty: output equals second range.
TEST(MergeInputRange, FirstEmpty) {
  std::vector<int> a{}, b{1, 3, 5};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(1, 3, 5));
}

// Second range empty: output equals first range.
TEST(MergeInputRange, SecondEmpty) {
  std::vector<int> a{2, 4, 6}, b{};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(2, 4, 6));
}

// Non-overlapping ranges, first all smaller.
TEST(MergeInputRange, NonOverlapping) {
  std::vector<int> a{1, 2, 3}, b{4, 5, 6};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(1, 2, 3, 4, 5, 6));
}

// Non-overlapping ranges, second all smaller.
TEST(MergeInputRange, NonOverlappingReversed) {
  std::vector<int> a{4, 5, 6}, b{1, 2, 3};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(1, 2, 3, 4, 5, 6));
}

// Interleaved elements.
TEST(MergeInputRange, Interleaved) {
  std::vector<int> a{1, 3, 5}, b{2, 4, 6};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(1, 2, 3, 4, 5, 6));
}

// Equal elements: only the element from the second range is yielded.
TEST(MergeInputRange, EqualElementsSecondWins) {
  std::vector<int> a{1, 2, 3}, b{1, 2, 3};
  // Each equal pair: second wins, first is skipped. Result is one copy each.
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(1, 2, 3));
}

// Partial overlap: some equal, some unique.
TEST(MergeInputRange, PartialOverlap) {
  std::vector<int> a{1, 3, 5}, b{3, 5, 7};
  // 1 only in a; 3 equal (second wins); 5 equal (second wins); 7 only in b.
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(1, 3, 5, 7));
}

// First range much longer.
TEST(MergeInputRange, FirstLonger) {
  std::vector<int> a{1, 2, 3, 4, 5, 6, 7, 8}, b{4, 6};
  // 4 and 6 are equal → second wins; all others from a.
  EXPECT_THAT(toVector(mergeInputRange(a, b)),
              ElementsAre(1, 2, 3, 4, 5, 6, 7, 8));
}

// Custom comparator: descending order.
TEST(MergeInputRange, CustomComparator) {
  std::vector<int> a{5, 3, 1}, b{6, 4, 2};
  EXPECT_THAT(toVector(mergeInputRange(a, b, std::greater<int>{})),
              ElementsAre(6, 5, 4, 3, 2, 1));
}

// Custom projection: compare pairs by second element only.
TEST(MergeInputRange, CustomProjection) {
  using P = std::pair<int, int>;
  std::vector<P> a{{10, 1}, {20, 3}, {30, 5}};
  std::vector<P> b{{40, 2}, {50, 4}, {60, 6}};
  auto proj = [](const P& p) { return p.second; };
  auto result = toVector(mergeInputRange(a, b, std::less<int>{}, proj));
  ASSERT_EQ(result.size(), 6u);
  EXPECT_THAT(result, ElementsAre(P{10, 1}, P{40, 2}, P{20, 3}, P{50, 4},
                                  P{30, 5}, P{60, 6}));
}

// Custom projection with equal values: second wins.
TEST(MergeInputRange, CustomProjectionEqualSecondWins) {
  using P = std::pair<int, int>;
  // Both ranges have an element with second==2; the one from b should win.
  std::vector<P> a{{1, 2}};
  std::vector<P> b{{99, 2}};
  auto proj = [](const P& p) { return p.second; };
  auto result = toVector(mergeInputRange(a, b, std::less<int>{}, proj));
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], P(99, 2));
}

// Single element ranges, no overlap.
TEST(MergeInputRange, SingleElements) {
  std::vector<int> a{1}, b{2};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(1, 2));
}

// Single element ranges, equal.
TEST(MergeInputRange, SingleElementsEqual) {
  std::vector<int> a{42}, b{42};
  EXPECT_THAT(toVector(mergeInputRange(a, b)), ElementsAre(42));
}

// Result is an input range (not forward/bidirectional).
TEST(MergeInputRange, IsInputRange) {
  std::vector<int> a{1}, b{2};
  auto r = mergeInputRange(a, b);
  static_assert(ql::ranges::input_range<decltype(r)>);
  static_assert(!ql::ranges::forward_range<decltype(r)>);
}
