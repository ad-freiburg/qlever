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

using ad_utility::zipIterator;
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

// Helper: collect elements from a ZipMergeIteratorImpl range into a vector.
template <typename It>
auto collect(It begin, It end) {
  std::vector<typename std::iterator_traits<It>::value_type> result;
  for (; begin != end; ++begin) {
    result.push_back(*begin);
  }
  return result;
}

// Helper: create a ZipMergeIteratorImpl pair (begin, end) from two vectors.
template <typename T, typename Compare = std::less<>,
          typename Projection = ql::identity>
auto makeZipRange(std::vector<T>& a, std::vector<T>& b, Compare cmp = {},
                  Projection proj = {}) {
  auto begin =
      zipIterator.operator()(a.begin(), a.end(), b.begin(), b.end(), cmp, proj);
  auto end =
      zipIterator.operator()(a.end(), a.end(), b.end(), b.end(), cmp, proj);
  return std::pair{begin, end};
}

}  // namespace

// ---------- ZipMergeIteratorImpl tests ----------

// Iterator traits.
TEST(ZipMergeIterator, IsInputIterator) {
  using It = decltype(zipIterator.operator()(
      std::declval<std::vector<int>::iterator>(),
      std::declval<std::vector<int>::iterator>(),
      std::declval<std::vector<int>::iterator>(),
      std::declval<std::vector<int>::iterator>()));
  static_assert(std::is_same_v<It::iterator_category, std::input_iterator_tag>);
  static_assert(std::is_same_v<It::iterator_concept, std::input_iterator_tag>);
  static_assert(std::is_same_v<It::value_type, int>);
}

// Both ranges empty.
TEST(ZipMergeIterator, BothEmpty) {
  std::vector<int> a{}, b{};
  auto [begin, end] = makeZipRange(a, b);
  EXPECT_THAT(collect(begin, end), IsEmpty());
}

// First range empty.
TEST(ZipMergeIterator, FirstEmpty) {
  std::vector<int> a{}, b{1, 3, 5};
  auto [begin, end] = makeZipRange(a, b);
  EXPECT_THAT(collect(begin, end), ElementsAre(1, 3, 5));
}

// Second range empty.
TEST(ZipMergeIterator, SecondEmpty) {
  std::vector<int> a{2, 4, 6}, b{};
  auto [begin, end] = makeZipRange(a, b);
  EXPECT_THAT(collect(begin, end), ElementsAre(2, 4, 6));
}

// Non-overlapping, first all smaller.
TEST(ZipMergeIterator, NonOverlapping) {
  std::vector<int> a{1, 2, 3}, b{4, 5, 6};
  auto [begin, end] = makeZipRange(a, b);
  EXPECT_THAT(collect(begin, end), ElementsAre(1, 2, 3, 4, 5, 6));
}

// Interleaved elements.
TEST(ZipMergeIterator, Interleaved) {
  std::vector<int> a{1, 3, 5}, b{2, 4, 6};
  auto [begin, end] = makeZipRange(a, b);
  EXPECT_THAT(collect(begin, end), ElementsAre(1, 2, 3, 4, 5, 6));
}

// Equal elements: second wins.
TEST(ZipMergeIterator, EqualElementsSecondWins) {
  std::vector<int> a{1, 2, 3}, b{1, 2, 3};
  auto [begin, end] = makeZipRange(a, b);
  EXPECT_THAT(collect(begin, end), ElementsAre(1, 2, 3));
}

// Partial overlap.
TEST(ZipMergeIterator, PartialOverlap) {
  std::vector<int> a{1, 3, 5}, b{3, 5, 7};
  auto [begin, end] = makeZipRange(a, b);
  EXPECT_THAT(collect(begin, end), ElementsAre(1, 3, 5, 7));
}

// Custom comparator: descending order.
TEST(ZipMergeIterator, CustomComparator) {
  std::vector<int> a{5, 3, 1}, b{6, 4, 2};
  auto [begin, end] = makeZipRange(a, b, std::greater<int>{});
  EXPECT_THAT(collect(begin, end), ElementsAre(6, 5, 4, 3, 2, 1));
}

// Custom projection: compare pairs by .second.
TEST(ZipMergeIterator, CustomProjection) {
  using P = std::pair<int, int>;
  std::vector<P> a{{10, 1}, {20, 3}, {30, 5}};
  std::vector<P> b{{40, 2}, {50, 4}, {60, 6}};
  auto proj = [](const P& p) { return p.second; };
  auto [begin, end] = makeZipRange(a, b, std::less<int>{}, proj);
  EXPECT_THAT(collect(begin, end), ElementsAre(P{10, 1}, P{40, 2}, P{20, 3},
                                               P{50, 4}, P{30, 5}, P{60, 6}));
}

// Custom projection with equal values: second wins.
TEST(ZipMergeIterator, CustomProjectionEqualSecondWins) {
  using P = std::pair<int, int>;
  std::vector<P> a{{1, 2}};
  std::vector<P> b{{99, 2}};
  auto proj = [](const P& p) { return p.second; };
  auto [begin, end] = makeZipRange(a, b, std::less<int>{}, proj);
  auto result = collect(begin, end);
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0], P(99, 2));
}

// Post-increment returns the old iterator state.
TEST(ZipMergeIterator, PostIncrement) {
  std::vector<int> a{1, 3}, b{2};
  auto [begin, end] = makeZipRange(a, b);
  auto old = begin++;
  EXPECT_EQ(*old, 1);
  EXPECT_EQ(*begin, 2);
}

// Arrow operator returns a pointer to the current element.
TEST(ZipMergeIterator, ArrowOperator) {
  using P = std::pair<int, int>;
  std::vector<P> a{{10, 1}}, b{{20, 2}};
  auto proj = [](const P& p) { return p.second; };
  auto [begin, end] = makeZipRange(a, b, std::less<int>{}, proj);
  EXPECT_EQ(begin->first, 10);
  EXPECT_EQ(begin->second, 1);
}

// Equality: begin equals end when both ranges are empty; differs otherwise.
TEST(ZipMergeIterator, Equality) {
  std::vector<int> empty1{}, empty2{};
  auto [b1, e1] = makeZipRange(empty1, empty2);
  EXPECT_EQ(b1, e1);

  std::vector<int> a{1}, b{2};
  auto [b2, e2] = makeZipRange(a, b);
  EXPECT_NE(b2, e2);
}
