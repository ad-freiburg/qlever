// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "util/GTestHelpers.h"
#include "util/views/ZipMergeUniqueView.h"

namespace {
template <typename T, typename Comp = std::less<>, typename Proj = ql::identity>
auto zipMerge(std::vector<T> first, std::vector<T> last, Comp comp = {},
              Proj proj = {}) {
  auto view = ad_utility::ZipMergeUniqueView{std::move(first), std::move(last),
                                             std::move(comp), std::move(proj)};
  return ::ranges::to<std::vector<T>>(view);
}
}  // namespace

TEST(ZipMergeUniqueViewTest, integerZip) {
  EXPECT_THAT(zipMerge<int>({}, {}), testing::ElementsAre());
  EXPECT_THAT(zipMerge<int>({1, 2, 3, 4}, {}),
              testing::ElementsAre(1, 2, 3, 4));
  EXPECT_THAT(zipMerge<int>({}, {1, 2, 3, 4}),
              testing::ElementsAre(1, 2, 3, 4));
  // Duplicated elements: the element from the second range wins.
  EXPECT_THAT(zipMerge<int>({1, 2, 3, 4}, {1, 2, 3, 4}),
              testing::ElementsAre(1, 2, 3, 4));
  EXPECT_THAT(zipMerge<int>({1, 3, 5}, {0, 2, 4}),
              testing::ElementsAre(0, 1, 2, 3, 4, 5));
  EXPECT_THAT(zipMerge<int>({1, 2, 3}, {4, 5, 6}),
              testing::ElementsAre(1, 2, 3, 4, 5, 6));
  // A custom comparator for differently sorted parts.
  EXPECT_THAT(zipMerge<int>({5, 3, 1}, {4, 2, 0}, std::greater{}),
              testing::ElementsAre(5, 4, 3, 2, 1, 0));
}

TEST(ZipMergeUniqueViewTest, intPairZip) {
  using Pair = std::pair<int, int>;
  EXPECT_THAT(
      zipMerge<Pair>({{1, 1}, {2, 1}, {3, 1}}, {{1, 2}, {2, 2}, {3, 2}}),
      testing::ElementsAre(Pair(1, 1), Pair(1, 2), Pair(2, 1), Pair(2, 2),
                           Pair(3, 1), Pair(3, 2)));
  // Under the projection both parts have elements 1, 2 and 3. The element from
  // the second range wins on equal keys.
  EXPECT_THAT(zipMerge<Pair>({{1, 1}, {2, 1}, {3, 1}}, {{1, 2}, {3, 2}}, {},
                             &Pair::first),
              testing::ElementsAre(Pair(1, 2), Pair(2, 1), Pair(3, 2)));
}

TEST(ZipMergeUniqueViewTest, MultiPass) {
  std::vector<int> a{1, 3, 5}, b{0, 2, 4};
  auto view = ad_utility::ZipMergeUniqueView{a, b};
  auto it = view.begin();
  auto itCopy = it;
  std::vector<int> r1, r2;
  while (it != view.end()) r1.push_back(*it++);
  while (itCopy != view.end()) r2.push_back(*itCopy++);
  EXPECT_THAT(r1, testing::ElementsAre(0, 1, 2, 3, 4, 5));
  EXPECT_THAT(r2, testing::ElementsAre(0, 1, 2, 3, 4, 5));
}

TEST(ZipMergeUniqueViewTest, Equality) {
  {
    std::vector<int> a, b;
    auto view = ad_utility::ZipMergeUniqueView{a, b};
    EXPECT_EQ(view.begin(), view.end());
  }
  {
    std::vector<int> a{1, 3, 5}, b{0, 2, 4};
    auto view = ad_utility::ZipMergeUniqueView{a, b};
    auto begin = view.begin();
    EXPECT_NE(begin, view.end());
    auto beginCopy = begin;
    EXPECT_EQ(begin, beginCopy);
    ++begin;
    EXPECT_NE(begin, beginCopy);
    ++beginCopy;
    EXPECT_EQ(begin, beginCopy);
  }
}
