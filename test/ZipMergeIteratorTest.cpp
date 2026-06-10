// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/GTestHelpers.h"
#include "util/ZipMergeIterator.h"

namespace {
template <typename T, typename Comp = std::less<>, typename Proj = ql::identity>
auto zipMerge(std::vector<T> first, std::vector<T> last, Comp comp = {},
              Proj proj = {}) {
  return std::vector(
      ad_utility::zipIterator(first.begin(), first.end(), last.begin(),
                              last.end(), comp, proj),
      ad_utility::zipIterator(first.end(), first.end(), last.end(), last.end(),
                              comp, proj));
}
}  // namespace

TEST(ZipMergeIteratorTest, integerZip) {
  EXPECT_THAT(zipMerge<int>({}, {}), testing::ElementsAre());
  EXPECT_THAT(zipMerge<int>({1, 2, 3, 4}, {}),
              testing::ElementsAre(1, 2, 3, 4));
  EXPECT_THAT(zipMerge<int>({}, {1, 2, 3, 4}),
              testing::ElementsAre(1, 2, 3, 4));
  // Duplicated elements, but we can't check which of the duplicates was chosen.
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

TEST(ZipMergeIteratorTest, intPairZip) {
  using Pair = std::pair<int, int>;
  EXPECT_THAT(
      zipMerge<Pair>({{1, 1}, {2, 1}, {3, 1}}, {{1, 2}, {2, 2}, {3, 2}}),
      testing::ElementsAre(Pair(1, 1), Pair(1, 2), Pair(2, 1), Pair(2, 2),
                           Pair(3, 1), Pair(3, 2)));
  // Under the projection both parts have the elements 1, 2 and 3. Check that
  // the elements from the second part override the first part.
  EXPECT_THAT(zipMerge<Pair>({{1, 1}, {2, 1}, {3, 1}}, {{1, 2}, {3, 2}}, {},
                             &Pair::first),
              testing::ElementsAre(Pair(1, 2), Pair(2, 1), Pair(3, 2)));
}
