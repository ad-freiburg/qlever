//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/LruCache.h"

TEST(LRUCache, testLruCache) {
  ad_utility::util::LRUCache<int, int> cache{2};
  // Type-erase the lambdas to get a better coverage report.
  using F = std::function<int(int)>;

  EXPECT_EQ(1, cache.getOrCompute<F>(1, [](int i) {
    EXPECT_EQ(i, 1);
    return 1;
  }));
  EXPECT_EQ(2, cache.getOrCompute<F>(2, [](int i) {
    EXPECT_EQ(i, 2);
    return 2;
  }));

  EXPECT_EQ(1, cache.getOrCompute<F>(1, [](int) {
    ADD_FAILURE();
    return 0;
  }));
  EXPECT_EQ(3, cache.getOrCompute<F>(3, [](int i) {
    EXPECT_EQ(i, 3);
    return 3;
  }));
  EXPECT_EQ(20, cache.getOrCompute<F>(2, [](int i) {
    EXPECT_EQ(i, 2);
    return 20;
  }));
  EXPECT_EQ(10, cache.getOrCompute<F>(1, [](int i) {
    EXPECT_EQ(i, 1);
    return 10;
  }));
}

// _____________________________________________________________________________
TEST(LRUCache, testEmptyCapacityForbidden) {
  EXPECT_THROW((ad_utility::util::LRUCache<int, int>{0}),
               ad_utility::Exception);
}
