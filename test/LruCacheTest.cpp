//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/LruCache.h"

TEST(LRUCache, testLruCache) {
  ad_utility::util::LRUCache<int, int> cache{2};
  // Type-erase the lambdas to get a better coverage report.
  using F = std::function<int(int)>;

  EXPECT_EQ(1, (cache.getOrCompute<int, F>(1, [](int i) {
              EXPECT_EQ(i, 1);
              return 1;
            })));
  EXPECT_EQ(2, (cache.getOrCompute<int, F>(2, [](int i) {
              EXPECT_EQ(i, 2);
              return 2;
            })));

  EXPECT_EQ(1, (cache.getOrCompute<int, F>(1, [](int) {
              ADD_FAILURE();
              return 0;
            })));
  EXPECT_EQ(3, (cache.getOrCompute<int, F>(3, [](int i) {
              EXPECT_EQ(i, 3);
              return 3;
            })));
  EXPECT_EQ(20, (cache.getOrCompute<int, F>(2, [](int i) {
              EXPECT_EQ(i, 2);
              return 20;
            })));
  EXPECT_EQ(10, (cache.getOrCompute<int, F>(1, [](int i) {
              EXPECT_EQ(i, 1);
              return 10;
            })));
}

// _____________________________________________________________________________
TEST(LRUCache, testEmptyCapacityForbidden) {
  EXPECT_THROW((ad_utility::util::LRUCache<int, int>{0}),
               ad_utility::Exception);
}

// _____________________________________________________________________________
TEST(LRUCache, tryGetReturnsNoneOnMiss) {
  ad_utility::util::LRUCache<int, int> cache{2};
  EXPECT_FALSE(cache.tryGet(42));
}

// _____________________________________________________________________________
TEST(LRUCache, tryGetReturnsValueOnHit) {
  ad_utility::util::LRUCache<int, int> cache{2};
  cache.getOrCompute(7, [](int k) { return k * 10; });
  auto v = cache.tryGet(7);
  ASSERT_TRUE(v);
  EXPECT_EQ(70, *v);
}

// _____________________________________________________________________________
// After a `tryGet` hit, the accessed key becomes MRU and must survive the next
// eviction.
TEST(LRUCache, tryGetPromotesToMostRecentlyUsed) {
  ad_utility::util::LRUCache<int, int> cache{2};
  cache.getOrCompute(1, [](int k) { return k; });
  cache.getOrCompute(2, [](int k) { return k; });
  // Key 2 is LRU; promote key 2 via tryGet so key 1 becomes LRU.
  ASSERT_TRUE(cache.tryGet(2));
  // Inserting key 3 must evict key 1, not key 2.
  cache.getOrCompute(3, [](int k) { return k; });
  EXPECT_TRUE(cache.tryGet(2));   // still present
  EXPECT_FALSE(cache.tryGet(1));  // evicted
}

// _____________________________________________________________________________
// `tryGet` on a missing key must not insert anything; a subsequent
// `getOrCompute` for the same key must still call the compute function.
TEST(LRUCache, tryGetDoesNotInsert) {
  ad_utility::util::LRUCache<int, int> cache{2};
  EXPECT_FALSE(cache.tryGet(99));
  int computeCount = 0;
  cache.getOrCompute(99, [&computeCount](int k) {
    ++computeCount;
    return k;
  });
  EXPECT_EQ(1, computeCount);
}
