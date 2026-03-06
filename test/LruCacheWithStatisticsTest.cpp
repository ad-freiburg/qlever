//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "util/LruCacheWithStatistics.h"

using ad_utility::util::LRUCacheWithStatistics;

TEST(LRUCacheWithStatistics, hitsAndMissesAreCountedCorrectly) {
  LRUCacheWithStatistics<int, int> cache{2};
  using F = std::function<int(int)>;

  // First lookup is a miss.
  cache.getOrCompute<F>(1, [](int i) { return i * 10; });
  EXPECT_EQ(cache.stats().hits_, 0);
  EXPECT_EQ(cache.stats().misses_, 1);

  // Second lookup for same key is a hit.
  cache.getOrCompute<F>(1, [](int) {
    ADD_FAILURE();
    return 0;
  });
  EXPECT_EQ(cache.stats().hits_, 1);
  EXPECT_EQ(cache.stats().misses_, 1);

  // New key is a miss.
  cache.getOrCompute<F>(2, [](int i) { return i * 10; });
  EXPECT_EQ(cache.stats().hits_, 1);
  EXPECT_EQ(cache.stats().misses_, 2);
}

// _____________________________________________________________________________
TEST(LRUCacheWithStatistics, totalLookupsAndHitRate) {
  LRUCacheWithStatistics<int, int> cache{4};
  using F = std::function<int(int)>;

  // Insert 4 elements (4 misses).
  for (int i = 0; i < 4; ++i) {
    cache.getOrCompute<F>(i, [](int k) { return k; });
  }
  // Hit all 4 (4 hits).
  for (int i = 0; i < 4; ++i) {
    cache.getOrCompute<F>(i, [](int) {
      ADD_FAILURE();
      return 0;
    });
  }

  EXPECT_EQ(cache.stats().totalLookups(), 8);
  EXPECT_EQ(cache.stats().hits_, 4);
  EXPECT_EQ(cache.stats().misses_, 4);
  EXPECT_DOUBLE_EQ(cache.stats().hitRate(), 0.5);
}

// _____________________________________________________________________________
TEST(LRUCacheWithStatistics, hitRateIsZeroForEmptyStats) {
  LRUCacheWithStatistics<int, int> cache{2};

  EXPECT_EQ(cache.stats().totalLookups(), 0);
  EXPECT_DOUBLE_EQ(cache.stats().hitRate(), 0.0);
}

// _____________________________________________________________________________
TEST(LRUCacheWithStatistics, capacityForwardsCorrectly) {
  LRUCacheWithStatistics<int, int> cache{42};
  EXPECT_EQ(cache.capacity(), 42);
}

// _____________________________________________________________________________
TEST(LRUCacheWithStatistics, tryGetHitIncrementsHitCounter) {
  LRUCacheWithStatistics<int, int> cache{2};
  using F = std::function<int(int)>;
  cache.getOrCompute<F>(1, [](int k) { return k * 10; });
  EXPECT_EQ(cache.stats().hits_, 0);
  EXPECT_EQ(cache.stats().misses_, 1);

  auto v = cache.tryGet(1);
  ASSERT_TRUE(v);
  EXPECT_EQ(10, *v);
  EXPECT_EQ(cache.stats().hits_, 1);
  EXPECT_EQ(cache.stats().misses_, 1);
}

// _____________________________________________________________________________
// A `tryGet` miss must not increment the miss counter; misses are only counted
// when `getOrCompute` triggers the compute function.
TEST(LRUCacheWithStatistics, tryGetMissDoesNotIncrementMissCounter) {
  LRUCacheWithStatistics<int, int> cache{2};
  EXPECT_FALSE(cache.tryGet(99));
  EXPECT_EQ(cache.stats().hits_, 0);
  EXPECT_EQ(cache.stats().misses_, 0);
}

// _____________________________________________________________________________
// Verify correct hit/miss counts when `tryGet` and `getOrCompute` are
// interleaved.
TEST(LRUCacheWithStatistics, tryGetInterleavedWithGetOrCompute) {
  LRUCacheWithStatistics<int, int> cache{2};
  using F = std::function<int(int)>;

  cache.getOrCompute<F>(1, [](int k) { return k * 10; });  // miss
  cache.getOrCompute<F>(2, [](int k) { return k * 10; });  // miss
  EXPECT_EQ(cache.stats().misses_, 2);
  EXPECT_EQ(cache.stats().hits_, 0);

  ASSERT_TRUE(cache.tryGet(1));  // hit via tryGet
  EXPECT_EQ(cache.stats().hits_, 1);
  EXPECT_EQ(cache.stats().misses_, 2);

  cache.getOrCompute<F>(2, [](int) {  // hit via getOrCompute
    ADD_FAILURE();
    return 0;
  });
  EXPECT_EQ(cache.stats().hits_, 2);
  EXPECT_EQ(cache.stats().misses_, 2);
}

// _____________________________________________________________________________
TEST(LRUCacheWithStatistics, lruEvictionWorksThrough) {
  LRUCacheWithStatistics<int, int> cache{2};
  using F = std::function<int(int)>;

  // Fill cache: {1, 2}
  cache.getOrCompute<F>(1, [](int i) { return i; });
  cache.getOrCompute<F>(2, [](int i) { return i; });

  // Access key 1 to make it most recently used.
  cache.getOrCompute<F>(1, [](int) {
    ADD_FAILURE();
    return 0;
  });

  // Insert key 3, which should evict key 2 (least recently used).
  cache.getOrCompute<F>(3, [](int i) { return i * 100; });

  // Key 2 should have been evicted, so this is a miss and recomputes.
  int recomputed = cache.getOrCompute<F>(2, [](int i) { return i * 100; });
  EXPECT_EQ(recomputed, 200);

  EXPECT_EQ(cache.stats().misses_, 4);  // keys 1, 2, 3, 2-again
  EXPECT_EQ(cache.stats().hits_, 1);    // key 1 was inserted and had 1 cache
  // hit.
}
