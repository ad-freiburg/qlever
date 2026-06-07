// Copyright 2025 - 2026 The QLever Authors, in particular:
//
// 2025 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <list>
#include <utility>
#include <variant>
#include <vector>

#include "util/LruCache.h"

namespace {
// Copy a recency list `keys_` (MRU first) into a vector, for white-box
// assertions on the internal ordering. The friend test reads `cache.keys_` and
// passes it here.
std::vector<int> keysOrder(const std::list<int>& keys) {
  return std::vector<int>(keys.begin(), keys.end());
}
}  // namespace

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
  auto prevCapacity = cache.capacity();
  EXPECT_FALSE(cache.tryGet(42));
  EXPECT_EQ(cache.capacity(), prevCapacity);
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

// _____________________________________________________________________________
TEST(LRUCache, insertReturnsWhetherKeyWasNewAndUpdatesValue) {
  ad_utility::util::LRUCache<int, int> cache{2};

  EXPECT_TRUE(cache.insert(1, 10));
  EXPECT_TRUE(cache.insert(2, 20));
  EXPECT_FALSE(cache.insert(1, 100));

  auto value = cache.tryGet(1);
  ASSERT_TRUE(value);
  EXPECT_EQ(100, *value);
}

// _____________________________________________________________________________
TEST(LRUCache, duplicateInsertPromotesToMostRecentlyUsed) {
  ad_utility::util::LRUCache<int, int> cache{2};

  EXPECT_TRUE(cache.insert(1, 10));
  EXPECT_TRUE(cache.insert(2, 20));
  EXPECT_FALSE(cache.insert(1, 100));
  EXPECT_TRUE(cache.insert(3, 30));

  EXPECT_TRUE(cache.tryGet(1));
  EXPECT_FALSE(cache.tryGet(2));
  EXPECT_TRUE(cache.tryGet(3));
}

// _____________________________________________________________________________
TEST(LRUCache, insertWithoutValueWorksForEmptyValueTypes) {
  ad_utility::util::LRUCache<int, std::monostate> cache{2};

  EXPECT_TRUE(cache.insert(1));
  EXPECT_FALSE(cache.insert(1));
  EXPECT_TRUE(cache.insert(2));
  EXPECT_FALSE(cache.insert(1));
  EXPECT_TRUE(cache.insert(3));

  EXPECT_TRUE(cache.tryGet(1));
  EXPECT_FALSE(cache.tryGet(2));
  EXPECT_TRUE(cache.tryGet(3));
}

// White-box tests for the private helpers. These live in the same namespace as
// `LRUCache` so the `FRIEND_TEST` friend declarations (which name the test
// classes in `ad_utility::util`) actually grant access to the private members.
namespace ad_utility::util {

// _____________________________________________________________________________
// `markMRU` moves the given list node to the front of `keys_` and leaves all
// other nodes (and `cache_`) untouched.
TEST(LRUCacheWhiteBox, markMRUReordersKeysList) {
  LRUCache<int, int> cache{3};
  cache.insert(1, 10);
  cache.insert(2, 20);
  cache.insert(3, 30);
  // After inserting 1, 2, 3 the recency list is 3, 2, 1 (MRU first).
  ASSERT_EQ((std::vector<int>{3, 2, 1}), keysOrder(cache.keys_));

  // Promote key 1 (currently LRU) to the front via its stored bookmark.
  auto bookmarkOfKey1 = cache.cache_.find(1)->second.second;
  cache.markMRU(bookmarkOfKey1);

  EXPECT_EQ((std::vector<int>{1, 3, 2}), keysOrder(cache.keys_));
  // `cache_` is unchanged in size and the bookmarks still resolve.
  EXPECT_EQ(3u, cache.cache_.size());
  EXPECT_EQ(10, cache.cache_.find(1)->second.first);
}

// _____________________________________________________________________________
// When the cache is below capacity, `evictLRUIfFullAndMarkMRU` simply pushes
// the new key to the front of `keys_` and evicts nothing. It does NOT insert a
// `cache_` entry (that is the caller's responsibility).
TEST(LRUCacheWhiteBox, evictLRUIfFullPushesFrontWhenNotFull) {
  LRUCache<int, int> cache{3};
  cache.insert(1, 10);  // keys_: {1}, cache_: {1}

  cache.evictLRUIfFullAndMarkMRU(2);

  EXPECT_EQ((std::vector<int>{2, 1}), keysOrder(cache.keys_));
  // No `cache_` entry was created for key 2 by the helper.
  EXPECT_FALSE(cache.cache_.contains(2));
  EXPECT_TRUE(cache.cache_.contains(1));
  EXPECT_EQ(1u, cache.cache_.size());
}

// _____________________________________________________________________________
// When the cache is full, `evictLRUIfFullAndMarkMRU` evicts the LRU key from
// both `keys_` and `cache_`, recycles its list node to the front, and seats the
// new key there. The new key's `cache_` entry is still left to the caller.
TEST(LRUCacheWhiteBox, evictLRUIfFullEvictsAndRecyclesWhenFull) {
  LRUCache<int, int> cache{2};
  cache.insert(1, 10);
  cache.insert(2, 20);  // keys_: {2, 1}; key 1 is LRU.

  cache.evictLRUIfFullAndMarkMRU(3);

  // The recycled node now holds key 3 at the front; key 1 was evicted.
  EXPECT_EQ((std::vector<int>{3, 2}), keysOrder(cache.keys_));
  // Key 1 is gone from `cache_`; key 2 remains; key 3 is not inserted yet.
  EXPECT_FALSE(cache.cache_.contains(1));
  EXPECT_TRUE(cache.cache_.contains(2));
  EXPECT_FALSE(cache.cache_.contains(3));
  EXPECT_EQ(1u, cache.cache_.size());
}

// _____________________________________________________________________________
// `insertNewEntry` seats the key at the front BEFORE invoking the value factory
// (so the factory may read `keys_.front()`), stores the produced value, and
// returns a reference to the stored value.
TEST(LRUCacheWhiteBox, insertNewEntrySeatsKeyBeforeComputingValue) {
  LRUCache<int, int> cache{2};

  bool factoryCalled = false;
  const int& stored = cache.insertNewEntry(7, [&] {
    factoryCalled = true;
    // The key must already be seated at the front when the factory runs.
    EXPECT_EQ(7, cache.keys_.front());
    return cache.keys_.front() * 10;
  });

  EXPECT_TRUE(factoryCalled);
  EXPECT_EQ(70, stored);
  // The returned reference aliases the stored value.
  EXPECT_EQ(&stored, &cache.cache_.find(7)->second.first);
  EXPECT_EQ((std::vector<int>{7}), keysOrder(cache.keys_));
}

}  // namespace ad_utility::util
