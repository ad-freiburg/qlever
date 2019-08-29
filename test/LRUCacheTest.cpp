// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include "../src/util/LRUCache.h"

using std::string;

namespace ad_utility {

// _____________________________________________________________________________
TEST(LRUCacheTest, testTypicalUsage) {
  LRUCache<string, string> cache(sizeof(string) * 3 + 3 + 4 + 5);
  cache.insert("1", "x");
  cache.insert("2", "xx");
  cache.insert("3", "xxx");
  cache.insert("4", "xxxx");
  cache.insert("5", "xxxxx");

  ASSERT_EQ(cache.itemsMemorySize(), 3 * sizeof(string) + 3 + 4 + 5);

  ASSERT_FALSE(cache["1"]);  // oldest dropped
  ASSERT_FALSE(cache["2"]);  // second oldest dropped
  ASSERT_EQ(*cache["3"], "xxx");
  ASSERT_EQ(*cache["4"], "xxxx");
  ASSERT_EQ(*cache["5"], "xxxxx");
  // Non-existing elements must yield shared_ptr<const Value>(nullptr)
  // this bool converts to false
  ASSERT_FALSE(cache["non-existant"]);
}
// _____________________________________________________________________________
TEST(LRUCacheTest, testMapUsage) {
  LRUCache<string, string> cache(sizeof(string) * 5 + 1 + 2 + 3 + 4 + 5);
  cache.insert("1", "x");
  cache.insert("2", "xx");
  cache.insert("3", "xxx");
  cache.insert("4", "xxxx");
  cache.insert("5", "xxxxx");

  ASSERT_EQ(cache.itemsMemorySize(), sizeof(string) * 5 + 1 + 2 + 3 + 4 + 5);

  ASSERT_EQ(*cache["1"], "x");
  ASSERT_EQ(*cache["2"], "xx");
  ASSERT_EQ(*cache["3"], "xxx");
  ASSERT_EQ(*cache["4"], "xxxx");
  ASSERT_EQ(*cache["5"], "xxxxx");
  // Non-existing elements must yield shared_ptr<const Value>(nullptr)
  // this bool converts to false
  ASSERT_FALSE(cache["non-existant"]);
}
// _____________________________________________________________________________
TEST(LRUCacheTest, testTryEmplace) {
  LRUCache<string, string> cache(sizeof(string) * 5 + 1 + 2);
  cache.insert("1", "x");
  cache.insert("2", "xx");
  // tryEmplace returns a pair of shared_ptr where the first is non-const and
  // only non-null if it was freshly inserted. That in turn converts to false
  // in bool context
  ASSERT_FALSE(cache.tryEmplace("2", "foo").first);
  ASSERT_EQ(*cache.tryEmplace("4", "bar").first, "bar");
  auto emplaced = cache.tryEmplace("5", "foo").first;
  ASSERT_TRUE(emplaced);
  *emplaced += "bar";
  ASSERT_EQ(*cache["5"], "foobar");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, testIncreasingCapacity) {
  LRUCache<string, string> cache(sizeof(string) * 5 + 5);
  cache.insert("1", "x");
  cache.insert("2", "x");
  cache.insert("3", "x");
  cache.insert("4", "x");
  cache.insert("5", "x");

  ASSERT_EQ(*cache["1"], "x");
  ASSERT_EQ(*cache["2"], "x");
  ASSERT_EQ(*cache["3"], "x");
  ASSERT_EQ(*cache["4"], "x");
  ASSERT_EQ(*cache["5"], "x");
  ASSERT_EQ(*cache["3"], "x");
  cache.insert("3", "xxxx");
  ASSERT_EQ(*cache["3"], "xxxx");
  ASSERT_EQ(*cache["5"], "x");
  cache.insert("0", "xxxx");
  ASSERT_EQ(*cache["0"], "xxxx");
  // 4 should be dropped as it wasn't used for a while
  ASSERT_FALSE(cache["4"]);
  // 5 was recently used and should be available
  ASSERT_EQ(*cache["5"], "x");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, testDecreasingCapacity) {
  LRUCache<string, string> cache(sizeof(string) * 10 + 10);
  cache.insert("1", "x");
  cache.insert("2", "x");
  cache.insert("3", "x");
  cache.insert("4", "x");
  cache.insert("5", "x");
  ASSERT_EQ(*cache["1"], "x");
  ASSERT_EQ(*cache["2"], "x");
  ASSERT_EQ(*cache["3"], "x");
  ASSERT_EQ(*cache["4"], "x");
  ASSERT_EQ(*cache["5"], "x");
  cache.insert("9", "x");
  cache.insert("10", "x");
  cache.setCapacity(5 * sizeof(string));
  ASSERT_EQ(*cache["9"], "x");
  ASSERT_EQ(*cache["10"], "x");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, testVectorUsage) {
  using vec = std::vector<size_t>;
  LRUCache<string, vec> cache(3 * sizeof(vec) + (3 + 4 + 5) * sizeof(size_t));
  cache.insert("1", vec({1}));
  cache.insert("2", vec({2, 2}));
  cache.insert("3", vec({3, 3, 3}));
  cache.insert("4", vec({4, 4, 4, 4}));
  cache.insert("5", vec({5, 5, 5, 5, 5}));

  // ASSERT_EQ(cache.itemsMemorySize(),
  //          3 * sizeof(vec) + (3 + 4 + 5) * sizeof(size_t));

  ASSERT_FALSE(cache["1"]);  // oldest dropped
  ASSERT_FALSE(cache["2"]);  // second oldest dropped
  ASSERT_EQ(*cache["3"], vec({3, 3, 3}));
  ASSERT_EQ(*cache["4"], vec({4, 4, 4, 4}));
  ASSERT_EQ(*cache["5"], vec({5, 5, 5, 5, 5}));
}

}  // namespace ad_utility

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
