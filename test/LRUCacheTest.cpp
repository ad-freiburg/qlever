// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>
#include <string>
#include "../src/util/LRUCache.h"


using std::string;

namespace ad_utility {
// _____________________________________________________________________________
TEST(LRUCacheTest, testTypicalusage) {
  LRUCache<string, string> cache(5);
  cache.insert("1", "x");
  cache.insert("2", "xx");
  cache.insert("3", "xxx");
  cache.insert("4", "xxxx");
  cache.insert("5", "xxxxx");

  ASSERT_EQ(*cache["1"], "x");
  ASSERT_EQ(*cache["2"], "xx");
  ASSERT_EQ(*cache["3"], "xxx");
  ASSERT_EQ(*cache["4"], "xxxx");
  ASSERT_EQ(*cache["5"], "xxxxx");
  // Non-existing elements must yield shared_ptr<const Value>(nullptr)
  // this bool converts to false
  ASSERT_FALSE(cache["non-existant"]);
  // insert and immediate result retrieval
  ASSERT_EQ(*cache.insert("new", "newvalue"), "newvalue");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, testIncreasingCapacity) {
  LRUCache<string, string> cache(5);
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
  cache.setCapacity(10);
  ASSERT_EQ(*cache["3"], "x");
  cache.insert("3", "xxxx");
  ASSERT_EQ(*cache["3"], "xxxx");
  ASSERT_EQ(*cache["5"], "x");
  cache.insert("0", "xxxx");
  ASSERT_EQ(*cache["0"], "xxxx");
  ASSERT_EQ(*cache["4"], "x");
  ASSERT_EQ(*cache["5"], "x");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, testDecreasingCapacity) {
  LRUCache<string, string> cache(10);
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
  cache.setCapacity(5);
  ASSERT_EQ(*cache["9"], "x");
  ASSERT_EQ(*cache["10"], "x");
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
