// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <string>

#include "../src/util/Cache.h"

using std::string;

// first some simple Tests for the general cache interface
TEST(FlexibleCacheTest, Simple) {
  auto accessUpdater = [](const auto& s, [[maybe_unused]] const auto& v) {
    return s;
  };
  auto scoreCalculator = [](const auto& v) { return v; };
  auto scoreComparator = std::less<>();
  ad_utility::HeapBasedCache<string, int, int, decltype(scoreComparator),
                             decltype(accessUpdater), decltype(scoreCalculator)>
      cache{3, 10000, 10000, scoreComparator, accessUpdater, scoreCalculator};
  cache.insert("24", 24);
  cache.insert("2", 2);
  cache.insert("8", 8);
  cache.insert("5", 5);
  ASSERT_TRUE(cache.contains("24"));
  ASSERT_TRUE(cache.contains("8"));
  ASSERT_TRUE(cache.contains("5"));
  ASSERT_FALSE(cache.contains("2"));
}
TEST(FlexibleCacheTest, LRUSimple) {
  ad_utility::HeapBasedLRUCache<string, int> cache(3, 10000, 10000);
  cache.insert("24", 24);
  cache.insert("2", 2);
  cache.insert("8", 8);
  cache.insert("5", 5);
  ASSERT_FALSE(cache.contains("24"));
  ASSERT_TRUE(cache.contains("8"));
  ASSERT_TRUE(cache.contains("5"));
  ASSERT_TRUE(cache.contains("2"));
}
namespace ad_utility {
// _____________________________________________________________________________
TEST(LRUCacheTest, testSimpleMapUsage) {
  LRUCache<string, string> cache(5, 10000, 10000);
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
}
// _____________________________________________________________________________
TEST(LRUCacheTest, testSimpleMapUsageWithDrop) {
  LRUCache<string, string> cache(3);
  cache.insert("1", "x");
  cache.insert("2", "xx");
  cache.insert("3", "xxx");
  cache.insert("4", "xxxx");
  cache.insert("5", "xxxxx");

  ASSERT_FALSE(cache["2"]);         // second oldest dropped
  ASSERT_FALSE(cache["1"]);         // oldest dropped
  ASSERT_EQ(*cache["5"], "xxxxx");  // not dropped
  ASSERT_EQ(*cache["4"], "xxxx");   // not dropped
  ASSERT_EQ(*cache["3"], "xxx");    // not dropped
  cache.insert("6", "xxxxxx");
  ASSERT_FALSE(cache["5"]);          // oldest access driooed
  ASSERT_EQ(*cache["3"], "xxx");     // not dropped
  ASSERT_EQ(*cache["4"], "xxxx");    // not dropped
  ASSERT_EQ(*cache["6"], "xxxxxx");  // not dropped
}

// _____________________________________________________________________________
TEST(LRUCacheTest, testIncreasingCapacity) {
  LRUCache<string, string> cache(5);
  cache.insert("1", "1x");
  cache.insert("2", "2x");
  cache.insert("3", "3x");
  cache.insert("4", "4x");
  cache.insert("5", "5x");

  ASSERT_EQ(*cache["1"], "1x");
  ASSERT_EQ(*cache["2"], "2x");
  ASSERT_EQ(*cache["3"], "3x");
  ASSERT_EQ(*cache["4"], "4x");
  ASSERT_EQ(*cache["5"], "5x");
  cache.setMaxNumEntries(10);
  ASSERT_EQ(*cache["3"], "3x");
  cache.insert("6", "6x");
  ASSERT_EQ(*cache["6"], "6x");
  ASSERT_EQ(*cache["5"], "5x");
  cache.insert("0", "0x");
  ASSERT_EQ(*cache["0"], "0x");
  ASSERT_EQ(*cache["4"], "4x");
  ASSERT_EQ(*cache["5"], "5x");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, testDecreasingCapacity) {
  LRUCache<string, string> cache(10);
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
  cache.insert("9", "xxxxxxxxx");
  cache.setMaxNumEntries(2);
  ASSERT_EQ(*cache["9"], "xxxxxxxxx");  // freshly inserted
  ASSERT_EQ(*cache["5"], "xxxxx");      // second leat recently used
  ASSERT_FALSE(cache["1"]);
  ASSERT_FALSE(cache["2"]);
  ASSERT_FALSE(cache["3"]);
  ASSERT_FALSE(cache["4"]);
}
}  // namespace ad_utility

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
