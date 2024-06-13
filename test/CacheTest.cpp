// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <string>

#include "util/Cache.h"
#include "util/DefaultValueSizeGetter.h"
#include "util/MemorySize/MemorySize.h"

using std::string;
using namespace ad_utility::memory_literals;
using enum ad_utility::ResizeResult;

using Vec = std::vector<int>;
auto vectorSizeGetter = [](const auto& pointer) {
  return pointer->size() * sizeof(int) * 1_B;
};

// first some simple Tests for the general cache interface
TEST(FlexibleCacheTest, Simple) {
  auto accessUpdater = [](const auto& s, [[maybe_unused]] const auto& v) {
    return s;
  };
  auto scoreCalculator = [](const auto& v) { return v; };
  auto scoreComparator = std::less<>();
  ad_utility::HeapBasedCache<string, int, int, decltype(scoreComparator),
                             decltype(accessUpdater), decltype(scoreCalculator),
                             ad_utility::SizeOfSizeGetter>
      cache{3,
            10_kB,
            10_kB,
            scoreComparator,
            accessUpdater,
            scoreCalculator,
            ad_utility::SizeOfSizeGetter{}};
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
  ad_utility::HeapBasedLRUCache<string, int, ad_utility::SizeOfSizeGetter>
      cache(3, 10_kB, 10_kB);
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
  LRUCache<string, string, StringSizeGetter<string>> cache(5, 10_kB, 10_kB);
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
  LRUCache<string, string, StringSizeGetter<string>> cache(3);
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
  LRUCache<string, string, StringSizeGetter<string>> cache(5);
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
  LRUCache<string, string, StringSizeGetter<string>> cache(10);
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

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyTransformValueWorksForNonPinnedValues) {
  LRUCache<string, string, StringSizeGetter<string>> cache{2, 3_B};
  cache.insert("1", "x");

  ASSERT_EQ(cache.nonPinnedSize(), 1_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);

  cache.transformValue("1",
                       [](const std::string& value) { return value + "a"; });

  ASSERT_EQ(cache.nonPinnedSize(), 2_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);
  ASSERT_TRUE(cache.contains("1"));
  ASSERT_EQ(*cache["1"], "xa");

  cache.insert("2", "y");

  ASSERT_EQ(cache.nonPinnedSize(), 3_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);

  cache.transformValue("1",
                       [](const std::string& value) { return value + "b"; });

  ASSERT_EQ(cache.nonPinnedSize(), 3_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);

  ASSERT_TRUE(cache.contains("1"));
  ASSERT_FALSE(cache.contains("2"));
  ASSERT_EQ(*cache["1"], "xab");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyTransformValueWorksForPinnedValues) {
  LRUCache<string, string, StringSizeGetter<string>> cache{1};
  cache.insertPinned("1", "x");

  ASSERT_EQ(cache.nonPinnedSize(), 0_B);
  ASSERT_EQ(cache.pinnedSize(), 1_B);

  cache.transformValue("1",
                       [](const std::string& value) { return value + "a"; });

  ASSERT_EQ(cache.nonPinnedSize(), 0_B);
  ASSERT_EQ(cache.pinnedSize(), 2_B);
  ASSERT_TRUE(cache.contains("1"));
  ASSERT_EQ(*cache["1"], "xa");

  cache.insert("2", "y");

  ASSERT_EQ(cache.nonPinnedSize(), 1_B);
  ASSERT_EQ(cache.pinnedSize(), 2_B);
  ASSERT_TRUE(cache.contains("1"));
  ASSERT_TRUE(cache.contains("2"));

  cache.transformValue("1",
                       [](const std::string& value) { return value + "b"; });

  ASSERT_EQ(cache.nonPinnedSize(), 0_B);
  ASSERT_EQ(cache.pinnedSize(), 3_B);
  ASSERT_TRUE(cache.contains("1"));
  ASSERT_FALSE(cache.contains("2"));

  ASSERT_EQ(*cache["1"], "xab");
}

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyTransformValueWorksIsNoOpForNonExistantValues) {
  LRUCache<string, string, StringSizeGetter<string>> cache{1};

  ASSERT_EQ(cache.nonPinnedSize(), 0_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);

  cache.transformValue("1",
                       [](const std::string&) { return std::string{"a"}; });

  ASSERT_EQ(cache.nonPinnedSize(), 0_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);
  ASSERT_FALSE(cache.contains("1"));

  cache.insert("2", "y");

  ASSERT_EQ(cache.nonPinnedSize(), 1_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);
  ASSERT_TRUE(cache.contains("2"));

  cache.transformValue("1",
                       [](const std::string&) { return std::string{"a"}; });

  ASSERT_EQ(cache.nonPinnedSize(), 1_B);
  ASSERT_EQ(cache.pinnedSize(), 0_B);
  ASSERT_FALSE(cache.contains("1"));
  ASSERT_TRUE(cache.contains("2"));
}

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyCacheSizeIsCorrectlyTrackedWhenChangedWhenErased) {
  LRUCache<int, std::shared_ptr<std::vector<int>>, decltype(vectorSizeGetter)>
      cache{1};

  auto vecA = std::make_shared<Vec>();

  cache.insert(0, vecA);

  ASSERT_EQ(cache._totalSizeNonPinned, 0_B);
  vecA->push_back(0);

  // Cache does was not notified about the size change
  ASSERT_EQ(cache._totalSizeNonPinned, 0_B);

  cache.erase(0);

  // Cache should not underflow
  ASSERT_EQ(cache._totalSizeNonPinned, 0_B);

  cache.insert(0, vecA);

  ASSERT_EQ(cache._totalSizeNonPinned, 4_B);
  vecA->clear();

  // Cache does was not notified about the size change
  ASSERT_EQ(cache._totalSizeNonPinned, 4_B);

  cache.erase(0);

  // Cache correctly remove size, even though the vector is empty by now.
  ASSERT_EQ(cache._totalSizeNonPinned, 0_B);
}

// _____________________________________________________________________________
TEST(LRUCacheTest,
     verifyCacheSizeIsCorrectlyTrackedWhenChangedWhenErasedPinned) {
  LRUCache<int, std::shared_ptr<std::vector<int>>, decltype(vectorSizeGetter)>
      cache{1};

  auto vecA = std::make_shared<Vec>();

  cache.insertPinned(0, vecA);

  ASSERT_EQ(cache._totalSizePinned, 0_B);
  vecA->push_back(0);

  // Cache does was not notified about the size change
  ASSERT_EQ(cache._totalSizePinned, 0_B);

  cache.erase(0);

  // Cache should not underflow
  ASSERT_EQ(cache._totalSizePinned, 0_B);

  cache.insertPinned(0, vecA);

  ASSERT_EQ(cache._totalSizePinned, 4_B);
  vecA->clear();

  // Cache does was not notified about the size change
  ASSERT_EQ(cache._totalSizePinned, 4_B);

  cache.erase(0);

  // Cache correctly remove size, even though the vector is empty by now.
  ASSERT_EQ(cache._totalSizePinned, 0_B);
}

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyCacheSizeIsCorrectlyRecomputed) {
  LRUCache<int, std::shared_ptr<std::vector<int>>, decltype(vectorSizeGetter)>
      cache{3, 8_B, 4_B};

  auto vecA = std::make_shared<Vec>();
  auto vecB = std::make_shared<Vec>(1);

  cache.insert(0, vecA);
  cache.insert(1, vecB);

  ASSERT_EQ(cache._totalSizeNonPinned, 4_B);

  vecA->push_back(0);
  vecB->push_back(1);

  // Cache does was not notified about the size change
  ASSERT_EQ(cache._totalSizeNonPinned, 4_B);

  ASSERT_EQ(FITS_IN_CACHE, cache.recomputeSize(0, false));

  ASSERT_EQ(cache._totalSizeNonPinned, 8_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));

  vecA->push_back(1);

  ASSERT_EQ(EXCEEDS_SINGLE_ENTRY_SIZE, cache.recomputeSize(0, false));

  ASSERT_EQ(cache._totalSizeNonPinned, 12_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));

  vecA->resize(1);

  ASSERT_EQ(FITS_IN_CACHE, cache.recomputeSize(0, false));

  ASSERT_EQ(cache._totalSizeNonPinned, 8_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));

  auto vecC = std::make_shared<Vec>(0);
  cache.insert(2, vecC);
  vecB->resize(1);

  ASSERT_EQ(FITS_IN_CACHE, cache.recomputeSize(1, false));

  ASSERT_EQ(cache._totalSizeNonPinned, 8_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));
  ASSERT_TRUE(cache.contains(2));

  // Set to high value to avoid getting limited by this.
  cache.setMaxSizeSingleEntry(64_B);
  vecC->push_back(0);
  vecC->push_back(1);
  vecC->push_back(2);
  ASSERT_EQ(EXCEEDS_MAX_SIZE, cache.recomputeSize(2, false));

  ASSERT_EQ(cache._totalSizeNonPinned, 8_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));
  ASSERT_FALSE(cache.contains(2));

  cache.setMaxSizeSingleEntry(4_B);
  vecA->push_back(1);

  ASSERT_EQ(EXCEEDS_SINGLE_ENTRY_SIZE, cache.recomputeSize(0, true));

  ASSERT_EQ(cache._totalSizeNonPinned, 4_B);
  ASSERT_FALSE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));
  ASSERT_FALSE(cache.contains(2));

  vecB->clear();
  cache.erase(1);

  ASSERT_EQ(cache._totalSizeNonPinned, 0_B);
  ASSERT_FALSE(cache.contains(0));
  ASSERT_FALSE(cache.contains(1));
  ASSERT_FALSE(cache.contains(2));
}

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyCacheSizeIsCorrectlyRecomputedPinned) {
  LRUCache<int, std::shared_ptr<std::vector<int>>, decltype(vectorSizeGetter)>
      cache{3, 8_B, 4_B};

  auto vecA = std::make_shared<Vec>();
  auto vecB = std::make_shared<Vec>(1);

  cache.insertPinned(0, vecA);
  cache.insertPinned(1, vecB);

  ASSERT_EQ(cache._totalSizePinned, 4_B);

  vecA->push_back(0);
  vecB->push_back(1);

  // Cache does was not notified about the size change
  ASSERT_EQ(cache._totalSizePinned, 4_B);

  ASSERT_EQ(FITS_IN_CACHE, cache.recomputeSize(0, false));

  ASSERT_EQ(cache._totalSizePinned, 8_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));

  vecA->push_back(1);

  ASSERT_EQ(EXCEEDS_SINGLE_ENTRY_SIZE, cache.recomputeSize(0, false));
  ASSERT_EQ(EXCEEDS_SINGLE_ENTRY_SIZE, cache.recomputeSize(0, true));

  ASSERT_EQ(cache._totalSizePinned, 12_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));

  vecA->resize(1);

  ASSERT_EQ(FITS_IN_CACHE, cache.recomputeSize(0, false));

  ASSERT_EQ(cache._totalSizePinned, 8_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));

  auto vecC = std::make_shared<Vec>(0);
  cache.insertPinned(2, vecC);
  vecB->resize(1);

  ASSERT_EQ(FITS_IN_CACHE, cache.recomputeSize(1, false));

  ASSERT_EQ(cache._totalSizePinned, 8_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));
  ASSERT_TRUE(cache.contains(2));

  // Set to high value to avoid getting limited by this.
  cache.setMaxSizeSingleEntry(64_B);
  vecC->push_back(0);
  vecC->push_back(1);
  vecC->push_back(2);
  ASSERT_EQ(EXCEEDS_MAX_SIZE, cache.recomputeSize(2, true));

  ASSERT_EQ(cache._totalSizePinned, 20_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));
  ASSERT_TRUE(cache.contains(2));
  cache.erase(2);

  cache.setMaxSizeSingleEntry(4_B);
  vecA->push_back(1);

  ASSERT_EQ(EXCEEDS_SINGLE_ENTRY_SIZE, cache.recomputeSize(0, true));

  ASSERT_EQ(cache._totalSizePinned, 12_B);
  ASSERT_TRUE(cache.contains(0));
  ASSERT_TRUE(cache.contains(1));
  ASSERT_FALSE(cache.contains(2));
  cache.erase(0);

  vecB->clear();
  cache.erase(1);

  ASSERT_EQ(cache._totalSizePinned, 0_B);
  ASSERT_FALSE(cache.contains(0));
  ASSERT_FALSE(cache.contains(1));
  ASSERT_FALSE(cache.contains(2));
}

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyNonPinnedEntriesAreRemovedToMakeRoomForResize) {
  LRUCache<int, std::shared_ptr<std::vector<int>>, decltype(vectorSizeGetter)>
      cache{3, 8_B, 4_B};

  auto vecA = std::make_shared<Vec>(1);
  auto vecB = std::make_shared<Vec>(1);
  auto vecC = std::make_shared<Vec>(0);

  cache.insertPinned(0, vecA);
  cache.insert(1, vecB);
  cache.insert(2, vecC);

  vecC->push_back(0);

  ASSERT_EQ(FITS_IN_CACHE, cache.recomputeSize(2, true));
  ASSERT_TRUE(cache.contains(0));
  ASSERT_FALSE(cache.contains(1));
  ASSERT_TRUE(cache.contains(2));
}

// _____________________________________________________________________________
TEST(LRUCacheTest, verifyRecomputeIsNoOpForNonExistentElement) {
  LRUCache<string, string, StringSizeGetter<string>> cache{1};
  cache.insert("1", "a");

  cache.recomputeSize("2", false);

  EXPECT_TRUE(cache.contains("1"));
  EXPECT_FALSE(cache.contains("2"));

  cache.recomputeSize("2", true);

  EXPECT_TRUE(cache.contains("1"));
  EXPECT_FALSE(cache.contains("2"));
}

// TODO<RobinTF> Add test to check if for EXCEEDS_MAX_SIZE the pinned entries
// are correctly taken into consideration
// TODO<RobinTF> Add test and update Cache code to signal EXCEEDS_MAX_SIZE and
// EXCEEDS_SINGLE_ENTRY_SIZE if after reduction in size entries are still too
// large.
}  // namespace ad_utility
