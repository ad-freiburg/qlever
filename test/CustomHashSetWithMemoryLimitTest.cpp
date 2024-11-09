#include <gtest/gtest.h>

#include "../src/util/HashSet.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::makeAllocationMemoryLeftThreadsafeObject;
using namespace ad_utility::memory_literals;

using Set = ad_utility::NodeHashSetWithMemoryLimit<int>;

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, sizeAndInsert) {
  Set hashSet{makeAllocationMemoryLeftThreadsafeObject(100_B)};
  ASSERT_EQ(hashSet.size(), 0);
  hashSet.insert(1);
  hashSet.insert(2);
  hashSet.insert(3);
  ASSERT_EQ(hashSet.size(), 3);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, memoryLimit) {
  Set hashSet{makeAllocationMemoryLeftThreadsafeObject(10_B)};
  std::initializer_list<int> testNums = {
      1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17,
      18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34,
      35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
      52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68,
      69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85,
      86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100};
  ASSERT_THROW(
      {
        for (auto num : testNums) {
          hashSet.insert(num);
        }
      },
      ad_utility::detail::AllocationExceedsLimitException);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, iteratorOperations) {
  Set hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};
  hashSet.insert(1);
  hashSet.insert(2);
  hashSet.insert(3);

  // Test iterator functionality
  auto it = hashSet.find(2);
  ASSERT_NE(it, hashSet.end());
  ASSERT_EQ(*it, 2);

  // Test non-existing element
  ASSERT_EQ(hashSet.find(4), hashSet.end());

  // Test iteration
  std::vector<int> values;
  for (const auto& val : hashSet) {
    values.push_back(val);
  }
  std::sort(values.begin(), values.end());
  ASSERT_EQ(values, std::vector<int>({1, 2, 3}));
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, eraseOperations) {
  Set hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};
  hashSet.insert(1);
  hashSet.insert(2);
  hashSet.insert(3);

  // Test erase of existing element
  hashSet.erase(2);
  ASSERT_EQ(hashSet.size(), 2);
  ASSERT_FALSE(hashSet.contains(2));

  // Test erase of non-existing element
  size_t originalSize = hashSet.size();
  hashSet.erase(4);
  ASSERT_EQ(hashSet.size(), originalSize);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, clearOperation) {
  Set hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};
  auto initialMemory = hashSet.getCurrentMemoryUsage();

  // Add some elements
  hashSet.insert(1);
  hashSet.insert(2);
  hashSet.insert(3);

  auto usedMemory = hashSet.getCurrentMemoryUsage();
  ASSERT_GT(usedMemory, initialMemory);

  // Clear the set
  hashSet.clear();
  ASSERT_EQ(hashSet.size(), 0);
  ASSERT_TRUE(hashSet.empty());

  // Memory usage should be back to approximately initial state
  // (might be slightly different due to bucket array size)
  ASSERT_LE(hashSet.getCurrentMemoryUsage(), usedMemory);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, memoryTrackingAccuracy) {
  Set hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};
  auto initialMemory = hashSet.getCurrentMemoryUsage();

  // Insert and track memory changes
  hashSet.insert(1);
  auto afterOneInsert = hashSet.getCurrentMemoryUsage();
  ASSERT_GT(afterOneInsert, initialMemory);

  // Insert duplicate and verify memory doesn't change
  auto beforeDuplicate = hashSet.getCurrentMemoryUsage();
  hashSet.insert(1);
  ASSERT_EQ(hashSet.getCurrentMemoryUsage(), beforeDuplicate);

  // Remove element and verify memory decreases
  hashSet.erase(1);
  ASSERT_EQ(hashSet.getCurrentMemoryUsage(), initialMemory);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, edgeCases) {
  // Test with zero memory limit
  ASSERT_THROW(Set zeroHashSet{makeAllocationMemoryLeftThreadsafeObject(0_B)},
               ad_utility::detail::AllocationExceedsLimitException);

  // Test multiple insert/erase cycles
  Set cycleHashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};
  auto memoryBeforeCycle = cycleHashSet.getCurrentMemoryUsage();
  for (int i = 0; i < 10; ++i) {
    cycleHashSet.insert(i);
    cycleHashSet.erase(i);
  }
  auto memoryAfterCycle = cycleHashSet.getCurrentMemoryUsage();
  ASSERT_TRUE(cycleHashSet.empty());
  ASSERT_EQ(memoryBeforeCycle, memoryAfterCycle);
}

// Define a custom size getter for strings that includes the actual string
// memory
struct StringSizeGetter {
  ad_utility::MemorySize operator()(const std::string& str) const {
    return ad_utility::MemorySize::bytes(str.capacity());
  }
};

using StringSet =
    ad_utility::NodeHashSetWithMemoryLimit<std::string, StringSizeGetter>;

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, stringInsertAndMemoryTracking) {
  StringSet hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};
  auto initialMemory = hashSet.getCurrentMemoryUsage();

  // Insert string and verify memory increase
  hashSet.insert("test");
  auto afterFirstInsert = hashSet.getCurrentMemoryUsage();
  ASSERT_GT(afterFirstInsert, initialMemory);

  // Insert longer string and verify proportional memory increase
  hashSet.insert("this is a much longer test string");
  auto afterSecondInsert = hashSet.getCurrentMemoryUsage();
  ASSERT_GT(afterSecondInsert, afterFirstInsert);

  // The difference should be proportional to string lengths
  ASSERT_GT(afterSecondInsert - afterFirstInsert,
            afterFirstInsert - initialMemory);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, stringMemoryLimit) {
  // Set a very small memory limit
  StringSet hashSet{makeAllocationMemoryLeftThreadsafeObject(100_B)};

  // This should work
  ASSERT_NO_THROW(hashSet.insert("small"));

  // This should fail due to memory limit
  ASSERT_THROW(
      hashSet.insert(
          "this is a very long string that should exceed our memory limit"),
      ad_utility::detail::AllocationExceedsLimitException);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, stringEraseAndClear) {
  StringSet hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};

  // Insert several strings
  hashSet.insert("first");
  hashSet.insert("second");
  hashSet.insert("third");
  auto memoryWithStrings = hashSet.getCurrentMemoryUsage();

  // Erase one string and verify memory decrease
  hashSet.erase("second");
  auto memoryAfterErase = hashSet.getCurrentMemoryUsage();
  ASSERT_LT(memoryAfterErase, memoryWithStrings);

  // Clear all and verify memory is minimal
  hashSet.clear();
  auto memoryAfterClear = hashSet.getCurrentMemoryUsage();
  ASSERT_LT(memoryAfterClear, memoryAfterErase);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, stringDuplicates) {
  StringSet hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};

  // Insert string and record memory
  hashSet.insert("duplicate");
  auto memoryAfterFirst = hashSet.getCurrentMemoryUsage();

  // Insert same string again and verify no memory change
  hashSet.insert("duplicate");
  auto memoryAfterSecond = hashSet.getCurrentMemoryUsage();
  ASSERT_EQ(memoryAfterFirst, memoryAfterSecond);
  ASSERT_EQ(hashSet.size(), 1);
}

// _____________________________________________________________________________
TEST(CustomHashSetWithMemoryLimitTest, stringCapacityVsSize) {
  StringSet hashSet{makeAllocationMemoryLeftThreadsafeObject(1000_B)};

  // Insert a string that will likely have different size and capacity
  std::string str = "test";
  str.reserve(100);  // Force larger capacity than needed

  auto beforeInsert = hashSet.getCurrentMemoryUsage();
  hashSet.insert(str);
  auto afterInsert = hashSet.getCurrentMemoryUsage();

  // The memory difference should account for the capacity, not just the size
  auto memoryDifference = afterInsert - beforeInsert;
  ASSERT_GT(memoryDifference, ad_utility::MemorySize::bytes(str.size()));
  ASSERT_GE(memoryDifference, ad_utility::MemorySize::bytes(str.capacity()));
}
