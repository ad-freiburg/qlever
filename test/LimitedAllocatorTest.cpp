//
// Created by johannes on 27.04.20.
//

#include <gtest/gtest.h>

#include <vector>

#include "../src/util/AllocatorWithLimit.h"

using ad_utility::AllocatorWithLimit;
using ad_utility::makeAllocationMemoryLeftThreadsafeObject;

using V = std::vector<int, AllocatorWithLimit<int>>;
TEST(AllocatorWithLimit, initial) {
  AllocatorWithLimit<int> all{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(25)};
  static_assert(sizeof(int) == 4);
  [[maybe_unused]] auto ptr = all.allocate(6);
  ASSERT_THROW(all.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);
  all.deallocate(ptr, 6);
}

TEST(AllocatorWithLimit, vector) {
  V v{AllocatorWithLimit<int>(makeAllocationMemoryLeftThreadsafeObject(18))};
  v.push_back(5);  // allocate 4 bytes -> works
  ASSERT_EQ(v.size(), 1u);
  ASSERT_EQ(v[0], 5);

  v.push_back(4);  // allocate 8 bytes, then free 4, works (10 bytes free)
  ASSERT_EQ(v.size(), 2u);
  ASSERT_EQ(v[1], 4);

  // allocate 16 bytes, FAILS (first allocate, then copy, then free 8)
  ASSERT_THROW(v.push_back(1),
               ad_utility::detail::AllocationExceedsLimitException);
}

TEST(AllocatorWithLimit, vectorShared) {
  AllocatorWithLimit<int> allocator(
      makeAllocationMemoryLeftThreadsafeObject(18));
  V v{allocator};
  V u{allocator};
  v.push_back(5);  // allocate 4 bytes -> works
  u.push_back(5);
  v.push_back(4);  // allocate 8 bytes, then free 4, works (10 bytes free)
  ASSERT_EQ(v.size(), 2u);
  ASSERT_EQ(v[1], 4);

  ASSERT_THROW(u.push_back(1),
               ad_utility::detail::AllocationExceedsLimitException);
}
