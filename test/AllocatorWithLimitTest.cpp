//
// Created by johannes on 27.04.20.
//

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/AllocatorWithLimit.h"

using ad_utility::AllocatorWithLimit;
using ad_utility::makeAllocationMemoryLeftThreadsafeObject;

using V = std::vector<int, AllocatorWithLimit<int>>;
TEST(AllocatorWithLimit, initial) {
  AllocatorWithLimit<int> all{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(2ul << 20)};
  static_assert(sizeof(int) == 4);
  [[maybe_unused]] auto ptr = all.allocate(250'000);
  ASSERT_EQ(all.numFreeBytes(), (2ul << 20) - 1'000'000);
  ASSERT_EQ(std::as_const(all).numFreeBytes(), (2ul << 20) - 1'000'000);
  try {
    all.allocate(600'000);
    FAIL() << "Should have thrown";
  } catch (const ad_utility::detail::AllocationExceedsLimitException& e) {
    ASSERT_THAT(e.what(),
                ::testing::StartsWith(
                    "Tried to allocate 2MB, but only 1MB were available."));
  }
  all.deallocate(ptr, 250'000);
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

TEST(AllocatorWithLimit, equality) {
  AllocatorWithLimit<int> a1{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(20)};
  AllocatorWithLimit<int> a2{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(20)};

  ASSERT_EQ(a1, a1);
  ASSERT_EQ(a2, a2);
  ASSERT_NE(a1, a2);
}
