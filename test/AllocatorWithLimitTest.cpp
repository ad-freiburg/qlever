//
// Created by johannes on 27.04.20.
//

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "util/AllocatorWithLimit.h"
#include "util/GTestHelpers.h"

using ad_utility::AllocatorWithLimit;
using ad_utility::makeAllocationMemoryLeftThreadsafeObject;
using namespace ad_utility::memory_literals;

using V = std::vector<int, AllocatorWithLimit<int>>;
TEST(AllocatorWithLimit, initial) {
  AllocatorWithLimit<int> all{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(2_MB)};
  static_assert(sizeof(int) == 4);
  [[maybe_unused]] auto ptr = all.allocate(250'000);
  ASSERT_EQ(all.amountMemoryLeft(), 1_MB);
  ASSERT_EQ(std::as_const(all).amountMemoryLeft(), 1_MB);
  AD_EXPECT_THROW_WITH_MESSAGE(
      all.allocate(500'000),
      ::testing::StrEq("Tried to allocate 2 MB, but only 1 MB were available"));
  all.deallocate(ptr, 250'000);
}

TEST(AllocatorWithLimit, vector) {
  V v{AllocatorWithLimit<int>(makeAllocationMemoryLeftThreadsafeObject(18_B))};
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
      makeAllocationMemoryLeftThreadsafeObject(18_B));
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
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B)};
  AllocatorWithLimit<int> a2{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B)};

  ASSERT_EQ(a1, a1);
  ASSERT_EQ(a2, a2);
  ASSERT_NE(a1, a2);
}

TEST(AllocatorWithLimit, unlikelyExceptionsDuringCopyingAndMoving) {
  struct ThrowOnCopy {
    ThrowOnCopy() = default;
    ThrowOnCopy& operator=(const ThrowOnCopy&) {
      throw std::runtime_error("unexpected copy assign");
    }
    ThrowOnCopy(const ThrowOnCopy&) {
      throw std::runtime_error("unexpected copy construct");
    }
    ThrowOnCopy& operator=(ThrowOnCopy&&) noexcept = default;
    ThrowOnCopy(ThrowOnCopy&&) noexcept = default;
    void operator()(ad_utility::MemorySize) const {}
  };
  AllocatorWithLimit<int> a1{
      ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B),
      ThrowOnCopy{}};
  auto copy = [&a1]() { [[maybe_unused]] auto a2 = a1; };
  auto copyAssign = [&a1]() {
    AllocatorWithLimit<int> a2{
        ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B)};
    a2 = a1;
  };
  ASSERT_THROW(copy(), std::runtime_error);
  ASSERT_THROW(copyAssign(), std::runtime_error);
  auto move = [&a1]() { auto a2 = std::move(a1); };
  auto moveAssign = [&a1]() {
    AllocatorWithLimit<int> a2{
        ad_utility::makeAllocationMemoryLeftThreadsafeObject(20_B)};
    a2 = std::move(a1);
  };
  // The move operations call the copy operations which throw, but are declared
  // `noexcept`, so the program dies when they are called.
  ASSERT_DEATH(move(), "The move constructor of `AllocatorWithLimit`");
  ASSERT_DEATH(moveAssign(),
               "The move assignment operator of `AllocatorWithLimit`");
}
