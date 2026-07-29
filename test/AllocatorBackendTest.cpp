// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Typed tests covering the shared public surface of both allocator backends.
// The same test body is compiled and run for both:
//   ad_utility::allocatorImpl::AllocatorWithLimit<int>  (limit backend)
//   ad_utility::PmrAllocator<int>                       (PMR backend)
//
// Backend-specific behaviour lives in AllocatorWithLimitTest.cpp and
// AllocatorPmrTest.cpp respectively.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "util/AllocatorPmr.h"
#include "util/AllocatorWithLimitImpl.h"
#include "util/GTestHelpers.h"

namespace {

using namespace ad_utility::memory_literals;

template <typename T>
class AllocatorBackendTest : public ::testing::Test {};

using AllocatorBackendTypes =
    ::testing::Types<ad_utility::allocatorImpl::AllocatorWithLimit<int>,
                     ad_utility::PmrAllocator<int>>;

TYPED_TEST_SUITE(AllocatorBackendTest, AllocatorBackendTypes);

// Allocating within the budget works; exceeding it throws
// AllocationExceedsLimitException; amountMemoryLeft() tracks allocate and
// deallocate correctly.
TYPED_TEST(AllocatorBackendTest, LimitEnforced) {
  static_assert(sizeof(int) == 4);
  auto alloc = TypeParam::makeLimited(12_B);
  EXPECT_EQ(alloc.amountMemoryLeft(), 12_B);

  int* p = alloc.allocate(1);  // 4 bytes consumed
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);

  int* p2 = alloc.allocate(2);  // 8 more bytes consumed
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);

  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);

  alloc.deallocate(p2, 2);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);
  alloc.deallocate(p, 1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 12_B);
}

// The clearOnAllocation hook is invoked when the budget is exceeded and may
// free room; since the hook here frees nothing, the allocation still throws
// afterwards. This behaviour is shared by both backends.
TYPED_TEST(AllocatorBackendTest, ClearOnAllocationHook) {
  bool called = false;
  auto alloc = TypeParam::makeLimited(
      4_B, [&called](ad_utility::MemorySize) { called = true; });
  int* p1 = alloc.allocate(1);  // consumes the whole 4-byte budget
  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_TRUE(called);
  alloc.deallocate(p1, 1);
}

// .as<char>() yields an allocator sharing the same budget: consuming bytes
// through the char allocator reduces the int allocator's amountMemoryLeft().
TYPED_TEST(AllocatorBackendTest, AsSharesBudget) {
  static_assert(sizeof(int) == 4);
  auto allocInt = TypeParam::makeLimited(8_B);
  auto allocChar = allocInt.template as<char>();

  EXPECT_EQ(allocChar.amountMemoryLeft(), 8_B);

  char* p = allocChar.allocate(3);
  EXPECT_EQ(allocInt.amountMemoryLeft(), 5_B);
  allocChar.deallocate(p, 3);
  EXPECT_EQ(allocInt.amountMemoryLeft(), 8_B);
}

// Copies and rebound allocators share the same backing state.
TYPED_TEST(AllocatorBackendTest, AsSharesBackingState) {
  auto allocInt = TypeParam::makeLimited(8_B);
  auto allocChar = allocInt.template as<char>();
  EXPECT_EQ(allocInt, allocInt.template as<int>());
  EXPECT_EQ(allocInt, allocChar.template as<int>());
}

// Larger allocations track memory correctly and report the same amount via a
// const allocator. Exceeding the remaining budget reports the requested and
// available memory.
TYPED_TEST(AllocatorBackendTest, LargeAllocationTracksMemory) {
  TypeParam alloc = TypeParam::makeLimited(2_MB);
  static_assert(sizeof(int) == 4);
  [[maybe_unused]] auto ptr = alloc.allocate(250'000);
  EXPECT_EQ(alloc.amountMemoryLeft(), 1_MB);
  EXPECT_EQ(std::as_const(alloc).amountMemoryLeft(), 1_MB);
  AD_EXPECT_THROW_WITH_MESSAGE(
      alloc.allocate(500'000),
      ::testing::StrEq("Tried to allocate 2 MB, but only 1 MB were available"));
  alloc.deallocate(ptr, 250'000);
}

// Both backends satisfy the standard allocator requirements when used
// with std::vector.
TYPED_TEST(AllocatorBackendTest, WorksWithVector) {
  auto alloc = TypeParam::makeLimited(ad_utility::MemorySize::megabytes(1));
  std::vector<int, TypeParam> v{alloc};
  for (int i = 0; i < 1000; ++i) {
    v.push_back(i);
  }
  EXPECT_EQ(v.size(), 1000u);
  EXPECT_EQ(v.front(), 0);
  EXPECT_EQ(v.back(), 999);
  EXPECT_LT(alloc.amountMemoryLeft(), ad_utility::MemorySize::megabytes(1));
}

// Multiple vectors using copies of the same allocator share the same budget.
TYPED_TEST(AllocatorBackendTest, VectorsShareBudgetAndThrowOnGrowth) {
  auto allocator = TypeParam::makeLimited(18_B);
  std::vector<int, TypeParam> v{allocator};
  std::vector<int, TypeParam> u{allocator};
  v.push_back(5);  // allocate 4 bytes -> works
  u.push_back(5);
  v.push_back(4);  // allocate 8 bytes, then free 4 bytes -> works
  ASSERT_EQ(v.size(), 2u);
  ASSERT_EQ(v[1], 4);

  ASSERT_THROW(u.push_back(1),
               ad_utility::detail::AllocationExceedsLimitException);
}

// makeUnlimited() never throws for reasonable allocations and reports max().
TYPED_TEST(AllocatorBackendTest, UnlimitedAllocator) {
  auto alloc = TypeParam::makeUnlimited();
  EXPECT_EQ(alloc.amountMemoryLeft(), ad_utility::MemorySize::max());
  int* p = alloc.allocate(1000);
  ASSERT_NE(p, nullptr);
  alloc.deallocate(p, 1000);
}

// operator==: an allocator equals itself and its own .as<int>() (same backing
// state). Two independently-created allocators are not equal.
TYPED_TEST(AllocatorBackendTest, Equality) {
  auto a1 = TypeParam::makeLimited(20_B);
  auto a2 = TypeParam::makeLimited(20_B);

  EXPECT_EQ(a1, a1);
  EXPECT_EQ(a1, a1.template as<int>());
  EXPECT_NE(a1, a2);
}

}  // namespace
