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

#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/AllocatorPmr.h"
#include "util/AllocatorWithLimitImpl.h"

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
  auto alloc = TypeParam::makeLimited(8_B);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);

  int* p = alloc.allocate(1);  // 4 bytes consumed
  EXPECT_EQ(alloc.amountMemoryLeft(), 4_B);

  int* p2 = alloc.allocate(1);  // 4 more bytes consumed
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);

  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);

  alloc.deallocate(p2, 1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 4_B);
  alloc.deallocate(p, 1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);
}

// .as<char>() yields an allocator sharing the same budget: consuming bytes
// through the char allocator reduces the int allocator's amountMemoryLeft().
TYPED_TEST(AllocatorBackendTest, AsSharesBudget) {
  static_assert(sizeof(int) == 4);
  auto allocInt = TypeParam::makeLimited(8_B);
  auto allocChar = allocInt.template as<char>();

  EXPECT_EQ(allocChar.amountMemoryLeft(), 8_B);

  char* p = allocChar.allocate(4);
  EXPECT_EQ(allocInt.amountMemoryLeft(), 4_B);
  allocChar.deallocate(p, 4);
  EXPECT_EQ(allocInt.amountMemoryLeft(), 8_B);
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
