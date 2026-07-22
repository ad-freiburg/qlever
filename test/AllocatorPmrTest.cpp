// Copyright 2026, QLever contributors.
//
// Unit tests for the `std::pmr`-based allocator backend
// (`util/AllocatorPmr.h`). These tests always compile against the PMR types
// directly, independently of which backend `qlever::Allocator` currently
// selects.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory_resource>
#include <vector>

#include "util/AllocatorPmr.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::LimitedMemoryResource;
using ad_utility::makePmrAllocatorFromResource;
using ad_utility::makePmrAllocatorWithLimit;
using ad_utility::makeUnlimitedPmrAllocator;
using ad_utility::MemorySize;
using ad_utility::PmrAllocatorWithLimit;

namespace {
using namespace ad_utility::memory_literals;
}

// The limit-tracking resource enforces the configured budget.
TEST(AllocatorPmr, LimitIsEnforced) {
  auto alloc = makePmrAllocatorWithLimit<int>(4_B);
  // Allocating one int (4 bytes) is fine.
  int* p = alloc.allocate(1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);
  // A second allocation exceeds the limit and throws.
  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);
  alloc.deallocate(p, 1);
  EXPECT_EQ(alloc.amountMemoryLeft(), 4_B);
}

// `clearOnAllocation` is invoked when the budget is exceeded and may free room.
TEST(AllocatorPmr, ClearOnAllocationHook) {
  bool called = false;
  auto resource = std::make_shared<LimitedMemoryResource>(
      4_B, std::pmr::new_delete_resource(),
      [&called](MemorySize) { called = true; });
  auto alloc = PmrAllocatorWithLimit<int>{
      std::static_pointer_cast<std::pmr::memory_resource>(resource)};
  int* p1 = alloc.allocate(1);
  // Second allocation triggers the hook; since the hook frees nothing, it still
  // throws afterwards.
  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_TRUE(called);
  alloc.deallocate(p1, 1);
}

// `.as<U>()` yields an allocator sharing the same underlying resource/budget.
TEST(AllocatorPmr, AsSharesResource) {
  auto allocInt = makePmrAllocatorWithLimit<int>(8_B);
  auto allocChar = allocInt.as<char>();
  EXPECT_EQ(allocInt.resource(), allocChar.resource());
  EXPECT_TRUE(allocInt == allocInt.as<int>());
  // Consuming through the char allocator reduces the shared budget.
  char* p = allocChar.allocate(4);
  EXPECT_EQ(allocInt.amountMemoryLeft(), 4_B);
  allocChar.deallocate(p, 4);
}

// A plain platform resource (non-owning) enforces no limit.
TEST(AllocatorPmr, FromResourceNoLimit) {
  std::pmr::monotonic_buffer_resource platformPool;
  auto alloc = makePmrAllocatorFromResource<int>(&platformPool);
  EXPECT_EQ(alloc.resource(), &platformPool);
  EXPECT_EQ(alloc.amountMemoryLeft(), MemorySize::max());
  int* p = alloc.allocate(10);
  ASSERT_NE(p, nullptr);
  alloc.deallocate(p, 10);
}

// The allocator works as a standard container allocator.
TEST(AllocatorPmr, WorksWithStdVector) {
  auto alloc = makePmrAllocatorWithLimit<int>(MemorySize::megabytes(1));
  std::vector<int, PmrAllocatorWithLimit<int>> v{alloc};
  for (int i = 0; i < 1000; ++i) {
    v.push_back(i);
  }
  EXPECT_EQ(v.size(), 1000u);
  EXPECT_EQ(v.front(), 0);
  EXPECT_EQ(v.back(), 999);
  EXPECT_LT(alloc.amountMemoryLeft(), MemorySize::megabytes(1));
}

// The unlimited factory never throws for reasonable allocations.
TEST(AllocatorPmr, UnlimitedAllocator) {
  auto alloc = makeUnlimitedPmrAllocator<int>();
  EXPECT_EQ(alloc.amountMemoryLeft(), MemorySize::max());
  int* p = alloc.allocate(1000);
  ASSERT_NE(p, nullptr);
  alloc.deallocate(p, 1000);
}
