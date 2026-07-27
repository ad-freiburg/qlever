// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Unit tests for the `ql::pmr`-based allocator backend
// (`util/AllocatorPmr.h`). These tests always compile against the PMR types
// directly, independently of which backend `qlever::Allocator` currently
// selects.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "backports/memory_resource.h"
#include "util/AllocatorPmr.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::LimitedMemoryResource;
using ad_utility::makePmrAllocatorFromResource;
using ad_utility::makePmrAllocatorWithLimit;
using ad_utility::MemorySize;
using ad_utility::PmrAllocator;

namespace {
using namespace ad_utility::memory_literals;
}

// `clearOnAllocation` is invoked when the budget is exceeded and may free room.
TEST(AllocatorPmr, ClearOnAllocationHook) {
  bool called = false;
  auto resource = std::make_shared<LimitedMemoryResource>(
      4_B, ql::pmr::get_default_resource(),
      [&called](MemorySize) { called = true; });
  auto alloc = PmrAllocator<int>{
      std::static_pointer_cast<ql::pmr::memory_resource>(resource)};
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
  ql::pmr::monotonic_buffer_resource platformPool;
  auto alloc = makePmrAllocatorFromResource<int>(&platformPool);
  EXPECT_EQ(alloc.resource(), &platformPool);
  EXPECT_EQ(alloc.amountMemoryLeft(), MemorySize::max());
  int* p = alloc.allocate(10);
  ASSERT_NE(p, nullptr);
  alloc.deallocate(p, 10);
}
