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

#include "backports/memory_resource.h"
#include "util/AllocatorPmr.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::makePmrAllocatorFromResource;
using ad_utility::makePmrAllocatorWithLimit;
using ad_utility::MemorySize;
using ad_utility::PmrAllocator;

namespace {
using namespace ad_utility::memory_literals;
}

// `.as<U>()` keeps using the same underlying PMR resource.
TEST(AllocatorPmr, AsSharesResourcePointer) {
  auto allocInt = makePmrAllocatorWithLimit<int>(8_B);
  auto allocChar = allocInt.as<char>();
  EXPECT_EQ(allocInt.resource(), allocChar.resource());
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
