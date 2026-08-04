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

#include <cstddef>

#include "backports/memory_resource.h"
#include "util/AllocatorPmr.h"
#include "util/MemorySize/MemorySize.h"

using ad_utility::makePmrAllocatorFromResource;
using ad_utility::makePmrAllocatorWithLimit;
using ad_utility::MemorySize;
using ad_utility::PmrAllocator;

namespace {
using namespace ad_utility::memory_literals;

// A `ql::pmr::memory_resource` that forwards to the default resource and counts
// the requests that pass through it. Used to check that a
// `LimitedMemoryResource` really allocates from the upstream resource it was
// created with.
class CountingResource : public ql::pmr::memory_resource {
 public:
  size_t numAllocations_ = 0;
  size_t numDeallocations_ = 0;
  size_t numBytesAllocated_ = 0;

 protected:
  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    ++numAllocations_;
    numBytesAllocated_ += bytes;
    return ql::pmr::get_default_resource()->allocate(bytes, alignment);
  }
  void do_deallocate(void* p, std::size_t bytes,
                     std::size_t alignment) override {
    ++numDeallocations_;
    ql::pmr::get_default_resource()->deallocate(p, bytes, alignment);
  }
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};
}  // namespace

// `.as<U>()` keeps using the same underlying PMR resource.
TEST(AllocatorPmr, AsSharesResourcePointer) {
  auto allocInt = makePmrAllocatorWithLimit<int>(8_B);
  auto allocChar = allocInt.as<char>();
  EXPECT_EQ(allocInt.resource(), allocChar.resource());
}

// ____________________________________________________________________________
// A `LimitedMemoryResource` created with a custom upstream resource forwards
// all its allocations to that upstream, while still enforcing the memory limit
// independently of the upstream's own capacity.
TEST(AllocatorPmr, CustomUpstreamResource) {
  static_assert(sizeof(int) == 4);
  CountingResource upstream;
  auto alloc = makePmrAllocatorWithLimit<int>(8_B, &upstream);

  int* p = alloc.allocate(2);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(upstream.numAllocations_, 1u);
  EXPECT_EQ(upstream.numBytesAllocated_, 8u);
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);

  // Exceeding the limit throws without ever reaching the upstream.
  EXPECT_THROW(alloc.allocate(1),
               ad_utility::detail::AllocationExceedsLimitException);
  EXPECT_EQ(upstream.numAllocations_, 1u);

  alloc.deallocate(p, 2);
  EXPECT_EQ(upstream.numDeallocations_, 1u);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);
}

// ____________________________________________________________________________
// A `nullptr` upstream means "use the default resource".
TEST(AllocatorPmr, NullptrUpstreamUsesDefaultResource) {
  static_assert(sizeof(int) == 4);
  auto alloc = makePmrAllocatorWithLimit<int>(8_B, nullptr);
  int* p = alloc.allocate(2);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);
  alloc.deallocate(p, 2);
  EXPECT_EQ(alloc.amountMemoryLeft(), 8_B);
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
