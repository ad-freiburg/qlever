// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <tuple>
#include <utility>
#include <vector>

#include "backports/memory_resource.h"
#include "util/AllocatorMemoryResource.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemoryLimitTracker.h"
#include "util/memorySize/MemorySize.h"

namespace {

using namespace ad_utility::memory_literals;
using ::testing::ElementsAre;
using ::testing::Pair;

// The number of bytes and the alignment of an allocation or deallocation.
using Event = std::pair<size_t, size_t>;

// An allocator that records the size and the alignment of each allocation and
// deallocation.
template <typename T>
struct RecordingAllocator {
  using value_type = T;
  std::shared_ptr<std::vector<Event>> allocations_ =
      std::make_shared<std::vector<Event>>();
  std::shared_ptr<std::vector<Event>> deallocations_ =
      std::make_shared<std::vector<Event>>();

  RecordingAllocator() = default;
  template <typename U>
  explicit RecordingAllocator(const RecordingAllocator<U>& other)
      : allocations_{other.allocations_},
        deallocations_{other.deallocations_} {}

  T* allocate(size_t n) {
    allocations_->emplace_back(n * sizeof(T), alignof(T));
    return std::allocator<T>{}.allocate(n);
  }
  void deallocate(T* p, size_t n) {
    deallocations_->emplace_back(n * sizeof(T), alignof(T));
    std::allocator<T>{}.deallocate(p, n);
  }
};

// Return true iff `p` is aligned to `alignment`.
bool isAligned(const void* p, size_t alignment) {
  return reinterpret_cast<std::uintptr_t>(p) % alignment == 0;
}
}  // namespace

// _____________________________________________________________________________
TEST(AllocatorMemoryResource, everyAlignmentIsPassedToTheAllocator) {
  RecordingAllocator<std::byte> allocator;
  ad_utility::AllocatorMemoryResource<RecordingAllocator<std::byte>> resource{
      allocator};
  for (size_t alignment = 1; alignment <= decltype(resource)::maxAlignment;
       alignment *= 2) {
    allocator.allocations_->clear();
    allocator.deallocations_->clear();
    // The size is rounded up to a multiple of the alignment.
    void* p = resource.allocate(2 * alignment + 1, alignment);
    EXPECT_TRUE(isAligned(p, alignment)) << alignment;
    resource.deallocate(p, 2 * alignment + 1, alignment);
    EXPECT_THAT(*allocator.allocations_,
                ElementsAre(Pair(3 * alignment, alignment)));
    EXPECT_THAT(*allocator.deallocations_,
                ElementsAre(Pair(3 * alignment, alignment)));
  }
}

// _____________________________________________________________________________
TEST(AllocatorMemoryResource, tooLargeAlignmentThrows) {
  ad_utility::AllocatorMemoryResource<RecordingAllocator<std::byte>> resource{
      RecordingAllocator<std::byte>{}};
  EXPECT_THROW(
      std::ignore = resource.allocate(8, 2 * decltype(resource)::maxAlignment),
      std::bad_alloc);
  // Memory with such an alignment can't have been allocated by the resource
  // (the pointer is never dereferenced).
  std::byte memory[8];
  EXPECT_ANY_THROW(
      resource.deallocate(memory, 8, 2 * decltype(resource)::maxAlignment));
}

// _____________________________________________________________________________
TEST(AllocatorMemoryResource, isEqualOnlyToItself) {
  using Resource =
      ad_utility::AllocatorMemoryResource<RecordingAllocator<std::byte>>;
  RecordingAllocator<std::byte> allocator;
  Resource resource{allocator};
  Resource other{allocator};
  EXPECT_TRUE(resource.is_equal(resource));
  EXPECT_FALSE(resource.is_equal(other));
  EXPECT_FALSE(other.is_equal(resource));
}

// _____________________________________________________________________________
TEST(AllocatorMemoryResource, memoryLimitIsRespected) {
  auto allocator = ad_utility::makeAllocatorWithLimit<std::byte>(10_kB);
  ad_utility::AllocatorMemoryResource<ad_utility::AllocatorWithLimit<std::byte>>
      resource{allocator};
  EXPECT_TRUE(resource.allocator() == allocator);
  EXPECT_THROW(std::ignore = resource.allocate(20'000, 16),
               ad_utility::detail::AllocationExceedsLimitException);

  // The memory can be used by a monotonic buffer resource, and is returned to
  // the allocator when the buffer resource is destroyed.
  auto memoryLeftBefore = allocator.amountMemoryLeft();
  {
    ql::pmr::monotonic_buffer_resource arena{&resource};
    void* p = arena.allocate(5'000, 64);
    EXPECT_TRUE(isAligned(p, 64));
    EXPECT_LE(allocator.amountMemoryLeft(), memoryLeftBefore - 5_kB);
  }
  EXPECT_EQ(allocator.amountMemoryLeft(), memoryLeftBefore);
}
