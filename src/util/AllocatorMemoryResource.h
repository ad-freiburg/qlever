// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ALLOCATORMEMORYRESOURCE_H
#define QLEVER_SRC_UTIL_ALLOCATORMEMORYRESOURCE_H

#include <cstddef>
#include <memory>
#include <new>

#include "backports/memory_resource.h"
#include "util/Exception.h"

namespace ad_utility {

// A `memory_resource` that allocates via the allocator `Allocator` (e.g. an
// `AllocatorWithLimit`, such that all the memory that is allocated from this
// resource counts toward its memory limit). This makes such an allocator usable
// as the upstream of a `ql::pmr::monotonic_buffer_resource` or for containers
// that allocate via a `ql::pmr::polymorphic_allocator`.
//
// This adapter is needed because an allocator (like every allocator that
// follows the standard allocator interface) only allocates arrays of its
// `value_type`, with the alignment of that type, while a `memory_resource` has
// to allocate a number of bytes with an alignment that is only known at
// runtime. The allocation is therefore dispatched on the requested alignment to
// a copy of the allocator that is rebound to a type with exactly this
// alignment.
//
// NOTE: Like every `memory_resource`, this resource has to outlive all the
// memory that was allocated from it.
template <typename Allocator>
class AllocatorMemoryResource : public ql::pmr::memory_resource {
 public:
  // The largest supported alignment.
  static constexpr size_t maxAlignment = 4096;

 private:
  // The unit in which memory with the given `Alignment` is allocated.
  template <size_t Alignment>
  struct alignas(Alignment) Unit {
    std::byte bytes_[Alignment];
  };

  Allocator allocator_;

  // Return a copy of `allocator_` that allocates `Unit<Alignment>`s.
  template <size_t Alignment>
  auto reboundAllocator() const {
    using Rebound = typename std::allocator_traits<
        Allocator>::template rebind_alloc<Unit<Alignment>>;
    return Rebound{allocator_};
  }

  // The number of `Unit<Alignment>`s that are needed for `bytes` bytes.
  template <size_t Alignment>
  static size_t numUnits(size_t bytes) {
    return (bytes + Alignment - 1) / Alignment;
  }

  // Allocate `bytes` bytes with the given `alignment`, which must be a power of
  // two that is at least `Alignment`. The recursion turns the `alignment`,
  // which is only known at runtime, into a compile-time constant.
  template <size_t Alignment = 1>
  void* allocateImpl(size_t bytes, size_t alignment) {
    if (alignment == Alignment) {
      return reboundAllocator<Alignment>().allocate(numUnits<Alignment>(bytes));
    }
    if constexpr (Alignment < maxAlignment) {
      return allocateImpl<2 * Alignment>(bytes, alignment);
    } else {
      throw std::bad_alloc{};
    }
  }

  // Deallocate memory that was allocated by `allocateImpl` with the same
  // arguments.
  template <size_t Alignment = 1>
  void deallocateImpl(void* p, size_t bytes, size_t alignment) {
    if (alignment == Alignment) {
      reboundAllocator<Alignment>().deallocate(static_cast<Unit<Alignment>*>(p),
                                               numUnits<Alignment>(bytes));
    } else if constexpr (Alignment < maxAlignment) {
      deallocateImpl<2 * Alignment>(p, bytes, alignment);
    } else {
      AD_FAIL();
    }
  }

 public:
  explicit AllocatorMemoryResource(Allocator allocator)
      : allocator_{std::move(allocator)} {}

  const Allocator& allocator() const { return allocator_; }

 protected:
  // Throw if the allocator throws (e.g. the `AllocationExceedsLimitException`
  // of an `AllocatorWithLimit` if its memory limit would be exceeded) or if
  // `alignment` is larger than `maxAlignment`.
  void* do_allocate(size_t bytes, size_t alignment) override {
    return allocateImpl(bytes, alignment);
  }

  void do_deallocate(void* p, size_t bytes, size_t alignment) override {
    deallocateImpl(p, bytes, alignment);
  }

  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ALLOCATORMEMORYRESOURCE_H
