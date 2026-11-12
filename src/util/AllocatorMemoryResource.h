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

#include "backports/memory_resource.h"
#include "util/AllocatorWithLimit.h"
#include "util/Exception.h"

namespace ad_utility {

// A `memory_resource` that allocates via an `AllocatorWithLimit` (e.g. the one
// of the `QueryExecutionContext`), such that all the memory that is allocated
// from this resource counts toward the memory limit of that allocator. This
// makes the allocator usable as the upstream of a
// `ql::pmr::monotonic_buffer_resource` or for containers that allocate via a
// `ql::pmr::polymorphic_allocator`.
//
// NOTE: Like every `memory_resource`, this resource has to outlive all the
// memory that was allocated from it.
class AllocatorMemoryResource : public ql::pmr::memory_resource {
 public:
  // The memory is allocated in units of `std::max_align_t`, which supports
  // every alignment that `operator new` supports without an explicit
  // alignment.
  using Unit = std::max_align_t;
  using Allocator = AllocatorWithLimit<Unit>;

 private:
  Allocator allocator_;

  static size_t numUnits(size_t bytes) {
    return (bytes + sizeof(Unit) - 1) / sizeof(Unit);
  }

 public:
  explicit AllocatorMemoryResource(Allocator allocator)
      : allocator_{std::move(allocator)} {}

  const Allocator& allocator() const { return allocator_; }

 protected:
  // Throw an `AllocationExceedsLimitException` if the memory limit of the
  // allocator would be exceeded.
  void* do_allocate(size_t bytes, size_t alignment) override {
    AD_CONTRACT_CHECK(alignment <= alignof(Unit));
    return allocator_.allocate(numUnits(bytes));
  }

  void do_deallocate(void* p, size_t bytes,
                     [[maybe_unused]] size_t alignment) override {
    allocator_.deallocate(static_cast<Unit*>(p), numUnits(bytes));
  }

  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ALLOCATORMEMORYRESOURCE_H
