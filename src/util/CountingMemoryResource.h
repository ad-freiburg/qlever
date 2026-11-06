// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_COUNTINGMEMORYRESOURCE_H
#define QLEVER_SRC_UTIL_COUNTINGMEMORYRESOURCE_H

#include <cstddef>

#include "backports/memory_resource.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility {

// A `memory_resource` that forwards all allocations to an upstream resource
// and keeps track of the number of bytes that are currently allocated through
// it. It is used to measure the memory that a set of data structures (which
// all use this resource via a `polymorphic_allocator`) currently holds.
//
// NOTE: This class is deliberately *not* thread-safe (the counter is a plain
// `size_t`), which makes it as cheap as possible. It may be used from several
// threads one after the other, as long as the accesses are properly
// synchronized, but never concurrently. In contrast to
// `LimitedMemoryResource` (see `AllocatorPmr.h`), it has no limit (and hence
// never throws on its own), needs no atomics, and reports the used bytes.
class CountingMemoryResource : public ql::pmr::memory_resource,
                               public NoCopyNoMove {
 private:
  ql::pmr::memory_resource* upstream_;
  size_t numBytesAllocated_ = 0;

 public:
  // Construct with the given `upstream` resource, which must outlive this
  // resource.
  explicit CountingMemoryResource(
      ql::pmr::memory_resource* upstream = ql::pmr::new_delete_resource())
      : upstream_{upstream} {
    AD_CONTRACT_CHECK(upstream_ != nullptr);
  }

  // Return the number of bytes that are currently allocated through this
  // resource, i.e. the bytes of all allocations minus those of all
  // deallocations.
  size_t numBytesAllocated() const { return numBytesAllocated_; }

 private:
  // Forward to the upstream resource and count the allocated bytes.
  void* do_allocate(size_t bytes, size_t alignment) override {
    void* result = upstream_->allocate(bytes, alignment);
    numBytesAllocated_ += bytes;
    return result;
  }

  // Forward to the upstream resource and subtract the deallocated bytes.
  void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
    upstream_->deallocate(ptr, bytes, alignment);
    AD_CORRECTNESS_CHECK(numBytesAllocated_ >= bytes);
    numBytesAllocated_ -= bytes;
  }

  // Two `CountingMemoryResource`s are only equal if they are the same object,
  // because memory has to be deallocated via the resource that counted its
  // allocation.
  bool do_is_equal(
      const ql::pmr::memory_resource& other) const noexcept override {
    return this == &other;
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_COUNTINGMEMORYRESOURCE_H
