// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/AllocationCounter.h"

// Without `QLEVER_COUNT_ALLOCATIONS` this translation unit is empty.
#ifdef QLEVER_COUNT_ALLOCATIONS

#include <atomic>
#include <cstdlib>
#include <new>

namespace {
std::atomic<uint64_t> numAllocations{0};
std::atomic<uint64_t> numBytes{0};

// Count one allocation of `size` bytes and allocate it with `alignment` (0 for
// the default alignment of `malloc`). Return `nullptr` on failure.
void* countedAllocate(std::size_t size, std::size_t alignment) noexcept {
  numAllocations.fetch_add(1, std::memory_order_relaxed);
  numBytes.fetch_add(size, std::memory_order_relaxed);
  if (size == 0) {
    size = 1;
  }
  if (alignment == 0) {
    return std::malloc(size);
  }
  // `aligned_alloc` requires the size to be a multiple of the alignment.
  size = (size + alignment - 1) / alignment * alignment;
  return std::aligned_alloc(alignment, size);
}

void* countedAllocateOrThrow(std::size_t size, std::size_t alignment) {
  void* ptr = countedAllocate(size, alignment);
  if (ptr == nullptr) {
    throw std::bad_alloc{};
  }
  return ptr;
}
}  // namespace

namespace ad_utility::allocationCounter {
Counts current() {
  return {numAllocations.load(std::memory_order_relaxed),
          numBytes.load(std::memory_order_relaxed)};
}
}  // namespace ad_utility::allocationCounter

// Replacements of the global allocation functions. The default deallocation
// functions release memory with `std::free`, which matches `malloc` and
// `aligned_alloc` above.
void* operator new(std::size_t size) { return countedAllocateOrThrow(size, 0); }
void* operator new[](std::size_t size) {
  return countedAllocateOrThrow(size, 0);
}
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  return countedAllocate(size, 0);
}
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return countedAllocate(size, 0);
}
void* operator new(std::size_t size, std::align_val_t alignment) {
  return countedAllocateOrThrow(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
  return countedAllocateOrThrow(size, static_cast<std::size_t>(alignment));
}
void* operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept {
  return countedAllocate(size, static_cast<std::size_t>(alignment));
}
void* operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
  return countedAllocate(size, static_cast<std::size_t>(alignment));
}

#endif  // QLEVER_COUNT_ALLOCATIONS
