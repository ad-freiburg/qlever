// Copyright 2020, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (April of 2020,
// kalmbach@informatik.uni-freiburg.de)

#ifndef QLEVER_SRC_UTIL_ALLOCATORWITHLIMIT_H
#define QLEVER_SRC_UTIL_ALLOCATORWITHLIMIT_H

// Backwards-compatibility shim.
//
// Historically `ad_utility::AllocatorWithLimit<T>` was the single allocator
// used throughout QLever. It is now one of several interchangeable *backends*
// selected at compile time behind the `qlever::Allocator<T>` seam (see
// `util/Allocator.h`): the historical, memory-limit enforcing allocator
// (default) or a `std::pmr`-based allocator that lets the target platform
// inject its own memory pools.
//
// To avoid touching the ~100 call sites that refer to the legacy names, the
// public name `ad_utility::AllocatorWithLimit<T>` (and the free functions
// `makeAllocatorWithLimit` / `makeUnlimitedAllocator`) are kept valid here as
// thin aliases/forwarders for the seam. The concrete implementation of the
// limit-based backend now lives in `util/AllocatorWithLimitImpl.h` in the
// namespace `ad_utility::allocatorImpl`.
//
// New code should prefer the `qlever::Allocator<T>` seam directly.

#include "util/Allocator.h"
#include "util/AllocatorWithLimitImpl.h"

namespace ad_utility {

// `ad_utility::AllocatorWithLimit<T>` is now an alias for the compile-time
// selected `qlever::Allocator<T>` backend. In the default (limit) build this
// resolves to `ad_utility::allocatorImpl::AllocatorWithLimit<T>` and therefore
// behaves exactly as before.
template <typename T>
using AllocatorWithLimit = qlever::Allocator<T>;

// Return a new allocator with the specified limit (forwards to the seam).
template <typename T>
AllocatorWithLimit<T> makeAllocatorWithLimit(MemorySize limit) {
  return qlever::makeAllocatorWithLimit<T>(limit);
}

// Return a new allocator with the maximal possible limit (forwards to the
// seam's default, unlimited allocator).
template <typename T>
AllocatorWithLimit<T> makeUnlimitedAllocator() {
  return qlever::makeDefaultAllocator<T>();
}

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_ALLOCATORWITHLIMIT_H
