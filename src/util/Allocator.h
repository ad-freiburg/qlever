#ifndef QLEVER_SRC_UTIL_ALLOCATOR_H
#define QLEVER_SRC_UTIL_ALLOCATOR_H

// QLever allocator seam.
//
// `qlever::Allocator<T>` and `qlever::makeDefaultAllocator<T>()` are the single
// point through which the engine's allocations are routed. The concrete
// allocator backend is selected at *compile time*:
//
//   * LIMIT (default): `ad_utility::allocatorImpl::AllocatorWithLimit<T>` - the
//     historical behaviour, a stateful allocator that enforces a global memory
//     limit. The legacy public name `ad_utility::AllocatorWithLimit<T>` is kept
//     valid as an alias for `qlever::Allocator<T>` (see
//     `util/AllocatorWithLimit.h`).
//   * PMR: a `std::pmr::memory_resource`-based allocator
//     (`ad_utility::PmrAllocatorWithLimit<T>`). By default it keeps the same
//     memory-limit semantics via a `LimitedMemoryResource`.
//
// Selection:
//   * Define `QLEVER_USE_PMR_ALLOCATOR` (e.g. via the CMake option
//     `QLEVER_ALLOCATOR_BACKEND=pmr`) to select the PMR backend.
//
// NOTE: This header must *not* include `util/AllocatorWithLimit.h`, because that
// header is a compatibility shim that includes *this* header. Each backend
// therefore includes its concrete implementation header directly.

#include <utility>

#include "util/MemorySize/MemorySize.h"

#ifdef QLEVER_USE_PMR_ALLOCATOR

#include "util/AllocatorPmr.h"

namespace qlever {

template <typename T>
using Allocator = ad_utility::PmrAllocatorWithLimit<T>;

// Unlimited allocator over new/delete (default). This is the factory used by
// the default `Qlever` construction path.
template <typename T>
Allocator<T> makeDefaultAllocator() {
  return ad_utility::makeUnlimitedPmrAllocator<T>();
}

// Create an allocator that enforces the given memory limit (over new/delete).
template <typename T>
Allocator<T> makeAllocatorWithLimit(ad_utility::MemorySize limit) {
  return ad_utility::makePmrAllocatorWithLimit<T>(limit);
}

}  // namespace qlever

#else  // LIMIT backend (default)

#include "util/AllocatorWithLimitImpl.h"

namespace qlever {

template <typename T>
using Allocator = ad_utility::allocatorImpl::AllocatorWithLimit<T>;

template <typename T>
Allocator<T> makeDefaultAllocator() {
  return ad_utility::allocatorImpl::makeUnlimitedAllocator<T>();
}

// Create an allocator that enforces the given memory limit.
template <typename T>
Allocator<T> makeAllocatorWithLimit(ad_utility::MemorySize limit) {
  return ad_utility::allocatorImpl::makeAllocatorWithLimit<T>(limit);
}

template <typename T, typename... Args>
Allocator<T> makeDefaultAllocator(Args&&... args) {
  return ad_utility::allocatorImpl::AllocatorWithLimit<T>{AD_FWD(args)...};
}

}  // namespace qlever

#endif  // QLEVER_USE_PMR_ALLOCATOR

#endif  // QLEVER_SRC_UTIL_ALLOCATOR_H
