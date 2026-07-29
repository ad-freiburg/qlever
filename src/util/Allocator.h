// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ALLOCATOR_H
#define QLEVER_SRC_UTIL_ALLOCATOR_H

// QLever allocator seam.
//
// `qlever::Allocator<T>`, `qlever::makeUnlimitedAllocator<T>()`, and
// `qlever::makeAllocatorWithLimit<T>(limit[, clearOnAllocation])` are the
// single point through which the engine's allocations are routed. The concrete
// allocator backend is selected at *compile time*:
//
//   * LIMIT (default): `ad_utility::allocatorImpl::AllocatorWithLimit<T>` - the
//     historical behaviour, a stateful allocator that enforces a global memory
//     limit. The legacy public name `ad_utility::AllocatorWithLimit<T>` is kept
//     valid as an alias for `qlever::Allocator<T>` (see
//     `util/AllocatorWithLimit.h`).
//   * PMR: a `ql::pmr::memory_resource`-based allocator
//     (`ad_utility::PmrAllocator<T>`). By default it keeps the same
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
using Allocator = ad_utility::PmrAllocator<T>;

#else  // LIMIT backend (default)

#include "util/AllocatorWithLimitImpl.h"

namespace qlever {

template <typename T>
using Allocator = ad_utility::allocatorImpl::AllocatorWithLimit<T>;

#endif  // QLEVER_USE_PMR_ALLOCATOR

template <typename T>
Allocator<T> makeUnlimitedAllocator() {
  return Allocator<T>::makeUnlimited();
}

template <typename T>
Allocator<T> makeAllocatorWithLimit(
    ad_utility::MemorySize limit,
    ad_utility::ClearOnAllocation c = ad_utility::noClearOnAllocation) {
  return Allocator<T>::makeLimited(limit, std::move(c));
}

}  // namespace qlever

#endif  // QLEVER_SRC_UTIL_ALLOCATOR_H
