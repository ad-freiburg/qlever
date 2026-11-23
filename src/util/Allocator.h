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
// Implements the type `qlever::Allocator`, as well as the functions
// `qlever::makeUnlimitedAllocator` and `qlever::makeAllocatorWithLimit` which
// both return `Allocator`. The actual implementation of `Allocator` can be
// specified at compile time via CMake from the following:
//
//   * LIMIT (default): `ad_utility::allocatorImpl::AllocatorWithLimit<T>` - the
//     historical behaviour, a strongly-typed allocator that enforces a global
//     memory limit. The legacy name of that implementation,
//     `ad_utility::AllocatorWithLimit<T>` is now an alias for
//     `qlever::Allocator<T>` (see `util/AllocatorWithLimit.h`).
//   * PMR: a `ql::pmr::memory_resource`-based allocator
//     (`ad_utility::PmrAllocator<T>`). By default it keeps the same
//     memory-limit semantics as the LIMIT implementation via a
//     `LimitedMemoryResource`.
//
// Selection:
//   * Define `QLEVER_USE_PMR_ALLOCATOR` (e.g. via the CMake option
//     `QLEVER_ALLOCATOR_BACKEND=pmr`) to select the PMR backend.
//
// NOTE: This header must *not* include `util/AllocatorWithLimit.h`, because
// that header is a compatibility shim that includes *this* header. Each backend
// therefore includes its concrete implementation header directly.

#include <type_traits>
#include <utility>
#include <vector>

#include "util/memorySize/MemorySize.h"

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

// True iff `T` is an instantiation of `qlever::Allocator`.
//
// NOTE: This cannot be expressed via `isInstantiation`, because clang rejects
// the pack expansion for the alias template `qlever::Allocator`.
template <typename T, typename = void>
inline constexpr bool isQleverAllocator = false;
template <typename T>
inline constexpr bool
    isQleverAllocator<T, std::void_t<typename T::value_type>> =
        std::is_same_v<T, Allocator<typename T::value_type>>;

static_assert(isQleverAllocator<Allocator<int>>);
static_assert(!isQleverAllocator<int>);
// Has a `value_type`, but is not an allocator.
static_assert(!isQleverAllocator<std::vector<int>>);

template <typename T>
Allocator<T> makeUnlimitedAllocator() {
  return Allocator<T>::makeUnlimited();
}

template <typename T>
Allocator<T> makeAllocatorWithLimit(
    ad_utility::MemorySize limit,
    ad_utility::ClearOnAllocation c = ad_utility::noop) {
  return Allocator<T>::makeLimited(limit, std::move(c));
}

}  // namespace qlever

#endif  // QLEVER_SRC_UTIL_ALLOCATOR_H
