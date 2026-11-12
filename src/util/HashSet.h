// Copyright 2011 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchholb@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HASHSET_H
#define QLEVER_SRC_UTIL_HASHSET_H

#include <absl/container/hash_container_defaults.h>

#include <memory>

#include "backports/unordered.h"
#include "util/AllocatorWithLimit.h"

namespace ad_utility {
// Wrapper for HashSets (with elements of type T) to be used everywhere
// throughout code for the semantic search. This wrapper interface is not
// designed to be complete from the beginning. Feel free to extend it at need.
// T is the element type. The underlying hash set is `boost::unordered_flat_set`
// if available, and `std::unordered_set` else (see `backports/unordered.h`).
// The hash function defaults to `absl::Hash`.
template <class T, class HashFct = absl::DefaultHashContainerHash<T>,
          class EqualElem = absl::DefaultHashContainerEq<T>,
          class Alloc = std::allocator<T>>
using HashSet = ql::FlatHashSetTemplate<T, HashFct, EqualElem, Alloc>;

// A hash set (with elements of type T) with a memory Limit.
// Note: We cannot use `absl::flat_hash_set`
// here, because it is inherently not exception safe, and the
// `AllocatorWithLimit` uses exceptions. Both `boost::unordered_flat_set` and
// `std::unordered_set` are exception safe.
template <class T, class HashFct = absl::DefaultHashContainerHash<T>,
          class EqualElem = absl::DefaultHashContainerEq<T>,
          class Alloc = ad_utility::AllocatorWithLimit<T>>
using HashSetWithMemoryLimit =
    ql::FlatHashSetTemplate<T, HashFct, EqualElem, Alloc>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HASHSET_H
