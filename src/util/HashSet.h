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
// The hash set (with elements of type `T`) that is used everywhere throughout
// the code. The underlying hash set is `boost::unordered_flat_set` if
// available, and otherwise `std::unordered_set` (see `backports/unordered.h`).
// The hash function defaults to `absl::Hash`.
template <class T, class HashFct = absl::DefaultHashContainerHash<T>,
          class EqualElem = absl::DefaultHashContainerEq<T>,
          class Alloc = std::allocator<T>>
using HashSet = ql::FlatHashSetTemplate<T, HashFct, EqualElem, Alloc>;

// A `HashSet` with a memory limit.
//
// NOTE: The `AllocatorWithLimit` throws when the limit is exceeded, so the
// underlying hash set has to be exception safe, which
// `boost::unordered_flat_set` and `std::unordered_set` are, but
// `absl::flat_hash_set` is not.
template <class T, class HashFct = absl::DefaultHashContainerHash<T>,
          class EqualElem = absl::DefaultHashContainerEq<T>,
          class Alloc = ad_utility::AllocatorWithLimit<T>>
using HashSetWithMemoryLimit =
    ql::FlatHashSetTemplate<T, HashFct, EqualElem, Alloc>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HASHSET_H
