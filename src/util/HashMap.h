// Copyright 2018, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb@informatik.uni-freiburg.de>
// Author: Niklas Schnelle <schnelle@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HASHMAP_H
#define QLEVER_SRC_UTIL_HASHMAP_H

#include <absl/container/hash_container_defaults.h>

#include <memory>
#include <utility>

#include "backports/unordered.h"
#include "util/AllocatorWithLimit.h"

namespace ad_utility {
// The hash map that is used everywhere throughout the code. The underlying
// hash map is `boost::unordered_flat_map` if available, and otherwise
// `std::unordered_map` (see `backports/unordered.h`). The hash function
// defaults to `absl::Hash`.
template <class K, class V, class HashFct = absl::DefaultHashContainerHash<K>,
          class EqualElem = absl::DefaultHashContainerEq<K>,
          class Alloc = std::allocator<std::pair<const K, V>>>
using HashMap = ql::FlatHashMapTemplate<K, V, HashFct, EqualElem, Alloc>;

// A `HashMap` with a memory limit.
//
// NOTE: The `AllocatorWithLimit` throws when the limit is exceeded, so the
// underlying hash map has to be exception safe, which
// `boost::unordered_flat_map` and `std::unordered_map` are, but
// `absl::flat_hash_map` is not.
template <class K, class V, class HashFct = absl::DefaultHashContainerHash<K>,
          class EqualElem = absl::DefaultHashContainerEq<K>,
          class Alloc = ad_utility::AllocatorWithLimit<std::pair<const K, V>>>
using HashMapWithMemoryLimit =
    ql::FlatHashMapTemplate<K, V, HashFct, EqualElem, Alloc>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HASHMAP_H
