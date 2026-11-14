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
// Wrapper for HashMaps to be used everywhere throughout code for the semantic
// search. This wrapper interface is not designed to be complete from the
// beginning. Feel free to extend it at need. The underlying hash map is
// `boost::unordered_flat_map` if available, and `std::unordered_map` else (see
// `backports/unordered.h`). The hash function defaults to `absl::Hash`.
template <class K, class V, class HashFct = absl::DefaultHashContainerHash<K>,
          class EqualElem = absl::DefaultHashContainerEq<K>,
          class Alloc = std::allocator<std::pair<const K, V>>>
using HashMap = ql::FlatHashMapTemplate<K, V, HashFct, EqualElem, Alloc>;

// A HashMap with a memory limit. Note: We cannot use `absl::flat_hash_map`
// here, because it is inherently not exception safe, and the
// `AllocatorWithLimit` uses exceptions. Both `boost::unordered_flat_map` and
// `std::unordered_map` are exception safe.
template <class K, class V, class HashFct = absl::DefaultHashContainerHash<K>,
          class EqualElem = absl::DefaultHashContainerEq<K>,
          class Alloc = ad_utility::AllocatorWithLimit<std::pair<const K, V>>>
using HashMapWithMemoryLimit =
    ql::FlatHashMapTemplate<K, V, HashFct, EqualElem, Alloc>;

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HASHMAP_H
