// Copyright 2018, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb@informatik.uni-freiburg.de>
// Author: Niklas Schnelle <schnelle@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HASHMAP_H
#define QLEVER_SRC_UTIL_HASHMAP_H

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <unordered_map>
#include <utility>

#include "backports/unordered.h"
#include "util/AllocatorWithLimit.h"

namespace ad_utility {
// Wrapper for HashMaps to be used everywhere throughout code for the semantic
// search. This wrapper interface is not designed to be complete from the
// beginning. Feel free to extend it at need. If available, use
// `boost::unordered_flat_map` (see `backports/unordered.h`), else
// `absl::flat_hash_map`. In both cases, the hash function defaults to
// `absl::Hash`.
#ifdef QLEVER_HAS_BOOST_UNORDERED
template <class K, class V, class HashFct = absl::DefaultHashContainerHash<K>,
          class EqualElem = absl::DefaultHashContainerEq<K>,
          class Alloc = std::allocator<std::pair<const K, V>>>
using HashMap = boost::unordered_flat_map<K, V, HashFct, EqualElem, Alloc>;
#else
template <typename... Ts>
using HashMap = absl::flat_hash_map<Ts...>;
#endif

// A HashMap with a memory limit. Note: We cannot use `absl::flat_hash_map`
// here, because it is inherently not exception safe, and the
// `AllocatorWithLimit` uses exceptions. `boost::unordered_flat_map` is
// exception safe, so we use it if available, and `std::unordered_map` else.
template <class K, class V, class HashFct = absl::DefaultHashContainerHash<K>,
          class EqualElem = absl::DefaultHashContainerEq<K>,
          class Alloc = ad_utility::AllocatorWithLimit<std::pair<const K, V>>>
#ifdef QLEVER_HAS_BOOST_UNORDERED
using HashMapWithMemoryLimit =
    boost::unordered_flat_map<K, V, HashFct, EqualElem, Alloc>;
#else
using HashMapWithMemoryLimit =
    std::unordered_map<K, V, HashFct, EqualElem, Alloc>;
#endif

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HASHMAP_H
