// Copyright 2018, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb@informatik.uni-freiburg.de>
// Author: Niklas Schnelle <schnelle@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HASHMAP_H
#define QLEVER_SRC_UTIL_HASHMAP_H

#include <absl/container/flat_hash_map.h>

#include <boost/optional.hpp>
#include <unordered_map>

#include "util/AllocatorWithLimit.h"

namespace ad_utility {
// Wrapper for HashMaps to be used everywhere throughout code for the semantic
// search. This wrapper interface is not designed to be complete from the
// beginning. Feel free to extend it at need.
template <typename... Ts>
using HashMap = absl::flat_hash_map<Ts...>;

// A HashMap with a memory limit. Note: We cannot use `absl::flat_hash_map`
// here, because it is inherently not exception safe, and the
// `AllocatorWithLimit` uses exceptions.
template <class K, class V,
          class HashFct = absl::container_internal::hash_default_hash<K>,
          class EqualElem = absl::container_internal::hash_default_eq<K>,
          class Alloc = ad_utility::AllocatorWithLimit<std::pair<const K, V>>>
using HashMapWithMemoryLimit =
    std::unordered_map<K, V, HashFct, EqualElem, Alloc>;

// Look up `key` in `map`. Returns a `boost::optional` reference to the mapped
// value if found, or `boost::none` otherwise.
template <typename Map, typename Key>
auto findOptionalFromHashMap(const Map& map, const Key& key)
    -> boost::optional<const typename Map::mapped_type&> {
  auto it = map.find(key);
  if (it != map.end()) {
    return it->second;
  }
  return {};
}
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_HASHMAP_H
