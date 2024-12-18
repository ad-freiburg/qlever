// Copyright 2018, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb@informatik.uni-freiburg.de>
// Author: Niklas Schnelle <schnelle@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/flat_hash_map.h>
#include <util/AllocatorWithLimit.h>

namespace ad_utility {
// Wrapper for HashMaps to be used everywhere throughout code for the semantic
// search. This wrapper interface is not designed to be complete from the
// beginning. Feel free to extend it at need.
template <typename... Ts>
using HashMap = absl::flat_hash_map<Ts...>;

// A HashMap with a memory limit.
template <class K, class V,
          class HashFct = absl::container_internal::hash_default_hash<K>,
          class EqualElem = absl::container_internal::hash_default_eq<K>,
          class Alloc = ad_utility::AllocatorWithLimit<std::pair<const K, V>>>
using HashMapWithMemoryLimit = HashMap<K, V, HashFct, EqualElem, Alloc>;
}  // namespace ad_utility
