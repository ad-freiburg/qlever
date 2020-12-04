// Copyright 2018, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb@informatik.uni-freiburg.de>
// Author: Niklas Schnelle <schnelle@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/flat_hash_map.h>

namespace ad_utility {
// Wrapper for HashMaps to be used everywhere throughout code for the semantic
// search. This wrapper interface is not designed to be complete from the
// beginning. Feel free to extend it at need.
template <class K, class V,
          class HashFcn = absl::container_internal::hash_default_hash<K>,
          class EqualKey = absl::container_internal::hash_default_eq<K>,
          class Alloc = std::allocator<std::pair<const K, V>>>
using HashMap = absl::flat_hash_map<K, V, HashFcn, EqualKey, Alloc>;
}  // namespace ad_utility
