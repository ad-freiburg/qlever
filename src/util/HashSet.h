// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <absl/container/flat_hash_set.h>

#include <string>

using std::string;

namespace ad_utility {
// Wrapper for HashSets to be used everywhere throughout code for
// the semantic search. This wrapper interface is not designed to
// be complete from the beginning. Feel free to extend it at need.
template <class K,
          class HashFcn = absl::container_internal::hash_default_hash<K>,
          class EqualKey = absl::container_internal::hash_default_eq<K>,
          class Alloc = std::allocator<K>>
using HashSet = absl::flat_hash_set<K, HashFcn, EqualKey, Alloc>;
}  // namespace ad_utility
