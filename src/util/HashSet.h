// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <absl/container/flat_hash_set.h>

#include <string>

#include "./AllocatorWithLimit.h"

using std::string;

namespace ad_utility {
// Wrapper for HashSets (with elements of type T) to be used everywhere
// throughout code for the semantic search. This wrapper interface is not
// designed to be complete from the beginning. Feel free to extend it at need.
// T is the element type.
template <class T,
          class HashFct = absl::container_internal::hash_default_hash<T>,
          class EqualElem = absl::container_internal::hash_default_eq<T>,
          class Alloc = std::allocator<T>>
using HashSet = absl::flat_hash_set<T, HashFct, EqualElem, Alloc>;

// A hash set (with elements of type T) with a memory Limit.
template <class T,
          class HashFct = absl::container_internal::hash_default_hash<T>,
          class EqualElem = absl::container_internal::hash_default_eq<T>,
          class Alloc = ad_utility::AllocatorWithLimit<T>>
using HashSetWithMemoryLimit =
    absl::flat_hash_set<T, HashFct, EqualElem, Alloc>;

}  // namespace ad_utility
