// Copyright 2011 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchholb@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <string>

#include "absl/container/flat_hash_set.h"
#include "util/AllocatorWithLimit.h"

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
