// Copyright 2011 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchholb@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_HASHSET_H
#define QLEVER_SRC_UTIL_HASHSET_H

#include <absl/container/flat_hash_set.h>

#include <memory>
#include <unordered_set>
#include <utility>

#include "backports/unordered.h"
#include "util/AllocatorWithLimit.h"

namespace ad_utility {
// Wrapper for HashSets (with elements of type T) to be used everywhere
// throughout code for the semantic search. This wrapper interface is not
// designed to be complete from the beginning. Feel free to extend it at need.
// T is the element type. If available, use `boost::unordered_flat_set` (see
// `backports/unordered.h`), else `absl::flat_hash_set`. In both cases, the hash
// function defaults to `absl::Hash`.
template <class T,
          class HashFct = absl::container_internal::hash_default_hash<T>,
          class EqualElem = absl::container_internal::hash_default_eq<T>,
          class Alloc = std::allocator<T>>
#ifdef QLEVER_HAS_BOOST_UNORDERED
using HashSet = boost::unordered_flat_set<T, HashFct, EqualElem, Alloc>;
#else
using HashSet = absl::flat_hash_set<T, HashFct, EqualElem, Alloc>;
#endif

// A hash set (with elements of type T) with a memory Limit.
// Note: We cannot use `absl::flat_hash_set`
// here, because it is inherently not exception safe, and the
// `AllocatorWithLimit` uses exceptions. `boost::unordered_flat_set` is
// exception safe, so we use it if available, and `std::unordered_set` else.
template <class T,
          class HashFct = absl::container_internal::hash_default_hash<T>,
          class EqualElem = absl::container_internal::hash_default_eq<T>,
          class Alloc = ad_utility::AllocatorWithLimit<T>>
#ifdef QLEVER_HAS_BOOST_UNORDERED
using HashSetWithMemoryLimit =
    boost::unordered_flat_set<T, HashFct, EqualElem, Alloc>;
#else
using HashSetWithMemoryLimit = std::unordered_set<T, HashFct, EqualElem, Alloc>;
#endif

}  // namespace ad_utility

#ifdef QLEVER_HAS_BOOST_UNORDERED
namespace boost::unordered {
// Make `boost::unordered_flat_set` hashable with `absl::Hash` (found via ADL),
// in the same way as `absl::flat_hash_set`. This is required for example for
// an `ad_utility::HashSet<ad_utility::HashSet<T>>`.
template <typename H, typename T, typename Hash, typename Eq, typename Alloc>
H AbslHashValue(H h, const unordered_flat_set<T, Hash, Eq, Alloc>& set) {
  return H::combine(H::combine_unordered(std::move(h), set.begin(), set.end()),
                    set.size());
}
}  // namespace boost::unordered
#endif

#endif  // QLEVER_SRC_UTIL_HASHSET_H
