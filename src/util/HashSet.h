// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <absl/container/flat_hash_set.h>
#include <string>
#include "./DefaultKeyProvider.h"

using std::string;

namespace ad_utility {
// Wrapper for HashSets to be used everywhere throughout code for
// the semantic search. This wrapper interface is not designed to
// be complete from the beginning. Feel free to extend it at need.
//
// The current version uses google's dense_hash_set and directly exports its
// functions. It only adds a constructor which automatically sets the empty and
// deleted keys.  This may be changed in the future.
template <class T,
          class HashFcn = absl::container_internal::hash_default_hash<T>,
          class EqualKey = absl::container_internal::hash_default_eq<T>,
          class Alloc = std::allocator<T>>
class HashSet : private absl::flat_hash_set<T, HashFcn, EqualKey, Alloc> {
  using Base = absl::flat_hash_set<T, HashFcn, EqualKey, Alloc>;

 public:
  HashSet() = default;

  // Iterator type of this set, it.first is the key, it.second the value
  using typename Base::iterator;

  // Const Iterator type of this set, it.first is the key, it.second the value
  using typename Base::const_iterator;

  // Returns an iterator to the first element of the set if it exists and an
  // iterator equal to end() otherwise
  using Base::begin;

  // Returns an iterator one past the end so that `it != m.end()` is a viable
  // stopping condition in a for loop
  using Base::end;

  // Find the iterator pointing to the given element
  using Base::find;

  // Erases the given element
  using Base::erase;

  // Counts the number of occurences (0 or 1) of the given element
  using Base::count;

  // Returns the size of the set
  using Base::size;

  // Clears the contents of the set
  using Base::clear;

  // Inserts an element or a range of elements (from a compatible
  // container).
  using Base::insert;
};
}  // namespace ad_utility
