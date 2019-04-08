// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <google/dense_hash_set>
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
          class HashFcn = SPARSEHASH_HASH<T>,  // defined in sparseconfig.h
          class EqualKey = std::equal_to<T>,
          class Alloc = google::libc_allocator_with_realloc<T>>
class HashSet : private google::dense_hash_set<T, HashFcn, EqualKey, Alloc> {
  using Base = google::dense_hash_set<T, HashFcn, EqualKey, Alloc>;

 public:
  HashSet() {
    Base::set_empty_key(DefaultKeyProvider<T>::DEFAULT_EMPTY_KEY);
    Base::set_deleted_key(DefaultKeyProvider<T>::DEFAULT_DELETED_KEY);
  }

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
