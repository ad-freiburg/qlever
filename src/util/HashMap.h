// Copyright 2018, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb@informatik.uni-freiburg.de>
// Author: Niklas Schnelle <schnelle@informatik.uni-freiburg.de>

#pragma once

#include <google/dense_hash_map>

#include "./DefaultKeyProvider.h"

namespace ad_utility {
// Wrapper for HashMaps to be used everywhere throughout code for the semantic
// search. This wrapper interface is not designed to be complete from the
// beginning. Feel free to extend it at need.
//
// The current version uses google's dense_hash_map and directly exports its
// functions. It only adds a constructor which automatically sets the empty and
// deleted keys.  This may be changed in the future.
template <class K, class V,
          class HashFcn = SPARSEHASH_HASH<K>,  // defined in sparseconfig.h
          class EqualKey = std::equal_to<K>,
          class Alloc =
              google::libc_allocator_with_realloc<std::pair<const K, V> > >
class HashMap : private google::dense_hash_map<K, V, HashFcn, EqualKey, Alloc> {
  using Base = typename google::dense_hash_map<K, V, HashFcn, EqualKey, Alloc>;

 public:
  HashMap<K, V, HashFcn, EqualKey, Alloc>(
      K emptyKey = DefaultKeyProvider<K>::DEFAULT_EMPTY_KEY,
      K deletedKey = DefaultKeyProvider<K>::DEFAULT_DELETED_KEY) {
    Base::set_empty_key(emptyKey);
    Base::set_deleted_key(deletedKey);
  }

  // Iterator type of this map, it.first is the key, it.second the value
  using Base::iterator;

  // Const Iterator type of this map, it.first is the key, it.second the value
  using Base::const_iterator;

  // Returns an iterator to the first element of the map if it exists and an
  // iterator equal to end() otherwise
  using Base::begin;

  // Returns an iterator one past the end so that `it != m.end()` is a viable
  // stopping condition in a for loop
  using Base::end;

  // Finds an element for a given key. Returns an iterator to the element or
  // end() if it is not found
  using Base::find;

  // Erases an element for a given key
  using Base::erase;

  // Access an element for the given key, creating by default construction if it
  // does not exist
  using Base::operator[];

  // Counts the number of occurences (0 or 1) of the given key
  using Base::count;

  // Returns the size of the map
  using Base::size;

  // Clears the contents of the map
  using Base::clear;

  // Inserts a std::pair<K, V> pair or a range of pairs from a compatible
  // container. Note: Such a pair can also be constructed using brace
  // initializer lists.
  //       Therefore syntax like m.insert({"answer", 42}); can be used in
  //       a HashMap<string, int>
  using Base::insert;
};
}  // namespace ad_utility
