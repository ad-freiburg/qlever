// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <google/dense_hash_map>

#include "./DefaultKeyProvider.h"

namespace ad_utility {
//! Wrapper for HashMaps to be used everywhere throughout code for
//! the semantic search. This wrapper interface is not designed to
//! be complete from the beginning. Feel free to extend it at need.
//! The first version as of May 2011 uses google's dense_hash_map.
//! Backing-up implementations may be changed in the future.
template <class K, class V,
          class HashFcn = SPARSEHASH_HASH<K>,  // defined in sparseconfig.h
          class EqualKey = std::equal_to<K>,
          class Alloc =
              google::libc_allocator_with_realloc<std::pair<const K, V> > >
class HashMap {
 public:
  HashMap<K, V, HashFcn, EqualKey, Alloc>(
      K emptyKey = DefaultKeyProvider<K>::DEFAULT_EMPTY_KEY,
      K deletedKey = DefaultKeyProvider<K>::DEFAULT_DELETED_KEY)
      : _impl() {
    _impl.set_empty_key(emptyKey);
    _impl.set_deleted_key(deletedKey);
  }

  typedef
      typename google::dense_hash_map<K, V, HashFcn, EqualKey, Alloc>::iterator
          iterator;
  typedef typename google::dense_hash_map<K, V, HashFcn, EqualKey,
                                          Alloc>::const_iterator const_iterator;

  const_iterator begin() const { return _impl.begin(); }

  const_iterator end() const { return _impl.end(); }

  iterator begin() { return _impl.begin(); }

  iterator end() { return _impl.end(); }

  iterator find(const K& key) { return _impl.find(key); }

  const_iterator find(const K& key) const { return _impl.find(key); }

  size_t erase(const K& key) { return _impl.erase(key); }

  //! Lookup operator that also adds pairs whenever
  //! they don't exist, using @param key as key
  //! and a value /w default constructor.
  //! If this is not the behavior you want, use "find" instead.
  V&

  operator[](const K& key) {
    return _impl[key];
  }

  size_t count(const K& key) const { return _impl.count(key); }

  size_t size() const { return _impl.size(); }

  void clear() { _impl.clear(); }

  // Insertion routines
  std::pair<iterator, bool> insert(const V& obj) { return _impl.insert(obj); }
  template <class InputIterator>
  void insert(InputIterator f, InputIterator l) {
    _impl.insert(f, l);
  }
  void insert(const_iterator f, const_iterator l) { _impl.insert(f, l); }
  // Required for std::insert_iterator; the passed-in iterator is ignored.
  iterator insert(iterator, const V& obj) { return insert(obj).first; }

 private:
  google::dense_hash_map<K, V, HashFcn, EqualKey, Alloc> _impl;
};
}  // namespace ad_utility
