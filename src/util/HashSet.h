// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold <buchholb>

#pragma once

#include <google/dense_hash_set>
#include <string>
#include "./DefaultKeyProvider.h"

using std::string;

namespace ad_utility {
//! Wrapper for HashSets to be used everywhere throughout code for
//! the semantic search. This wrapper interface is not designed to
//! be complete from the beginning. Feel free to extend it at need.
//! The first version as of May 2011 uses google's dense_hash_set.
//! Backing-up implementations may be changed in the future.
template <class T,
          class HashFcn = SPARSEHASH_HASH<T>,  // defined in sparseconfig.h
          class EqualKey = std::equal_to<T>,
          class Alloc = google::libc_allocator_with_realloc<T>>
class HashSet {
 public:
  HashSet() {
    _impl.set_empty_key(DefaultKeyProvider<T>::DEFAULT_EMPTY_KEY);
    _impl.set_deleted_key(DefaultKeyProvider<T>::DEFAULT_DELETED_KEY);
  }

  typedef typename google::dense_hash_set<T, HashFcn, EqualKey,
                                          Alloc>::const_iterator const_iterator;
  typedef typename google::dense_hash_set<T, HashFcn, EqualKey, Alloc>::iterator
      iterator;

  void set_empty_key(const T& emptyKey) { _impl.set_empty_key(emptyKey); }

  void set_deleted_key(const T& emptyKey) { _impl.set_deleted_key(emptyKey); }

  const_iterator begin() const { return _impl.begin(); }

  const_iterator end() const { return _impl.end(); }

  const_iterator find(const T& key) const { return _impl.find(key); }

  iterator begin() { return _impl.begin(); }

  iterator end() { return _impl.end(); }

  iterator find(const T& key) { return _impl.find(key); }

  void insert(const T& value) { _impl.insert(value); }

  template <class InputIterator>
  void insert(InputIterator f, InputIterator l) {
    _impl.insert(f, l);
  }
  void insert(const_iterator f, const_iterator l) { _impl.insert(f, l); }
  // Required for std::insert_iterator; the passed-in iterator is ignored.
  iterator insert(iterator, const T& obj) { return insert(obj).first; }

  size_t erase(const T& value) { return _impl.erase(value); }

  size_t count(const T& value) const { return _impl.count(value); }

  size_t size() const { return _impl.size(); }

  void clear() { _impl.clear(); }

 private:
  google::dense_hash_set<T, HashFcn, EqualKey, Alloc> _impl;
};
}  // namespace ad_utility