// Copyright 2011 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchholb@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <absl/container/node_hash_set.h>

#include <cstddef>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "util/AllocatorWithLimit.h"
#include "util/DefaultValueSizeGetter.h"
#include "util/MemorySize/MemorySize.h"

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

// Wrapper around absl::node_hash_set with a memory limit. All operations that
// may change the allocated memory of the hash set is tracked using a
// `AllocationMemoryLeftThreadsafe` object.
template <class T, class SizeGetter = SizeOfSizeGetter,
          class HashFct = absl::container_internal::hash_default_hash<T>,
          class EqualElem = absl::container_internal::hash_default_eq<T>>
class CustomHashSetWithMemoryLimit {
 private:
  using HashSet = absl::node_hash_set<T, HashFct, EqualElem>;
  HashSet hashSet_;
  detail::AllocationMemoryLeftThreadsafe memoryLeft_;
  MemorySize memoryUsed_;
  SizeGetter sizeGetter_;

 public:
  CustomHashSetWithMemoryLimit(
      detail::AllocationMemoryLeftThreadsafe memoryLeft,
      SizeGetter sizeGetter = {})
      : memoryLeft_{memoryLeft},
        memoryUsed_{MemorySize::bytes(0)},
        sizeGetter_{sizeGetter} {}

  ~CustomHashSetWithMemoryLimit() {
    memoryLeft_.ptr()->wlock()->increase(memoryUsed_);
  }

  // Insert an element into the hash set. If the memory limit is exceeded, the
  // insert operation fails with a runtime error.
  std::pair<typename HashSet::iterator, bool> insert(const T& value) {
    MemorySize size = sizeGetter_(value);
    if (!memoryLeft_.ptr()->wlock()->decrease_if_enough_left_or_return_false(
            size)) {
      throw std::runtime_error(
          "The element to be inserted is too large for the hash set.");
    }

    const auto& [it, wasInserted] = hashSet_.insert(value);

    if (wasInserted) {
      memoryUsed_ += size;
    } else {
      memoryLeft_.ptr()->wlock()->increase(size);
    }
    return std::pair{it, wasInserted};
  }

  // _____________________________________________________________________________
  void erase(const T& value) {
    auto it = hashSet_.find(value);
    if (it != hashSet_.end()) {
      MemorySize size = sizeGetter_(*it);
      hashSet_.erase(it);
      memoryLeft_.ptr()->wlock()->increase(size);
      memoryUsed_ -= size;
    }
  }

  // _____________________________________________________________________________
  void clear() {
    hashSet_.clear();
    memoryLeft_.ptr()->wlock()->increase(memoryUsed_);
    memoryUsed_ = MemorySize::bytes(0);
  }

  // _____________________________________________________________________________
  size_t size() const { return hashSet_.size(); }

  // _____________________________________________________________________________
  bool empty() const { return hashSet_.empty(); }

  // _____________________________________________________________________________
  size_t count(const T& value) const { return hashSet_.count(value); }

  // _____________________________________________________________________________
  HashSet::const_iterator find(const T& value) const {
    return hashSet_.find(value);
  }

  // _____________________________________________________________________________
  bool contains(const T& key) { return hashSet_.contains(key); }

  // _____________________________________________________________________________
  HashSet::const_iterator begin() const { return hashSet_.begin(); }

  // _____________________________________________________________________________
  HashSet::const_iterator end() const { return hashSet_.end(); }

  // _____________________________________________________________________________
  MemorySize getCurrentMemoryUsage() const { return memoryUsed_; }
};

}  // namespace ad_utility
