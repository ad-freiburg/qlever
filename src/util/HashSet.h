// Copyright 2011 - 2022, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <b.buchholb@gmail.com>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <absl/container/node_hash_set.h>

#include <cstddef>
#include <cstdlib>
#include <string>
#include <utility>

#include "absl/container/flat_hash_set.h"
#include "util/AllocatorWithLimit.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ValueSizeGetters.h"

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
template <class T, class SizeGetter = DefaultValueSizeGetter<T>,
          class HashFct = absl::container_internal::hash_default_hash<T>,
          class EqualElem = absl::container_internal::hash_default_eq<T>>
class NodeHashSetWithMemoryLimit {
 private:
  using HashSet = absl::node_hash_set<T, HashFct, EqualElem>;
  HashSet hashSet_;
  detail::AllocationMemoryLeftThreadsafe memoryLeft_;
  MemorySize memoryUsed_{MemorySize::bytes(0)};
  SizeGetter sizeGetter_{};
  size_t currentNumSlots_{0};

  // `slotMemoryCost` represents the per-slot memory cost of a node hash set.
  // It accounts for the memory used by a slot in the hash table, which
  // typically consists of a pointer (used for node storage) plus any additional
  // control bytes required for maintaining the hash set's structure and state.
  // This value helps estimate and manage memory consumption for operations that
  // involve slots, such as insertion and rehashing.
  //
  // The value is defined as `sizeof(void*) + 1` bytes, where:
  // - `sizeof(void*)` represents the size of a pointer on the platform (usually
  // 4 bytes for 32-bit and 8 bytes for 64-bit systems).
  // - `+ 1` accounts for an extra control byte used for state management in the
  // hash set.
  constexpr static MemorySize slotMemoryCost =
      MemorySize::bytes(sizeof(void*) + 1);

 public:
  NodeHashSetWithMemoryLimit(detail::AllocationMemoryLeftThreadsafe memoryLeft)
      : memoryLeft_{memoryLeft} {
    // Once the hash set is initialized, calculate the initial memory
    // used by the slots of the hash set
    updateSlotArrayMemoryUsage();
  }

  ~NodeHashSetWithMemoryLimit() { decreaseMemoryUsed(memoryUsed_); }

  // Try to allocate the amount of memory requested
  void increaseMemoryUsed(ad_utility::MemorySize amount) {
    memoryLeft_.ptr()->wlock()->decrease_if_enough_left_or_throw(amount);
    memoryUsed_ += amount;
  }

  // Decrease the amount of memory used
  void decreaseMemoryUsed(ad_utility::MemorySize amount) {
    memoryLeft_.ptr()->wlock()->increase(amount);
    memoryUsed_ -= amount;
  }

  // Update the memory usage for the slot array if the bucket count changes.
  // This function should be called after any operation that could cause
  // rehashing. When the slot count increases, it reserves additional memory,
  // and if the slot count decreases, it releases the unused memory back to the
  // memory tracker.
  void updateSlotArrayMemoryUsage() {
    size_t newNumSlots = hashSet_.bucket_count();
    if (newNumSlots != currentNumSlots_) {
      if (newNumSlots > currentNumSlots_) {
        ad_utility::MemorySize sizeIncrease =
            slotMemoryCost * (newNumSlots - currentNumSlots_);
        increaseMemoryUsed(sizeIncrease);
      } else {
        ad_utility::MemorySize sizeDecrease =
            slotMemoryCost * (currentNumSlots_ - newNumSlots);

        decreaseMemoryUsed(sizeDecrease);
      }
    }
    currentNumSlots_ = newNumSlots;
  }

  // Insert an element into the hash set. If the memory limit is exceeded, the
  // insert operation fails with a runtime error.
  std::pair<typename HashSet::iterator, bool> insert(const T& value) {
    MemorySize size =
        sizeGetter_(value) + ad_utility::MemorySize::bytes(sizeof(T));
    increaseMemoryUsed(size);

    const auto& [it, wasInserted] = hashSet_.insert(value);

    if (!wasInserted) {
      decreaseMemoryUsed(size);
    }

    updateSlotArrayMemoryUsage();
    return std::pair{it, wasInserted};
  }

  // _____________________________________________________________________________
  void erase(const T& value) {
    auto it = hashSet_.find(value);
    if (it != hashSet_.end()) {
      MemorySize size =
          sizeGetter_(*it) + ad_utility::MemorySize::bytes(sizeof(T));
      hashSet_.erase(it);
      decreaseMemoryUsed(size);
      updateSlotArrayMemoryUsage();
    }
  }

  // _____________________________________________________________________________
  void clear() {
    hashSet_.clear();
    // Release all node memory
    decreaseMemoryUsed(memoryUsed_);

    // Update slot memory usage based on the new bucket count after clearing
    size_t newNumSlots = hashSet_.bucket_count();
    ad_utility::MemorySize slotMemoryAfterClear = slotMemoryCost * newNumSlots;
    // After clearing it only tracks the slot memory as nodes are gone
    increaseMemoryUsed(slotMemoryAfterClear);

    currentNumSlots_ = newNumSlots;
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
