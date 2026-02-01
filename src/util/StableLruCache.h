//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_STABLELRUCACHE_H
#define QLEVER_SRC_UTIL_STABLELRUCACHE_H

#include <list>

#include "util/Exception.h"
#include "util/HashMap.h"

namespace ad_utility {

// ============================================================================
// StableLRUCache
// ============================================================================
//
// An LRU (Least Recently Used) cache with pointer/reference stability.
//
// POINTER STABILITY GUARANTEE
// ---------------------------
// References returned by getOrCompute() remain valid until the referenced
// entry is evicted by LRU replacement. This is achieved by pre-reserving the
// underlying hash map to prevent rehashing.
//
// This differs from ad_utility::util::LRUCache which uses absl::flat_hash_map
// without pre-reservation, meaning insertions can trigger rehashing and
// invalidate all existing references.
//
// USE CASE
// --------
// Use this cache when you need to store pointers/references to cached values
// for later use (within the same batch of operations). For example,
// ConstructTripleGenerator stores pointers to cached strings in a batch
// buffer to avoid repeated hash lookups during triple instantiation.
//
// COMPLEXITY
// ----------
// - getOrCompute: O(1) average for lookup/insert, O(1) for LRU bookkeeping
// - Space: O(capacity) for hash map + O(capacity) for LRU list
//
// ============================================================================
template <typename K, typename V>
class StableLRUCache {
 public:
  explicit StableLRUCache(size_t capacity) : capacity_{capacity} {
    AD_CONTRACT_CHECK(capacity > 0);
    // Pre-allocate hash map to prevent rehashing, ensuring pointer stability.
    // After this, the hash map won't reallocate until size exceeds capacity,
    // which never happens because we evict before inserting when at capacity.
    cache_.reserve(capacity);
  }

  // Look up key in cache. On hit, mark as recently used and return reference.
  // On miss, compute value, insert (evicting LRU if at capacity), return ref.
  //
  // The returned reference is stable until this entry is evicted by LRU.
  // Within a batch where you access at most `capacity` unique keys, all
  // returned references remain valid.
  template <typename Func>
  const V& getOrCompute(const K& key, Func computeFunc) {
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      // Cache hit: move key to front of LRU list (most recently used)
      keys_.splice(keys_.begin(), keys_, it->second.second);
      return it->second.first;
    }

    // Cache miss: evict LRU entry if at capacity
    if (cache_.size() >= capacity_) {
      cache_.erase(keys_.back());
      keys_.pop_back();
    }

    // Insert new entry at front of LRU list
    keys_.push_front(key);
    auto [newIt, inserted] =
        cache_.emplace(key, std::make_pair(computeFunc(key), keys_.begin()));
    AD_CORRECTNESS_CHECK(inserted);
    return newIt->second.first;
  }

  // Returns current number of cached entries.
  size_t size() const { return cache_.size(); }

  // Returns maximum capacity.
  size_t capacity() const { return capacity_; }

 private:
  size_t capacity_;

  // LRU ordering: front = most recently used, back = least recently used.
  // Using std::list for O(1) splice operations and iterator stability.
  std::list<K> keys_;

  // Maps key -> (value, iterator into keys_ list).
  // The iterator allows O(1) LRU list updates on cache hit.
  HashMap<K, std::pair<V, typename std::list<K>::iterator>> cache_;
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_STABLELRUCACHE_H
