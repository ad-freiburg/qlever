// Copyright 2025 The QLever Authors, in particular
// 2025 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_LRUCACHE_H
#define QLEVER_SRC_UTIL_LRUCACHE_H

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest_prod.h>

#include <boost/optional.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "util/Exception.h"

namespace ad_utility::util {

// Simple LRU cache implementation that stores key-value pairs up to a
// configurable threshold and discards the least recently used element when the
// threshold is exceeded.
template <typename K, typename V>
class LRUCache {
 private:
  size_t capacity_;
  // Stores keys in order of usage (MRU at front).
  std::list<K> keys_;
  // Maps each key to its mapped value, which is itself a pair: `.first` is the
  // cached value `V`, and `.second` is an iterator (a "bookmark") into `keys_`
  // marking this key's position in the recency list. Storing the iterator lets
  // a cache hit move the key to the front (MRU) in O(1) without searching
  // `keys_`.
  absl::flat_hash_map<K, std::pair<V, typename std::list<K>::iterator>> cache_;

 public:
  explicit LRUCache(size_t capacity) : capacity_{capacity} {
    AD_CONTRACT_CHECK(capacity > 0, "Capacity must be greater than 0");
  }

  size_t capacity() const { return capacity_; }

  // Check if `key` is in the cache. If found, move it to the front (most
  // recently used) and return a reference to the cached value wrapped in
  // `boost::optional`. If not found, return `boost::none`. Does not insert or
  // compute anything.
  template <typename Key>
  boost::optional<const V&> tryGet(const Key& key) {
    auto cacheHitIterator = cache_.find(key);
    if (cacheHitIterator == cache_.end()) return boost::none;
    const auto& [value, listIterator] = cacheHitIterator->second;
    markMRU(listIterator);
    return boost::optional<const V&>(value);
  }

  // Check if `key` is in the cache and return a reference to the value if it is
  // found. Otherwise, compute the value by calling `computeFunction(key)`
  // (`computeFunction` is invoked with the key as a `const K&` and must return
  // something convertible to `V`), store it in the cache, and return a
  // reference to it.
  // If the cache is already at maximum capacity, evict the least recently used
  // element first.
  CPP_template(typename Key, typename Func)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          Func, V, const K&>) const V& getOrCompute(Key&& key,
                                                    Func computeFunction) {
    auto optValue = tryGet(key);
    if (optValue) {
      return optValue.value();
    }
    return insertNewEntry(std::forward<Key>(key), [&computeFunction, this] {
      return computeFunction(keys_.front());
    });
  }

  // Insert the key-value pair into the cache.
  //
  // Returns `true` if `key` was not previously present and inserts it. If `key`
  // is already present, update the stored value, mark the entry as most
  // recently used and return `false`. If the cache is full, evict the least
  // recently used entry first.
  template <typename Key, typename Value>
  bool insert(Key&& key, Value&& newValue) {
    // `find` returns an iterator to the matching `{key, mapped value}` entry,
    // or `end()` if the key is absent.
    auto cacheHitIterator = cache_.find(key);

    if (cacheHitIterator != cache_.end()) {
      // Cache hit: mark most-recently-used, then overwrite the stored value.
      auto& [oldValue, listIteratorToKey] = cacheHitIterator->second;
      markMRU(listIteratorToKey);
      oldValue = std::forward<Value>(newValue);  // perfect forwarding.
      return false;
    }

    // Cache miss: make room at the front and insert the new entry there.
    insertNewEntry(std::forward<Key>(key),
                   [&newValue] { return std::forward<Value>(newValue); });
    return true;
  }

  // Set-like insertion for empty value types. This stores a default-constructed
  // empty value and returns true iff the key was not already present.
  template <typename Key>
  bool insert(Key&& key)
      requires std::is_empty_v<V> && std::is_default_constructible_v<V> {
    return insert(std::forward<Key>(key), V{});
  }

 private:
  // Move the list node `node` to the front of `keys_`, marking its key as the
  // most recently used. O(1); does not invalidate any iterators.
  void markMRU(typename std::list<K>::iterator node) {
    keys_.splice(keys_.begin(), keys_, node);
  }

  // If the cache is already at capacity, evict the least-recently-used key
  // from both `keys_` and `cache_`.Caller must still insert the matching
  // `cache_` entry, keyed on `keys_.front()`.
  // Precondition: `key` is not already present in the cache.
  template <typename Key>
  void evictLRUIfFullAndMarkMRU(Key&& key) {
    if (cache_.size() >= capacity_) {
      cache_.erase(keys_.back());
      // Recycle the evicted (back) node by moving it to the front and
      // reassigning its key.
      auto leastRecentlyUsedKey = std::prev(keys_.end());
      markMRU(leastRecentlyUsedKey);
      // replace least recently used key with new key.
      keys_.front() = std::forward<Key>(key);  // perfect forwarding
    } else {
      // if the capacity is not full, just insert new key as most recently used
      // key (at the front of the list).
      keys_.push_front(K{std::forward<Key>(key)});  // perfect forwarding
    }
  }

  // Insert a brand-new entry for `key` as the most-recently-used element,
  // evicting the least-recently-used entry first if the cache is full. The
  // value is obtained by invoking `makeValue()` (after `key` has been seated at
  // the front, so it may read `keys_.front()`). Returns a reference to the
  // stored value. Precondition: `key` is not already present in the cache.
  CPP_template(typename Key, typename MakeValue)(
      requires ad_utility::InvocableWithConvertibleReturnType<MakeValue, V>)
      V& insertNewEntry(Key&& key, MakeValue&& makeValue) {
    evictLRUIfFullAndMarkMRU(std::forward<Key>(key));
    auto result = cache_.try_emplace(keys_.front(), makeValue(), keys_.begin());
    AD_CORRECTNESS_CHECK(result.second);
    return result.first->second.first;
  }

  FRIEND_TEST(LRUCacheWhiteBox, markMRUReordersKeysList);
  FRIEND_TEST(LRUCacheWhiteBox, evictLRUIfFullPushesFrontWhenNotFull);
  FRIEND_TEST(LRUCacheWhiteBox, evictLRUIfFullEvictsAndRecyclesWhenFull);
  FRIEND_TEST(LRUCacheWhiteBox, insertNewEntrySeatsKeyBeforeComputingValue);
};

}  // namespace ad_utility::util

#endif  // QLEVER_SRC_UTIL_LRUCACHE_H
