//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <list>

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
  // Stores keys in order of usage (MRU at front)
  std::list<K> keys_;
  absl::flat_hash_map<K, std::pair<V, typename std::list<K>::iterator>> cache_;

 public:
  explicit LRUCache(size_t capacity) : capacity_{capacity} {
    AD_CONTRACT_CHECK(capacity > 0, "Capacity must be greater than 0");
  }

  // Check if `key` is in the cache and return a reference to the value if it is
  // found. Otherwise, compute the value using `computeFunction` and store it in
  // the cache. If the cache is already at maximum capacity, evict the least
  // recently used element.
  CPP_template(typename Func)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          Func, V, const K&>) const V& getOrCompute(const K& key,
                                                    Func computeFunction) {
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      const auto& [value, listIterator] = it->second;
      // Move accessed key to front (most recently used)
      keys_.splice(keys_.begin(), keys_, listIterator);

      return value;
    }
    // Evict LRU if cache is full
    if (cache_.size() >= capacity_) {
      K& lruKey = keys_.back();
      cache_.erase(lruKey);
      // Reuse allocated memory by moving node and reassigning key
      keys_.splice(keys_.begin(), keys_, std::prev(keys_.end()));
      lruKey = key;
    } else {
      // Push new element if not full
      keys_.push_front(key);
    }
    auto result = cache_.try_emplace(key, computeFunction(key), keys_.begin());
    AD_CORRECTNESS_CHECK(result.second);
    return result.first->second.first;
  }
};

}  // namespace ad_utility::util
