//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <list>

namespace ad_utility::util {

template <typename K, typename V>
class LRUCache {
 private:
  size_t capacity_;
  // Stores keys in order of usage (MRU at front)
  std::list<K> keys_;
  absl::flat_hash_map<K, std::pair<V, typename std::list<K>::iterator>> cache_;

 public:
  explicit LRUCache(size_t capacity) : capacity_{capacity} {}

  template <typename Func>
  const V& getOrCompute(const K& key, Func computeFunction) {
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      // Move accessed key to front (most recently used)
      keys_.erase(it->second.second);
      keys_.push_front(key);
      it->second.second = keys_.begin();

      return it->second.first;
    }
    // Evict LRU if cache is full
    if (cache_.size() >= capacity_) {
      K lruKey = keys_.back();
      keys_.pop_back();
      cache_.erase(lruKey);
    }
    // Insert new element
    keys_.push_front(key);
    auto result = cache_.try_emplace(key, computeFunction(key), keys_.begin());
    AD_CORRECTNESS_CHECK(result.second);
    return result.first->second.first;
  }
};

}  // namespace ad_utility::util
