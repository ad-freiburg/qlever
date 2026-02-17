//   Copyright 2025, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_LRUCACHEWITHSTATISTICS_H
#define QLEVER_SRC_UTIL_LRUCACHEWITHSTATISTICS_H

#include <cstddef>
#include <cstdint>

#include "util/LruCache.h"

namespace ad_utility::util {

// Statistics about cache hits and misses.
struct LRUCacheStats {
  uint64_t hits_ = 0;
  uint64_t misses_ = 0;

  uint64_t totalLookups() const { return hits_ + misses_; }

  double hitRate() const {
    auto total = totalLookups();
    return total == 0 ? 0.0 : static_cast<double>(hits_) / total;
  }
};

// A wrapper around `LRUCache` that tracks hit/miss statistics.
template <typename K, typename V>
class LRUCacheWithStatistics {
 private:
  mutable LRUCache<K, V> cache_;
  mutable LRUCacheStats stats_;

 public:
  explicit LRUCacheWithStatistics(size_t capacity) : cache_{capacity} {}

  // Look up `key` in the cache. If found, return the cached value (cache hit).
  // Otherwise, compute the value using `computeFunction`, store it, and return
  // it (cache miss).
  CPP_template(typename Func)(
      requires ad_utility::InvocableWithConvertibleReturnType<
          Func, V, const K&>) const V& getOrCompute(const K& key,
                                                    Func computeFunction) {
    bool wasMiss = false;
    const V& result =
        cache_.getOrCompute(key, [&wasMiss, &computeFunction](const K& k) {
          wasMiss = true;
          return computeFunction(k);
        });
    if (wasMiss) {
      ++stats_.misses_;
    } else {
      ++stats_.hits_;
    }
    return result;
  }

  const LRUCacheStats& stats() const { return stats_; }

  size_t capacity() const { return cache_.capacity(); }
};

}  // namespace ad_utility::util

#endif  // QLEVER_SRC_UTIL_LRUCACHEWITHSTATISTICS_H
