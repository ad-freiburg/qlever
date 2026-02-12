// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTIDCACHE_H
#define QLEVER_SRC_ENGINE_CONSTRUCTIDCACHE_H

#include <memory>
#include <string>

#include "engine/ConstructTypes.h"
#include "global/Id.h"
#include "util/LruCache.h"

// Statistics for ID cache performance analysis
struct ConstructIdCacheStats {
  size_t hits_ = 0;
  size_t misses_ = 0;
  size_t totalLookups() const { return hits_ + misses_; }
  double hitRate() const {
    return totalLookups() > 0 ? static_cast<double>(hits_) /
                                    static_cast<double>(totalLookups())
                              : 0.0;
  }
};

// Cache for ID-to-EvaluatedTerm conversions to avoid redundant
// vocabulary lookups when the same ID appears multiple times across rows.
// Uses LRU eviction to bound memory usage for queries with many unique IDs.
// Statistics (hits/misses) are tracked internally.
class ConstructIdCache {
 public:
  explicit ConstructIdCache(size_t capacity) : cache_(capacity) {}

  // Look up the value for a key, computing it if not present.
  // Statistics are tracked automatically.
  template <typename ComputeFunc>
  const EvaluatedTerm& getOrCompute(const Id& key, ComputeFunc&& compute) {
    bool wasHit = true;
    const auto& result =
        cache_.getOrCompute(key, [&wasHit, &compute](const Id& k) {
          wasHit = false;
          return compute(k);
        });
    if (wasHit) {
      ++stats_.hits_;
    } else {
      ++stats_.misses_;
    }
    return result;
  }

  const ConstructIdCacheStats& stats() const { return stats_; }
  size_t capacity() const { return cache_.capacity(); }

 private:
  ad_utility::util::LRUCache<Id, EvaluatedTerm> cache_;
  ConstructIdCacheStats stats_;
};

// RAII logger for `IdCache` statistics. Logs stats at INFO level when
// destroyed (i.e., after query execution completes). Only logs if there
// were a meaningful number of lookups (> 1000).
class ConstructIdCacheStatsLogger {
 public:
  ConstructIdCacheStatsLogger(size_t numRows, const ConstructIdCache& cache)
      : numRows_(numRows), cache_(cache) {}

  ~ConstructIdCacheStatsLogger();

  // Non-copyable, non-movable
  ConstructIdCacheStatsLogger(const ConstructIdCacheStatsLogger&) = delete;
  ConstructIdCacheStatsLogger& operator=(const ConstructIdCacheStatsLogger&) =
      delete;
  ConstructIdCacheStatsLogger(ConstructIdCacheStatsLogger&&) = delete;
  ConstructIdCacheStatsLogger& operator=(ConstructIdCacheStatsLogger&&) =
      delete;

 private:
  size_t numRows_;
  const ConstructIdCache& cache_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTIDCACHE_H
