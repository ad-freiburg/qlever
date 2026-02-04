// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTIDCACHE_H
#define QLEVER_SRC_ENGINE_CONSTRUCTIDCACHE_H

#include <memory>
#include <string>

#include "global/Id.h"
#include "util/LruCache.h"

// Cache for ID-to-string conversions to avoid redundant vocabulary lookups
// when the same ID appears multiple times across rows.
// Uses LRU eviction to bound memory usage for queries with many unique IDs.
using ConstructIdCache =
    ad_utility::util::LRUCache<Id, std::shared_ptr<const std::string>>;

// Minimum capacity for the LRU cache. Sized to maximize cross-batch cache
// hits on repeated values (e.g., predicates that appear in many rows).
inline constexpr size_t CONSTRUCT_ID_CACHE_MIN_CAPACITY = 100'000;

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

// RAII logger for IdCache statistics. Logs stats at INFO level when
// destroyed (i.e., after query execution completes). Only logs if there
// were a meaningful number of lookups (> 1000).
class ConstructIdCacheStatsLogger {
 public:
  ConstructIdCacheStatsLogger(size_t numRows, size_t cacheCapacity)
      : numRows_(numRows), cacheCapacity_(cacheCapacity) {}

  ~ConstructIdCacheStatsLogger();

  // Non-copyable: copying would cause duplicate logging on destruction.
  ConstructIdCacheStatsLogger(const ConstructIdCacheStatsLogger&) = delete;
  ConstructIdCacheStatsLogger& operator=(const ConstructIdCacheStatsLogger&) =
      delete;

  // Non-movable: this logger is used in-place and should not be moved.
  ConstructIdCacheStatsLogger(ConstructIdCacheStatsLogger&&) = delete;
  ConstructIdCacheStatsLogger& operator=(ConstructIdCacheStatsLogger&&) =
      delete;

  // Accessors for the stats (used during cache operations)
  ConstructIdCacheStats& stats() { return stats_; }
  const ConstructIdCacheStats& stats() const { return stats_; }

 private:
  ConstructIdCacheStats stats_;
  size_t numRows_;
  size_t cacheCapacity_;
};

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTIDCACHE_H
