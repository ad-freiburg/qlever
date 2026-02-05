// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/ConstructIdCache.h"

#include <iomanip>

#include "util/Log.h"

// _____________________________________________________________________________
ConstructIdCacheStatsLogger::~ConstructIdCacheStatsLogger() {
  const auto& stats = cache_.stats();
  // Only log if there were a meaningful number of lookups
  if (stats.totalLookups() > 1000) {
    AD_LOG_INFO << "CONSTRUCT IdCache stats - Rows: " << numRows_
                << ", Capacity: " << cache_.capacity()
                << ", Lookups: " << stats.totalLookups()
                << ", Hits: " << stats.hits_ << ", Misses: " << stats.misses_
                << ", Hit rate: " << std::fixed << std::setprecision(1)
                << (stats.hitRate() * 100.0) << "%" << std::endl;
  }
}
