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
  // Only log if there were a meaningful number of lookups
  if (stats_.totalLookups() > 1000) {
    AD_LOG_INFO << "CONSTRUCT IdCache stats - Rows: " << numRows_
                << ", Capacity: " << cacheCapacity_
                << ", Lookups: " << stats_.totalLookups()
                << ", Hits: " << stats_.hits_ << ", Misses: " << stats_.misses_
                << ", Hit rate: " << std::fixed << std::setprecision(1)
                << (stats_.hitRate() * 100.0) << "%" << std::endl;
  }
}
