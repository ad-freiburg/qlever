//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/QueryExecutionContext.h"

// _____________________________________________________________________________
QueryExecutionContext::QueryExecutionContext(
    const Index& index, QueryResultCache* const cache,
    ad_utility::AllocatorWithLimit<Id> allocator,
    SortPerformanceEstimator sortPerformanceEstimator,
    NamedQueryCache* namedCache,
    std::function<void(std::string)> updateCallback, const bool pinSubtrees,
    const bool pinResult)
    : _pinSubtrees(pinSubtrees),
      _pinResult(pinResult),
      _index(index),
      _subtreeCache(cache),
      _allocator(std::move(allocator)),
      _sortPerformanceEstimator(sortPerformanceEstimator),
      updateCallback_(std::move(updateCallback)),
      namedQueryCache_{namedCache} {}
