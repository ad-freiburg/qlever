// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Authors: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/QueryExecutionContext.h"

#include "global/RuntimeParameters.h"

using namespace std::chrono_literals;

// _____________________________________________________________________________
bool QueryExecutionContext::areWebSocketUpdatesEnabled() {
  return getRuntimeParameter<&RuntimeParameters::websocketUpdatesEnabled_>();
}

// _____________________________________________________________________________
std::chrono::milliseconds QueryExecutionContext::websocketUpdateInterval() {
  return getRuntimeParameter<&RuntimeParameters::websocketUpdateInterval_>();
}

// _____________________________________________________________________________
QueryExecutionContext::QueryExecutionContext(
    const Index& index, QueryResultCache* const cache,
    ad_utility::AllocatorWithLimit<Id> allocator,
    SortPerformanceEstimator sortPerformanceEstimator,
    NamedResultCache* namedResultCache,
    std::function<void(std::string)> updateCallback, const bool pinSubtrees,
    const bool pinResult)
    : _pinSubtrees(pinSubtrees),
      _pinResult(pinResult),
      _index(index),
      _subtreeCache(cache),
      _allocator(std::move(allocator)),
      _sortPerformanceEstimator(sortPerformanceEstimator),
      updateCallback_(std::move(updateCallback)),
      namedResultCache_(namedResultCache) {}

// _____________________________________________________________________________
void QueryExecutionContext::signalQueryUpdate(
    const RuntimeInformation& runtimeInformation,
    RuntimeInformation::SendPriority sendPriority) const {
  auto now = std::chrono::steady_clock::now();
  if (sendPriority == RuntimeInformation::SendPriority::Always ||
      (now - lastWebsocketUpdate_) >= websocketUpdateInterval_) {
    lastWebsocketUpdate_ = now;
    updateCallback_(nlohmann::ordered_json(runtimeInformation).dump());
  }
}
