//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "QueryState.h"

#include <absl/container/flat_hash_map.h>

#include <mutex>

#include "./WebSocketManager.h"

namespace ad_utility::query_state {

static std::mutex queryStatesMutex{};
static absl::flat_hash_map<QueryId, TimedClientPayload> queryStates{};

void signalUpdateForQuery(const QueryId& queryId,
                          const RuntimeInformation& runtimeInformation) {
  std::string payload = nlohmann::ordered_json(runtimeInformation).dump();
  auto snapshot = std::make_shared<TimedClientPayload::element_type>(
      std::move(payload), std::chrono::steady_clock::now());
  // Only insert data if there are additional waiting websockets
  if (websocket::fireAllCallbacksForQuery(queryId, snapshot)) {
    std::lock_guard lock{queryStatesMutex};
    queryStates[queryId] = std::move(snapshot);
  }
}

void clearQueryInfo(const QueryId& queryId) {
  std::lock_guard lock{queryStatesMutex};
  queryStates.erase(queryId);
}

TimedClientPayload getIfUpdatedSince(const QueryId& queryId,
                                     websocket::common::TimeStamp timeStamp) {
  std::lock_guard lock{queryStatesMutex};
  if (queryStates.count(queryId)) {
    auto snapshot = queryStates[queryId];
    if (snapshot->updateMoment > timeStamp) {
      return snapshot;
    }
  }
  return nullptr;
}

}  // namespace ad_utility::query_state
