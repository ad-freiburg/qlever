//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "QueryState.h"

namespace ad_utility::query_state {

void QueryStateManager::signalUpdateForQuery(
    const QueryId& queryId, const RuntimeInformation& runtimeInformation,
    ad_utility::websocket::WebSocketManager& webSocketManager) {
  std::string payload = nlohmann::ordered_json(runtimeInformation).dump();
  auto snapshot = std::make_shared<SharedPayloadAndTimestamp::element_type>(
      std::move(payload), std::chrono::steady_clock::now());
  // Only insert data if there are additional waiting websockets
  if (webSocketManager.fireAllCallbacksForQuery(queryId, snapshot)) {
    std::lock_guard lock{queryStatesMutex};
    queryStates[queryId] = std::move(snapshot);
  }
}

void QueryStateManager::clearQueryInfo(const QueryId& queryId) {
  std::lock_guard lock{queryStatesMutex};
  queryStates.erase(queryId);
}

SharedPayloadAndTimestamp QueryStateManager::getIfUpdatedSince(
    const QueryId& queryId, websocket::common::Timestamp timeStamp) {
  std::lock_guard lock{queryStatesMutex};
  if (queryStates.contains(queryId)) {
    auto snapshot = queryStates[queryId];
    if (snapshot->updateMoment > timeStamp) {
      return snapshot;
    }
  }
  return nullptr;
}

}  // namespace ad_utility::query_state
