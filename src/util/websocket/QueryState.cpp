//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "QueryState.h"

#include <absl/container/flat_hash_map.h>

#include "./WebSocketManager.h"

namespace ad_utility::query_state {


static absl::flat_hash_map<QueryId, RuntimeInformationSnapshot> queryStates{};

void signalUpdateForQuery(QueryId queryId, RuntimeInformation runtimeInformation) {
  RuntimeInformationSnapshot snapshot{std::move(runtimeInformation), std::chrono::steady_clock::now()};
  {
    // TODO lock for query states
    queryStates[queryId] = snapshot;
  }
  websocket::fireAllCallbacksForQuery(queryId, std::move(snapshot));
}

void clearQueryInfo(QueryId queryId) {
  // TODO lock for query states
  queryStates.erase(queryId);
}

std::optional<RuntimeInformationSnapshot> getIfUpdatedSince(QueryId queryId, websocket::common::TimeStamp timeStamp) {
  // TODO lock for query states
  if (queryStates.count(queryId)) {
    auto snapshot = queryStates[queryId];
    if (snapshot.updateMoment > timeStamp) {
      return snapshot;
    }
  }
  return std::nullopt;
}

}  // namespace ad_utility::query_state
