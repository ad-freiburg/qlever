//  Copyright 2023, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#pragma once

#include <absl/container/flat_hash_map.h>

#include <mutex>
#include <optional>

#include "util/websocket/Common.h"
#include "util/websocket/WebSocketManager.h"

namespace ad_utility::websocket {
class WebSocketManager;
}

namespace ad_utility::query_state {

using websocket::common::QueryId;
using websocket::common::SharedPayloadAndTimestamp;

class QueryStateManager {
  std::mutex queryStatesMutex{};
  absl::flat_hash_map<QueryId, SharedPayloadAndTimestamp> queryStates{};

 public:
  QueryStateManager() = default;
  void signalUpdateForQuery(
      const QueryId& queryId, const RuntimeInformation& runtimeInformation,
      ad_utility::websocket::WebSocketManager& webSocketManager);
  void clearQueryInfo(const QueryId& queryId);
  SharedPayloadAndTimestamp getIfUpdatedSince(
      const QueryId& queryId, websocket::common::Timestamp timeStamp);
};

}  // namespace ad_utility::query_state
