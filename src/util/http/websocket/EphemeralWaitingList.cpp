//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/EphemeralWaitingList.h"

namespace ad_utility::websocket {

void EphemeralWaitingList::signalQueryUpdate(const QueryId& queryId) {
  auto [start, stop] = waitingCallbacks_.equal_range(queryId);
  for (auto it = start; it != stop; ++it) {
    auto& [func, id] = it->second;
    functionIdToQueryId_.erase(id);
    func();
  }
  waitingCallbacks_.erase(queryId);
}

FunctionId EphemeralWaitingList::callOnQueryUpdate(
    const QueryId& queryId, std::function<void()> callback) {
  FunctionId functionId{idCounter_++};
  waitingCallbacks_.emplace(
      queryId, IdentifiableFunction{std::move(callback), functionId});
  functionIdToQueryId_.emplace(functionId, queryId);
  return functionId;
}

void EphemeralWaitingList::removeCallback(const FunctionId& functionId) {
  if (!functionIdToQueryId_.contains(functionId)) {
    return;
  }
  const QueryId& queryId = functionIdToQueryId_.at(functionId);
  auto [start, stop] = waitingCallbacks_.equal_range(queryId);
  for (auto it = start; it != stop; ++it) {
    if (it->second.id_ == functionId) {
      waitingCallbacks_.erase(it);
      break;
    }
  }
  functionIdToQueryId_.erase(functionId);
}

}  // namespace ad_utility::websocket
