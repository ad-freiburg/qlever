//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/EphemeralWaitingList.h"

namespace ad_utility::websocket {

void EphemeralWaitingList::signalQueryStart(const QueryId& queryId) {
  auto [start, stop] = waitingCallbacks_.equal_range(queryId);
  for (auto it = start; it != stop; ++it) {
    auto& [func, id] = it->second;
    func();
  }
  waitingCallbacks_.erase(queryId);
}

template <typename CompletionToken>
net::awaitable<void> EphemeralWaitingList::registerCallbackAndWait(
    const QueryId& queryId, const FunctionId& functionId,
    CompletionToken&& token) {
  auto initiate = [this, &queryId,
                   &functionId]<typename Handler>(Handler&& self) mutable {
    auto callback = [selfPtr = std::make_shared<Handler>(
                         std::forward<Handler>(self))]() { (*selfPtr)(); };
    waitingCallbacks_.emplace(
        queryId, IdentifiableFunction{std::move(callback), functionId});
  };
  return net::async_initiate<CompletionToken, void()>(
      initiate, std::forward<CompletionToken>(token));
}

net::awaitable<void> EphemeralWaitingList::waitForQueryStart(
    const QueryId& queryId) {
  FunctionId functionId{idCounter_++};
  absl::Cleanup cleanup{
      [this, &queryId, &functionId]() { removeCallback(queryId, functionId); }};

  co_await registerCallbackAndWait(queryId, functionId, net::use_awaitable);
  std::move(cleanup).Cancel();
}

void EphemeralWaitingList::removeCallback(const QueryId& queryId,
                                          const FunctionId& functionId) {
  auto [start, stop] = waitingCallbacks_.equal_range(queryId);
  for (auto it = start; it != stop; ++it) {
    if (it->second.id_ == functionId) {
      waitingCallbacks_.erase(it);
      break;
    }
  }
}

}  // namespace ad_utility::websocket
