//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/EphemeralWaitingList.h"

#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/dispatch.hpp>

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
    const QueryId& queryId, CompletionToken&& token) {
  auto initiate = [this, &queryId]<typename Handler>(Handler&& self) mutable {
    auto sharedSelf = std::make_shared<Handler>(std::forward<Handler>(self));
    auto cancellationSlot = net::get_associated_cancellation_slot(
        *sharedSelf, net::cancellation_slot());
    FunctionId functionId{idCounter_++};
    auto executor = net::get_associated_executor(*sharedSelf);
    waitingCallbacks_.emplace(
        queryId, IdentifiableFunction{[sharedSelf = std::move(sharedSelf)]() {
                                        (*sharedSelf)();
                                      },
                                      functionId});

    if (cancellationSlot.is_connected()) {
      cancellationSlot.assign(
          [this, &queryId, &functionId,
           executor = std::move(executor)](net::cancellation_type) {
            net::dispatch(executor, [this, &queryId, &functionId]() {
              removeCallback(queryId, functionId);
            });
          });
    }
  };
  return net::async_initiate<CompletionToken, void()>(
      initiate, std::forward<CompletionToken>(token));
}

net::awaitable<void> EphemeralWaitingList::waitForQueryStart(
    const QueryId& queryId) {
  co_await registerCallbackAndWait(queryId, net::use_awaitable);
}

void EphemeralWaitingList::removeCallback(
    const QueryId& queryId, const FunctionId& functionId) noexcept {
  auto [start, stop] = waitingCallbacks_.equal_range(queryId);
  for (auto it = start; it != stop; ++it) {
    if (it->second.id_ == functionId) {
      waitingCallbacks_.erase(it);
      break;
    }
  }
}

}  // namespace ad_utility::websocket
