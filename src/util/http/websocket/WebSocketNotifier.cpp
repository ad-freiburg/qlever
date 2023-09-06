//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/WebSocketNotifier.h"

#include <boost/asio/co_spawn.hpp>

namespace ad_utility::websocket {

net::awaitable<WebSocketNotifier> WebSocketNotifier::create(
    common::QueryId queryId, WebSocketTracker& webSocketTracker) {
  co_return WebSocketNotifier{
      co_await webSocketTracker.createOrAcquireDistributor(std::move(queryId)),
      co_await net::this_coro::executor};
}

void WebSocketNotifier::operator()(std::string json) const {
  auto lambda = [](auto distributor, auto json) -> net::awaitable<void> {
    co_await distributor->addQueryStatusUpdate(std::move(json));
  };
  net::co_spawn(executor_, lambda(*distributor_, std::move(json)),
                net::detached);
}
}  // namespace ad_utility::websocket
