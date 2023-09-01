//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketNotifier.h"

#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>

namespace ad_utility::websocket {

net::awaitable<WebSocketNotifier> WebSocketNotifier::create(
    common::OwningQueryId owningQueryId, WebSocketTracker& webSocketTracker) {
  auto initialExecutor = co_await net::this_coro::executor;

  auto distributor =
      co_await webSocketTracker.createDistributor(owningQueryId.toQueryId());

  co_await net::post(initialExecutor, net::use_awaitable);

  co_return WebSocketNotifier{std::move(owningQueryId), webSocketTracker,
                              std::move(distributor)};
}

void WebSocketNotifier::operator()(std::string json) const {
  distributor_->addQueryStatusUpdate(std::move(json));
}

WebSocketNotifier::~WebSocketNotifier() {
  webSocketTracker_.releaseQuery(owningQueryId_.toQueryId());
}
}  // namespace ad_utility::websocket
