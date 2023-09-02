//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/WebSocketTracker.h"

namespace ad_utility::websocket {
net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
WebSocketTracker::createDistributor(const QueryId& queryId) {
  co_await net::post(globalStrand_, net::use_awaitable);
  AD_CORRECTNESS_CHECK(!socketDistributors_.contains(queryId));
  auto distributor =
      std::make_shared<QueryToSocketDistributor>(queryId, ioContext_);
  socketDistributors_.emplace(queryId, distributor);
  waitingList_.signalQueryStart(queryId);
  co_return distributor;
}

net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
WebSocketTracker::waitForDistributor(QueryId queryId) {
  co_await net::post(globalStrand_, net::use_awaitable);
  if (socketDistributors_.contains(queryId)) {
    co_return socketDistributors_.at(queryId);
  } else {
    co_await waitingList_.waitForQueryStart(queryId);
    // The same strand used in createDistributor, but just to be sure
    co_await net::post(globalStrand_, net::use_awaitable);
    co_return socketDistributors_.at(queryId);
  }
}

void WebSocketTracker::releaseQuery(QueryId queryId) {
  net::post(globalStrand_, [this, queryId = std::move(queryId)]() {
    auto distributor = socketDistributors_.at(queryId);
    socketDistributors_.erase(queryId);
    distributor->signalEnd();
    distributor->wakeUpWebSocketsForQuery();
  });
}
}  // namespace ad_utility::websocket
