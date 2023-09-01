//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketTracker.h"

namespace ad_utility::websocket {
net::awaitable<std::shared_ptr<QueryToSocketDistributor>>
WebSocketTracker::createDistributor(const QueryId& queryId) {
  co_await net::post(socketStrand_, net::use_awaitable);
  AD_CORRECTNESS_CHECK(!socketDistributors_.contains(queryId));
  auto distributor =
      std::make_shared<QueryToSocketDistributor>(queryId, ioContext_);
  socketDistributors_.emplace(queryId, distributor);
  waitingList_.signalQueryUpdate(queryId);
  co_return distributor;
}

void WebSocketTracker::releaseQuery(QueryId queryId) {
  net::post(socketStrand_, [this, queryId = std::move(queryId)]() {
    auto distributor = socketDistributors_.at(queryId);
    socketDistributors_.erase(queryId);
    distributor->signalEnd();
    distributor->wakeUpWebSocketsForQuery();
  });
}
}  // namespace ad_utility::websocket
