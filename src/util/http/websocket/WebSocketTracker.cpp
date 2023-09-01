//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketTracker.h"

namespace ad_utility::websocket {
std::shared_ptr<QueryToSocketDistributor> WebSocketTracker::createDistributor(
    const QueryId& queryId) {
  auto future = net::post(
      socketStrand_,
      std::packaged_task<std::shared_ptr<QueryToSocketDistributor>()>(
          [this, queryId]() {
            AD_CORRECTNESS_CHECK(!socketDistributors_.contains(queryId));
            auto distributor =
                std::make_shared<QueryToSocketDistributor>(queryId, ioContext_);
            socketDistributors_.emplace(queryId, distributor);
            waitingList_.signalQueryUpdate(queryId);
            return distributor;
          }));
  return future.get();
}

void WebSocketTracker::invokeOnQueryStart(
    const QueryId& queryId,
    std::function<void(std::shared_ptr<QueryToSocketDistributor>)> callback) {
  net::post(socketStrand_,
            [this, queryId, callback = std::move(callback)]() mutable {
              if (socketDistributors_.contains(queryId)) {
                callback(socketDistributors_.at(queryId));
              } else {
                waitingList_.callOnQueryUpdate(
                    queryId, [this, queryId, callback = std::move(callback)]() {
                      callback(socketDistributors_.at(queryId));
                    });
              }
            });
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
