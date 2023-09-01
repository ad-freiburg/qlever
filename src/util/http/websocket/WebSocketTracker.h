//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_WEBSOCKETTRACKER_H
#define QLEVER_WEBSOCKETTRACKER_H
#include "util/http/beast.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/EphemeralWaitingList.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace ad_utility::websocket {
class WebSocketTracker {
  net::io_context& ioContext_;
  net::strand<net::io_context::executor_type>& socketStrand_;
  absl::flat_hash_map<QueryId, std::shared_ptr<QueryToSocketDistributor>>
      socketDistributors_{};
  EphemeralWaitingList waitingList_{};

 public:
  explicit WebSocketTracker(net::io_context& ioContext,
                            net::strand<net::io_context::executor_type>& strand)
      : ioContext_{ioContext}, socketStrand_{strand} {}

  /// Notifies this class that the given query will no longer receive any
  /// updates, so all waiting connections will be closed.
  void releaseQuery(QueryId queryId);

  std::shared_ptr<QueryToSocketDistributor> createDistributor(const QueryId&);

  template <typename CompletionToken>
  net::awaitable<std::shared_ptr<QueryToSocketDistributor>> waitForDistributor(
      QueryId queryId, CompletionToken&& token) {
    auto initiate = [this, &queryId]<typename Handler>(Handler&& self) mutable {
      // TODO convert to coroutine
      auto sharedSelf = std::make_shared<Handler>(std::forward<Handler>(self));

      net::post(socketStrand_, [this, &queryId,
                                sharedSelf = std::move(sharedSelf)]() mutable {
        if (socketDistributors_.contains(queryId)) {
          (*sharedSelf)(socketDistributors_.at(queryId));
        } else {
          waitingList_.callOnQueryUpdate(
              queryId, [this, queryId, sharedSelf = std::move(sharedSelf)]() {
                (*sharedSelf)(socketDistributors_.at(queryId));
              });
        }
      });
    };
    return net::async_initiate<CompletionToken,
                               void(std::shared_ptr<QueryToSocketDistributor>)>(
        initiate, std::forward<CompletionToken>(token));
  }
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETTRACKER_H
