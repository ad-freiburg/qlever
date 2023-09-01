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
  void invokeOnQueryStart(
      const QueryId&,
      std::function<void(std::shared_ptr<QueryToSocketDistributor>)>);
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETTRACKER_H
