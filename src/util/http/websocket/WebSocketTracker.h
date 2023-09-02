//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_WEBSOCKETTRACKER_H
#define QLEVER_WEBSOCKETTRACKER_H

#include <absl/container/flat_hash_map.h>

#include "util/http/beast.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/EphemeralWaitingList.h"
#include "util/http/websocket/QueryToSocketDistributor.h"

namespace ad_utility::websocket {
using StrandType = net::strand<net::any_io_executor>;
class WebSocketTracker {
  net::io_context& ioContext_;
  StrandType globalStrand_;
  absl::flat_hash_map<QueryId, std::shared_ptr<QueryToSocketDistributor>>
      socketDistributors_{};
  EphemeralWaitingList waitingList_{};

 public:
  explicit WebSocketTracker(net::io_context& ioContext)
      : ioContext_{ioContext}, globalStrand_{net::make_strand(ioContext)} {}

  /// Notifies this class that the given query will no longer receive any
  /// updates, so all waiting connections will be closed.
  void releaseQuery(QueryId queryId);

  net::awaitable<std::shared_ptr<QueryToSocketDistributor>> createDistributor(
      const QueryId&);

  net::awaitable<std::shared_ptr<QueryToSocketDistributor>> waitForDistributor(
      QueryId);
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETTRACKER_H
