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

/// Class that provides the functionality to create, acquire and remove
/// a `QueryToSocketDistributor`. All operations are synchronized on a strand
/// unique for this class, so in the common case of this class being used
/// globally the provided thread-safety comes at a cost.
class WebSocketTracker {
  net::io_context& ioContext_;
  StrandType globalStrand_;
  absl::flat_hash_map<QueryId, std::shared_ptr<QueryToSocketDistributor>>
      socketDistributors_{};
  EphemeralWaitingList waitingList_{};

 public:
  explicit WebSocketTracker(net::io_context& ioContext)
      : ioContext_{ioContext}, globalStrand_{net::make_strand(ioContext)} {}

  /// Creates a new `QueryToSocketDistributor` and registers it.
  net::awaitable<std::shared_ptr<QueryToSocketDistributor>> createDistributor(
      const QueryId&);

  /// Waits until `QueryToSocketDistributor` is registered and returns it.
  net::awaitable<std::shared_ptr<QueryToSocketDistributor>> waitForDistributor(
      QueryId);

  /// Notifies this class that the given query will no longer receive any
  /// updates, so the distributor will be unregistered (it will continue to
  /// exists for all objects still holding a shared pointer to the instance) and
  /// signaled that all updates have ended.
  net::awaitable<void> releaseDistributor(QueryId queryId);
};
}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETTRACKER_H
