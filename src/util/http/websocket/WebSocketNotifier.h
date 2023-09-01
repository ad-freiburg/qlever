//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_WEBSOCKETNOTIFIER_H
#define QLEVER_WEBSOCKETNOTIFIER_H

#include "util/CleanupDeleter.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/WebSocketManager.h"

namespace ad_utility::websocket {
using cleanup_deleter::CleanupDeleter;

/// This class wraps bundles an OwningQueryId and a reference to a
/// WebSocketManager together. On Destruction this automatically notifies
/// the WebSocketManager that the query was completed.
class WebSocketNotifier {
  CleanupDeleter<common::OwningQueryId> owningQueryId_;
  std::shared_ptr<QueryToSocketDistributor> distributor_;

  WebSocketNotifier(common::OwningQueryId owningQueryId,
                    WebSocketTracker& webSocketTracker,
                    std::shared_ptr<QueryToSocketDistributor> distributor)
      : owningQueryId_{std::move(owningQueryId),
                       [&webSocketTracker](auto&& owningQueryId) {
                         webSocketTracker.releaseQuery(
                             std::move(owningQueryId.toQueryId()));
                       }},
        distributor_{std::move(distributor)} {}

 public:
  WebSocketNotifier(WebSocketNotifier&&) noexcept = default;

  static net::awaitable<WebSocketNotifier> create(
      common::OwningQueryId owningQueryId, WebSocketTracker& webSocketTracker);

  /// Broadcast the string to all listeners of this query.
  void operator()(std::string) const;
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETNOTIFIER_H
