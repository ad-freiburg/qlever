//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_WEBSOCKETNOTIFIER_H
#define QLEVER_WEBSOCKETNOTIFIER_H

#include "util/http/websocket/Common.h"
#include "util/http/websocket/WebSocketManager.h"

namespace ad_utility::websocket {

/// This class wraps bundles an OwningQueryId and a reference to a
/// WebSocketManager together. On Destruction this automatically notifies
/// the WebSocketManager that the query was completed.
class WebSocketNotifier {
  common::OwningQueryId owningQueryId_;
  WebSocketTracker& webSocketTracker_;
  std::shared_ptr<QueryToSocketDistributor> distributor_;

  WebSocketNotifier(common::OwningQueryId owningQueryId,
                    WebSocketTracker& webSocketTracker,
                    std::shared_ptr<QueryToSocketDistributor> distributor)
      : owningQueryId_{std::move(owningQueryId)},
        webSocketTracker_{webSocketTracker},
        distributor_{std::move(distributor)} {}

 public:
  WebSocketNotifier(WebSocketNotifier&&) noexcept = default;

  static net::awaitable<WebSocketNotifier> create(
      common::OwningQueryId owningQueryId, WebSocketTracker& webSocketTracker);

  /// Broadcast the string to all listeners of this query.
  void operator()(std::string) const;

  /// Notifies all listeners the query was completed.
  ~WebSocketNotifier();
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETNOTIFIER_H
