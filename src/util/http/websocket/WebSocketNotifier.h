//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_WEBSOCKETNOTIFIER_H
#define QLEVER_WEBSOCKETNOTIFIER_H

#include "util/UniqueCleanup.h"
#include "util/http/websocket/Common.h"
#include "util/http/websocket/WebSocketManager.h"

namespace ad_utility::websocket {
using unique_cleanup::UniqueCleanup;

/// This class wraps bundles an OwningQueryId and a reference to a
/// WebSocketTracker together. On Destruction this automatically notifies
/// the WebSocketTracker that the query was completed.
class WebSocketNotifier {
  UniqueCleanup<common::OwningQueryId> owningQueryId_;
  std::shared_ptr<QueryToSocketDistributor> distributor_;
  net::any_io_executor executor_;

  WebSocketNotifier(common::OwningQueryId owningQueryId,
                    WebSocketTracker& webSocketTracker,
                    std::shared_ptr<QueryToSocketDistributor> distributor,
                    net::any_io_executor executor)
      : owningQueryId_{std::move(owningQueryId),
                       [&webSocketTracker, executor](auto&& owningQueryId) {
                         net::co_spawn(
                             executor,
                             webSocketTracker.releaseDistributor(
                                 std::move(owningQueryId.toQueryId())),
                             net::detached);
                       }},
        distributor_{std::move(distributor)},
        executor_{std::move(executor)} {}

 public:
  WebSocketNotifier(WebSocketNotifier&&) noexcept = default;

  /// Asynchronously creates an instance of this class. This is because because
  /// creating a distributor for this class needs to be done in a synchronized
  /// way.
  static net::awaitable<WebSocketNotifier> create(
      common::OwningQueryId owningQueryId, WebSocketTracker& webSocketTracker);

  /// Broadcast the string to all listeners of this query asynchronously.
  void operator()(std::string) const;
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETNOTIFIER_H
