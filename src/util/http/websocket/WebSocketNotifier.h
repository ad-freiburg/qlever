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
  WebSocketManager& webSocketManager_;

 public:
  WebSocketNotifier(common::OwningQueryId owningQueryId,
                    WebSocketManager& webSocketManager)
      : owningQueryId_{std::move(owningQueryId)},
        webSocketManager_{webSocketManager} {}

  /// Broadcast the string to all listeners of this query.
  void operator()(std::string) const;

  /// Notifies all listeners the query was completed.
  ~WebSocketNotifier();
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETNOTIFIER_H
