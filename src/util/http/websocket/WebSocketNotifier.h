//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_WEBSOCKETNOTIFIER_H
#define QLEVER_WEBSOCKETNOTIFIER_H

#include "util/http/websocket/Common.h"
#include "util/http/websocket/WebSocketManager.h"

namespace ad_utility::websocket {
class WebSocketNotifier {
  common::OwningQueryId owningQueryId_;
  WebSocketManager& webSocketManager_;

 public:
  WebSocketNotifier(common::OwningQueryId owningQueryId,
                    WebSocketManager& webSocketManager)
      : owningQueryId_{std::move(owningQueryId)},
        webSocketManager_{webSocketManager} {}

  void operator()(std::string) const;

  std::function<void(std::string)> toFunction() const;

  ~WebSocketNotifier();
};

}  // namespace ad_utility::websocket

#endif  // QLEVER_WEBSOCKETNOTIFIER_H
