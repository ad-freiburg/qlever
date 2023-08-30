//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketNotifier.h"

namespace ad_utility::websocket {
void WebSocketNotifier::operator()(std::string json) const {
  webSocketManager_.addQueryStatusUpdate(owningQueryId_.toQueryId(),
                                         std::move(json));
}

WebSocketNotifier::operator std::function<void(std::string)>() {
  return [this](std::string json) { (*this)(std::move(json)); };
}

WebSocketNotifier::~WebSocketNotifier() {
  webSocketManager_.releaseQuery(owningQueryId_.toQueryId());
}
}  // namespace ad_utility::websocket
