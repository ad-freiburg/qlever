//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "WebSocketNotifier.h"

namespace ad_utility::websocket {
void WebSocketNotifier::operator()(std::string json) const {
  distributor_->addQueryStatusUpdate(std::move(json));
}

WebSocketNotifier::~WebSocketNotifier() {
  webSocketTracker_.releaseQuery(owningQueryId_.toQueryId());
}
}  // namespace ad_utility::websocket
