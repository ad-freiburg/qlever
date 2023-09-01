//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "QueryToSocketDistributor.h"

namespace ad_utility::websocket {
void QueryToSocketDistributor::runAndEraseWakeUpCallsSynchronously() {
  for (auto& wakeupCall : wakeupCalls_) {
    wakeupCall();
  }
  wakeupCalls_.clear();
}

void QueryToSocketDistributor::wakeUpWebSocketsForQuery() {
  net::post(strand_, [this]() { runAndEraseWakeUpCallsSynchronously(); });
}

void QueryToSocketDistributor::callOnUpdate(std::function<void()> callback) {
  net::post(strand_, [this, callback = std::move(callback)]() mutable {
    wakeupCalls_.emplace_back(std::move(callback));
  });
}

void QueryToSocketDistributor::addQueryStatusUpdate(std::string payload) {
  auto sharedPayload = std::make_shared<const std::string>(std::move(payload));
  net::post(strand_,
            [this, sharedPayload = std::move(sharedPayload)]() mutable {
              data_.push_back(std::move(sharedPayload));
              runAndEraseWakeUpCallsSynchronously();
            });
}

void QueryToSocketDistributor::signalEnd() { finished_ = true; }

bool QueryToSocketDistributor::isFinished() const { return finished_; }

const std::vector<std::shared_ptr<const std::string>>&
QueryToSocketDistributor::getData() {
  return data_;
}
}  // namespace ad_utility::websocket
