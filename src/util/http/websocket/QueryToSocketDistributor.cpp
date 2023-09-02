//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include <boost/asio/use_awaitable.hpp>

namespace ad_utility::websocket {

template <typename CompletionToken>
auto QueryToSocketDistributor::waitForUpdate(CompletionToken&& token) {
  auto initiate = [this]<typename Handler>(Handler&& self) mutable {
    auto callback = [selfPtr = std::make_shared<Handler>(
                         std::forward<Handler>(self))]() { (*selfPtr)(); };

    net::post(strand_, [this, callback = std::move(callback)]() mutable {
      wakeupCalls_.emplace_back(std::move(callback));
    });
  };
  return net::async_initiate<CompletionToken, void()>(
      initiate, std::forward<CompletionToken>(token));
}

void QueryToSocketDistributor::runAndEraseWakeUpCallsSynchronously() {
  for (auto& wakeupCall : wakeupCalls_) {
    wakeupCall();
  }
  wakeupCalls_.clear();
}

net::awaitable<void> QueryToSocketDistributor::wakeUpWebSocketsForQuery() {
  co_await net::post(strand_, net::use_awaitable);
  runAndEraseWakeUpCallsSynchronously();
}

net::awaitable<void> QueryToSocketDistributor::addQueryStatusUpdate(
    std::string payload) {
  auto sharedPayload = std::make_shared<const std::string>(std::move(payload));
  co_await net::post(strand_, net::use_awaitable);
  data_.push_back(std::move(sharedPayload));
  runAndEraseWakeUpCallsSynchronously();
}

void QueryToSocketDistributor::signalEnd() { finished_ = true; }

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPiece(size_t index) {
  co_await net::post(strand_, net::use_awaitable);

  if (index < data_.size()) {
    co_return data_.at(index);
  }

  co_await waitForUpdate(net::use_awaitable);

  if (index < data_.size()) {
    co_return data_.at(index);
  } else {
    if (!finished_) {
      LOG(WARN) << "No new data after waking up" << std::endl;
    }
    co_return nullptr;
  }
}
}  // namespace ad_utility::websocket
