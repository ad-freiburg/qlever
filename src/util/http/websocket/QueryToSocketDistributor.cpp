//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <optional>

#include "util/AsioHelpers.h"
#include "util/Exception.h"

namespace ad_utility::websocket {

// _____________________________________________________________________________
QueryToSocketDistributor::QueryToSocketDistributor(
    net::io_context& ioContext, const std::function<void(bool)>& cleanupCall)
    : strand_{net::make_strand(ioContext)},
      mutex_{ioContext.get_executor()},
      cleanupCall_{
          cleanupCall,
          [](const auto& cleanupCall) { std::invoke(cleanupCall, false); }},
      signalEndCall_{[cleanupCall] { std::invoke(cleanupCall, true); }} {}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::addQueryStatusUpdate(
    std::string payload) {
  auto sharedPayload = std::make_shared<const std::string>(std::move(payload));
  auto guard = co_await mutex_.asyncLockGuard(net::use_awaitable);
  data_.push_back(std::move(sharedPayload));
  signal_.notifyAll();
}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::signalEnd() {
  auto guard = co_await mutex_.asyncLockGuard(net::use_awaitable);
  AD_CONTRACT_CHECK(!finished_);
  finished_ = true;
  signal_.notifyAll();
  // Invoke cleanup pre-emptively
  signalEndCall_();
  std::move(cleanupCall_).cancel();
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPiece(size_t index) const {
  auto guard = co_await mutex_.asyncLockGuard(net::use_awaitable);
  if (index < data_.size()) {
    co_return data_.at(index);
  } else if (finished_) {
    co_return nullptr;
  }
  co_await signal_.asyncWait(mutex_, net::use_awaitable);
  if (index < data_.size()) {
    co_return data_.at(index);
  } else {
    co_return nullptr;
  }
}
}  // namespace ad_utility::websocket
