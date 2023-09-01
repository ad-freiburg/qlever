//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "UpdateFetcher.h"

namespace ad_utility::websocket {

net::awaitable<UpdateFetcher::payload_type> UpdateFetcher::waitForEvent() {
  co_await net::post(socketStrand_, net::use_awaitable);
  if (!distributor_) {
    auto distributor = co_await webSocketTracker_.waitForDistributor(queryId_);
    co_await net::post(socketStrand_, net::use_awaitable);
    distributor_ = std::move(distributor);
  }

  auto data = co_await distributor_->waitForNextDataPiece(nextIndex_);
  co_await net::post(socketStrand_, net::use_awaitable);
  if (data) {
    nextIndex_++;
  }
  co_return data;
}
}  // namespace ad_utility::websocket
