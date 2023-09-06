//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateFetcher.h"

namespace ad_utility::websocket {

net::awaitable<UpdateFetcher::PayloadType> UpdateFetcher::waitForEvent() {
  co_await net::dispatch(socketStrand_, net::use_awaitable);
  if (!distributor_) {
    distributor_ =
        co_await webSocketTracker_.createOrAcquireDistributor(queryId_);
  }

  auto data = co_await distributor_->waitForNextDataPiece(nextIndex_);
  co_await net::dispatch(socketStrand_, net::use_awaitable);
  if (data) {
    nextIndex_++;
  }
  co_return data;
}
}  // namespace ad_utility::websocket
