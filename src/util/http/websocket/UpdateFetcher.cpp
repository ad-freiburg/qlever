//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateFetcher.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

net::awaitable<UpdateFetcher::PayloadType> UpdateFetcher::waitForEvent() {
  if (!distributor_) {
    distributor_ =
        co_await queryHub_.createOrAcquireDistributorForReceiving(queryId_);
  }

  auto data =
      co_await sameExecutor(distributor_->waitForNextDataPiece(nextIndex_));
  if (data) {
    nextIndex_++;
  }
  co_return data;
}
}  // namespace ad_utility::websocket
