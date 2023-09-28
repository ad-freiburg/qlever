//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateFetcher.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

net::awaitable<UpdateFetcher::PayloadType> UpdateFetcher::waitForEvent() {
  try {
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
  } catch (boost::system::system_error& error) {
    if (error.code() == net::error::operation_aborted) {
      co_return nullptr;
    }
    throw;
  }
}
}  // namespace ad_utility::websocket
