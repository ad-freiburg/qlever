//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateFetcher.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

net::awaitable<UpdateFetcher::PayloadType> UpdateFetcher::waitForEvent() {
  AD_CORRECTNESS_CHECK(distributor_);
  AD_EXPENSIVE_CHECK(strand().running_in_this_thread());

  auto [data, latest] =
      co_await distributor_->waitForNextDataPiece(currentIndex_);
  currentIndex_ = latest;
  co_return data;
}
}  // namespace ad_utility::websocket
