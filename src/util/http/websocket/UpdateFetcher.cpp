//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateFetcher.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

net::awaitable<UpdateFetcher::PayloadType> UpdateFetcher::waitForEvent() {
  AD_CORRECTNESS_CHECK(distributor_);
  AD_EXPENSIVE_CHECK(strand().running_in_this_thread());

  // NOTE: Deliberately not a structured binding. See `Server::process`: with
  // GCC 15 the hidden object behind a structured binding in a coroutine is
  // never destroyed, which would leak the payload on every update.
  auto dataAndLatest =
      co_await distributor_->waitForNextDataPiece(currentIndex_);
  currentIndex_ = dataAndLatest.second;
  co_return dataAndLatest.first;
}
}  // namespace ad_utility::websocket
