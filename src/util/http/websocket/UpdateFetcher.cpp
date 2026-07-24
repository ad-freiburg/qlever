//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/UpdateFetcher.h"

#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

net::awaitable<UpdateFetcher::PayloadType> UpdateFetcher::waitForEvent() {
  AD_CORRECTNESS_CHECK(distributor_);
  AD_EXPENSIVE_CHECK(strand().running_in_this_thread());

  // Workaround for a GCC 15/16 bug: the hidden object of a by-value
  // structured binding is not destroyed when the coroutine frame is
  // destroyed while suspended (gcc.gnu.org bug 124584).
  auto dataPiece = co_await distributor_->waitForNextDataPiece(currentIndex_);
  auto& [data, latest] = dataPiece;
  currentIndex_ = latest;
  co_return data;
}
}  // namespace ad_utility::websocket
