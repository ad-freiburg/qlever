//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/Log.h"

namespace ad_utility::websocket {

auto QueryToSocketDistributor::useStrandedAwaitable() const {
  return net::bind_executor(strand_, net::use_awaitable);
}

net::awaitable<void> QueryToSocketDistributor::waitForUpdate() const {
  auto [error] =
      co_await infiniteTimer_.async_wait(net::as_tuple(useStrandedAwaitable()));
  // If this fails this means the infinite timer expired or aborted with an
  // unexpected error code. This should not happen at all
  AD_CORRECTNESS_CHECK(error == net::error::operation_aborted);
  AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
  // Clear cancellation flag if set, and wake up to allow the caller
  // to return no-data and gracefully end this
  co_await net::this_coro::reset_cancellation_state();
}

// _____________________________________________________________________________

void QueryToSocketDistributor::wakeUpWaitingListeners() {
  // Setting the time anew automatically cancels waiting operations
  infiniteTimer_.expires_at(boost::posix_time::pos_infin);
}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::addQueryStatusUpdate(
    std::string payload) {
  auto sharedPayload = std::make_shared<const std::string>(std::move(payload));
  co_await net::dispatch(useStrandedAwaitable());
  AD_CONTRACT_CHECK(!finished_);
  data_.push_back(std::move(sharedPayload));
  wakeUpWaitingListeners();
}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::signalEnd() {
  co_await net::dispatch(useStrandedAwaitable());
  AD_CONTRACT_CHECK(!finished_);
  finished_ = true;
  wakeUpWaitingListeners();
  // Invoke cleanup pre-emptively
  std::move(cleanupCall_).invokeManuallyAndCancel();
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPiece(size_t index) const {
  co_await net::dispatch(useStrandedAwaitable());

  if (index < data_.size()) {
    co_return data_.at(index);
  } else if (finished_) {
    co_return nullptr;
  }

  co_await waitForUpdate();

  if (index < data_.size()) {
    co_return data_.at(index);
  } else {
    co_return nullptr;
  }
}
}  // namespace ad_utility::websocket
