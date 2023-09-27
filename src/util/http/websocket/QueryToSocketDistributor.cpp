//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/Log.h"

namespace ad_utility::websocket {

net::awaitable<void> QueryToSocketDistributor::waitForUpdate() const {
  try {
    co_await infiniteTimer_.async_wait(net::use_awaitable);
    AD_THROW("Infinite timer expired. This should not happen");
  } catch (boost::system::system_error& error) {
    if (error.code() != net::error::operation_aborted) {
      throw;
    }
  }
  // Clear cancellation flag if set, and wake up to allow the caller
  // to return no-data and gracefully end this
  co_await net::this_coro::reset_cancellation_state();
  co_await net::dispatch(strand_, net::use_awaitable);
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
  co_await net::dispatch(strand_, net::use_awaitable);
  AD_CONTRACT_CHECK(!finished_);
  data_.push_back(std::move(sharedPayload));
  wakeUpWaitingListeners();
}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::signalEnd() {
  co_await net::dispatch(strand_, net::use_awaitable);
  AD_CONTRACT_CHECK(!finished_);
  finished_ = true;
  wakeUpWaitingListeners();
  // Invoke cleanup pre-emptively
  std::move(cleanupCall_).invokeManuallyAndCancel();
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPiece(size_t index) const {
  co_await net::dispatch(strand_, net::use_awaitable);

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
