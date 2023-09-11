//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "util/Log.h"

namespace ad_utility::websocket {

net::awaitable<void> QueryToSocketDistributor::waitForUpdate() {
  try {
    co_await infiniteTimer_.async_wait(net::use_awaitable);
    AD_THROW("Infinite timer expired. This should not happen");
  } catch (boost::system::system_error& error) {
    if (error.code() != boost::asio::error::operation_aborted) {
      throw;
    }
  }
  // Clear cancellation flag if set, and wake up to allow the caller
  // to return no-data and gracefully end this
  co_await net::this_coro::reset_cancellation_state();
  co_await net::dispatch(strand_, net::use_awaitable);
}

// _____________________________________________________________________________

void QueryToSocketDistributor::runAndEraseWakeUpCallsSynchronously() {
  // Setting the time anew automatically cancels waiting operations
  infiniteTimer_.expires_at(boost::posix_time::pos_infin);
}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::addQueryStatusUpdate(
    std::string payload) {
  auto sharedPayload = std::make_shared<const std::string>(std::move(payload));
  co_await net::dispatch(strand_, net::use_awaitable);
  if (finished_) {
    // This warning indicates that something is wrong with your code
    LOG(WARN) << "Added query status after being finished. Doing nothing"
                 "in order to prevent surprising behaviour!"
              << std::endl;
    co_return;
  }
  data_.push_back(std::move(sharedPayload));
  runAndEraseWakeUpCallsSynchronously();
}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::signalEnd() {
  co_await net::dispatch(strand_, net::use_awaitable);
  AD_CORRECTNESS_CHECK(!finished_);
  finished_ = true;
  runAndEraseWakeUpCallsSynchronously();
  // Invoke cleanup pre-emptively
  auto cleanupCall = std::move(cleanupCall_);
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPiece(size_t index) {
  co_await net::this_coro::reset_cancellation_state(
      [](auto) { return net::cancellation_type::terminal; });
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

// _____________________________________________________________________________

net::awaitable<bool> QueryToSocketDistributor::signalStartIfNotStarted() {
  co_await net::dispatch(strand_, net::use_awaitable);
  if (!started_) {
    started_ = true;
    co_return false;
  }
  co_return true;
}
}  // namespace ad_utility::websocket
