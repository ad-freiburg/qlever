//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/deferred.hpp>

#include "util/Exception.h"
#include "util/AsioHelpers.h"

namespace ad_utility::websocket {

// _____________________________________________________________________________
QueryToSocketDistributor::QueryToSocketDistributor(
    net::io_context& ioContext, const std::function<void(bool)>& cleanupCall)
    : strand_{net::make_strand(ioContext)},
      infiniteTimer_{strand_, static_cast<net::deadline_timer::time_type>(
                                  boost::posix_time::pos_infin)},
      cleanupCall_{
          cleanupCall,
          [](const auto& cleanupCall) { std::invoke(cleanupCall, false); }},
      signalEndCall_{[cleanupCall] { std::invoke(cleanupCall, true); }} {}

// _____________________________________________________________________________
net::awaitable<void> QueryToSocketDistributor::waitForUpdate() const {
  auto [error] = co_await infiniteTimer_.async_wait(
      net::bind_executor(strand_, net::as_tuple(net::use_awaitable)));
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
  auto impl = [this, sharedPayload=std::move(sharedPayload)]() {
    AD_CONTRACT_CHECK(!finished_);
    data_.push_back(std::move(sharedPayload));
    wakeUpWaitingListeners();
  };
  return ad_utility::runFunctionOnStrand(strand_, impl, net::use_awaitable);
}

// _____________________________________________________________________________

net::awaitable<void> QueryToSocketDistributor::signalEnd() {
  auto impl = [this]() {
    AD_CONTRACT_CHECK(!finished_);
    finished_ = true;
    wakeUpWaitingListeners();
    // Invoke cleanup pre-emptively
    signalEndCall_();
    std::move(cleanupCall_).cancel();
  };
  co_await ad_utility::runFunctionOnStrand(strand_, impl, net::deferred);
}

// _____________________________________________________________________________

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPieceUnguarded(size_t index) const {
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

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPiece(size_t index) const {
  return ad_utility::runAwaitableOnStrandAwaitable(
      strand_, waitForNextDataPieceUnguarded(index));
}
}  // namespace ad_utility::websocket
