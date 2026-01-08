//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include "util/Exception.h"

namespace ad_utility::websocket {

// _____________________________________________________________________________
QueryToSocketDistributor::QueryToSocketDistributor(
    net::io_context& ioContext, const std::function<void(bool)>& cleanupCall)
    : strand_{net::make_strand(ioContext)},
      infiniteTimer_{strand_, net::steady_timer::time_point::max()},
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
  infiniteTimer_.expires_at(net::steady_timer::time_point::max());
}

// _____________________________________________________________________________

void QueryToSocketDistributor::addQueryStatusUpdate(std::string payload) {
  auto sharedPayload = std::make_shared<const std::string>(std::move(payload));
  // Technically the `shared_from_this` is not required, because the destructor
  // will post something that keeps `*this` alive long enough in combination
  // with the FIFO guarantees from Boost:Asio. We still do it, as it makes the
  // reasoning much simpler.
  auto impl = [self = shared_from_this(),
               sharedPayload = std::move(sharedPayload)]() mutable {
    self->data_ = std::move(sharedPayload);
    self->currentIndex_++;
    self->wakeUpWaitingListeners();
  };
  AD_CONTRACT_CHECK(!finished_.test());
  net::post(strand_, std::move(impl));
}

// _____________________________________________________________________________
void QueryToSocketDistributor::signalEnd() {
  auto impl = [self = shared_from_this()]() { self->wakeUpWaitingListeners(); };
  if (finished_.test_and_set()) {
    // Only one call to signal end is allowed.
    AD_FAIL();
  }
  signalEndCall_();
  std::move(cleanupCall_).cancel();
  net::post(strand_, std::move(impl));
}

// _____________________________________________________________________________
net::awaitable<std::pair<std::shared_ptr<const std::string>, size_t>>
QueryToSocketDistributor::waitForNextDataPiece(size_t oldIndex) const {
  AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
  if (oldIndex < currentIndex_) {
    co_return std::make_pair(data_, currentIndex_);
  } else if (finished_.test()) {
    co_return std::make_pair(nullptr, currentIndex_);
  }

  co_await waitForUpdate();

  // When `waitForUpdate()` returns, `currentIteration_` might have a different
  // value.
  if (oldIndex < currentIndex_) {
    co_return std::make_pair(data_, currentIndex_);
  } else {
    co_return std::make_pair(nullptr, currentIndex_);
  }
}
}  // namespace ad_utility::websocket
