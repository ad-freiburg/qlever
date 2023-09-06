//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "util/http/websocket/QueryToSocketDistributor.h"

#include <boost/asio/associated_cancellation_slot.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace ad_utility::websocket {

template <typename CompletionToken>
auto QueryToSocketDistributor::waitForUpdate(CompletionToken&& token) {
  auto initiate = [strand = strand_,
                   wakeupCalls =
                       wakeupCalls_]<typename Handler>(Handler&& self) mutable {
    auto sharedSelf = std::make_shared<Handler>(std::forward<Handler>(self));
    auto cancellationSlot = net::get_associated_cancellation_slot(
        *sharedSelf, net::cancellation_slot());

    net::dispatch(strand, [wakeupCalls, sharedSelf]() mutable {
      wakeupCalls->emplace_back(
          [sharedSelf = std::move(sharedSelf)]() { (*sharedSelf)(); });
    });

    if (cancellationSlot.is_connected()) {
      cancellationSlot.assign(
          [strand = std::move(strand), wakeupCalls = std::move(wakeupCalls)](
              net::cancellation_type) mutable {
            // Make sure wakeupCalls_ is accessed safely.
            net::dispatch(strand, [wakeupCalls = std::move(wakeupCalls)]() {
              wakeupCalls->clear();
            });
          });
    }
  };
  return net::async_initiate<CompletionToken, void()>(
      initiate, std::forward<CompletionToken>(token));
}

void QueryToSocketDistributor::runAndEraseWakeUpCallsSynchronously() {
  for (auto& wakeupCall : *wakeupCalls_) {
    wakeupCall();
  }
  wakeupCalls_->clear();
}

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

net::awaitable<void> QueryToSocketDistributor::signalEnd() {
  co_await net::dispatch(strand_, net::use_awaitable);
  finished_ = true;
  runAndEraseWakeUpCallsSynchronously();
}

net::awaitable<std::shared_ptr<const std::string>>
QueryToSocketDistributor::waitForNextDataPiece(size_t index) {
  co_await net::dispatch(strand_, net::use_awaitable);

  if (index < data_.size()) {
    co_return data_.at(index);
  } else if (finished_) {
    co_return nullptr;
  }

  co_await waitForUpdate(net::use_awaitable);

  if (index < data_.size()) {
    co_return data_.at(index);
  } else {
    if (!finished_) {
      LOG(WARN) << "No new data after waking up" << std::endl;
    }
    co_return nullptr;
  }
}
}  // namespace ad_utility::websocket
