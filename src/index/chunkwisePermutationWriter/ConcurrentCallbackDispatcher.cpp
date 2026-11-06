// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "index/chunkwisePermutationWriter/ConcurrentCallbackDispatcher.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <algorithm>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace chunkwisePermutationWriter {

// _____________________________________________________________________________
ConcurrentCallbackDispatcher::ConcurrentCallbackDispatcher(
    net::any_io_executor executor,
    std::vector<ConcurrentBlockCallback> callbacks, size_t maxNumBlocksInFlight,
    ReportException reportException)
    : callbacks_{std::move(callbacks)},
      reportException_{std::move(reportException)},
      permits_{std::move(executor), std::max<size_t>(1, maxNumBlocksInFlight)} {
  AD_CONTRACT_CHECK(reportException_ != nullptr);
}

// _____________________________________________________________________________
net::awaitable<bool> ConcurrentCallbackDispatcher::asyncDispatch(
    SharedBlock block, std::shared_ptr<void> keepAlive) {
  if (callbacks_.empty()) {
    co_return true;
  }
  // NOTE: The `cancel` of the semaphore is not sticky (see
  // `AsyncResourcePool::cancel`), hence the additional checks of `cancelled_`.
  if (cancelled_.load()) {
    co_return false;
  }
  auto [errorCode, permit] =
      co_await permits_.asyncAcquire(net::as_tuple(net::use_awaitable));
  if (errorCode || cancelled_.load()) {
    co_return false;
  }
  invokeCallbacks(std::move(block), std::move(permit), std::move(keepAlive));
  co_return true;
}

// _____________________________________________________________________________
void ConcurrentCallbackDispatcher::cancel() {
  cancelled_.store(true);
  permits_.cancel();
}

// _____________________________________________________________________________
void ConcurrentCallbackDispatcher::waitUntilIdle() {
  std::unique_lock lock{mutex_};
  conditionVariable_.wait(lock, [this]() { return numBlocksInFlight_ == 0; });
}

// _____________________________________________________________________________
void ConcurrentCallbackDispatcher::invokeCallbacks(
    SharedBlock block, Permit permit, std::shared_ptr<void> keepAlive) {
  // The state that the `done` callbacks of all the callbacks of this block
  // share. The permit is given back (and the block is no longer in flight)
  // once the last of them is done.
  struct Join {
    ConcurrentCallbackDispatcher* self_;
    std::shared_ptr<void> keepAlive_;
    Permit permit_;
    std::atomic<size_t> remaining_;
    Join(ConcurrentCallbackDispatcher* self, std::shared_ptr<void> keepAlive,
         Permit permit, size_t remaining)
        : self_{self},
          keepAlive_{std::move(keepAlive)},
          permit_{std::move(permit)},
          remaining_{remaining} {
      std::lock_guard lock{self_->mutex_};
      ++self_->numBlocksInFlight_;
    }
  };
  auto join = std::make_shared<Join>(this, std::move(keepAlive),
                                     std::move(permit), callbacks_.size());
  auto done = [join](std::exception_ptr exception) {
    // NOTE: The copy of the `keepAlive_` keeps this dispatcher alive until this
    // function has returned, in particular across the `notify_all` below,
    // after which the owner may destroy the dispatcher at any time.
    auto keepAlive = join->keepAlive_;
    auto* self = join->self_;
    if (exception != nullptr) {
      self->reportException_(std::move(exception));
    }
    if (join->remaining_.fetch_sub(1) == 1) {
      join->permit_ = Permit{};
      {
        std::lock_guard lock{self->mutex_};
        --self->numBlocksInFlight_;
      }
      self->conditionVariable_.notify_all();
    }
  };
  for (const auto& callback : callbacks_) {
    // NOTE: A callback that throws (instead of reporting its exception via
    // `done`) is treated as if it had reported the exception. It must not
    // throw after it has invoked `done`.
    try {
      callback(block, done);
    } catch (...) {
      done(std::current_exception());
    }
  }
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
