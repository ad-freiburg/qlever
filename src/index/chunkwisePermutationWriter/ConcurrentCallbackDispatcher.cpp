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
    std::shared_ptr<ConcurrentCallbackDispatcher> self, SharedBlock block) {
  AD_CONTRACT_CHECK(self != nullptr);
  if (self->callbacks_.empty()) {
    co_return true;
  }
  // NOTE: The `cancel` of the semaphore only completes the waits that are
  // pending at the time of the `cancel`, it is not sticky (see
  // `AsyncResourcePool::cancel`). Therefore `wasCancelled_` is checked twice:
  // before the wait (for a `cancel` that happened before this call, after
  // which the wait would succeed as soon as a slot is free), and after the
  // wait (for a `cancel` that happened while the wait was already succeeding).
  if (self->wasCancelled_.load()) {
    co_return false;
  }
  auto [errorCode, permit] =
      co_await self->permits_.asyncAcquire(net::as_tuple(net::use_awaitable));
  if (errorCode || self->wasCancelled_.load()) {
    co_return false;
  }
  invokeCallbacks(std::move(self), std::move(block), std::move(permit));
  co_return true;
}

// _____________________________________________________________________________
void ConcurrentCallbackDispatcher::cancel() {
  wasCancelled_.store(true);
  permits_.cancel();
}

// _____________________________________________________________________________
void ConcurrentCallbackDispatcher::waitUntilIdle() {
  std::unique_lock lock{mutex_};
  conditionVariable_.wait(lock, [this]() { return numBlocksInFlight_ == 0; });
}

// _____________________________________________________________________________
void ConcurrentCallbackDispatcher::invokeCallbacks(
    std::shared_ptr<ConcurrentCallbackDispatcher> self, SharedBlock block,
    Permit permit) {
  // The state that the `done` callbacks of all the callbacks of this block
  // share. The permit is given back (and the block is no longer in flight)
  // once the last of them is done.
  struct Join {
    std::shared_ptr<ConcurrentCallbackDispatcher> self_;
    Permit permit_;
    std::atomic<size_t> remaining_;
    Join(std::shared_ptr<ConcurrentCallbackDispatcher> self, Permit permit,
         size_t remaining)
        : self_{std::move(self)},
          permit_{std::move(permit)},
          remaining_{remaining} {
      std::lock_guard lock{self_->mutex_};
      ++self_->numBlocksInFlight_;
    }
  };
  auto& callbacks = self->callbacks_;
  auto join = std::make_shared<Join>(std::move(self), std::move(permit),
                                     callbacks.size());
  auto done = [join](std::exception_ptr exception) {
    // NOTE: The copy of the `self_` keeps the dispatcher alive until this
    // function has returned, in particular across the `notify_all` below,
    // after which the owner may destroy the dispatcher at any time.
    auto self = join->self_;
    // NOTE: This runs on the thread of the callback, so the `reportException_`
    // is called concurrently, which its contract allows (see
    // `ReportException`).
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
  for (auto& callback : callbacks) {
    // Every callback gets a `DoneCallback` of its own, because a
    // `DoneCallback` is consumed by its (single) invocation. A callback that
    // throws (instead of reporting its exception via its `DoneCallback`) is
    // treated as if it had reported the exception, so it must not throw after
    // it has invoked its `DoneCallback`.
    //
    // NOTE: The callbacks are called concurrently for different blocks, which
    // their contract allows (see `ConcurrentBlockCallback`).
    try {
      callback(block, DoneCallback{done});
    } catch (...) {
      done(std::current_exception());
    }
  }
}

}  // namespace chunkwisePermutationWriter

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
