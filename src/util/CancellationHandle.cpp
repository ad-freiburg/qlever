//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "CancellationHandle.h"

#include "util/Exception.h"

namespace ad_utility {
using detail::CancellationMode;

template <CancellationMode Mode>
void CancellationHandle<Mode>::cancel(
    [[maybe_unused]] CancellationState reason) {
  if constexpr (CancellationEnabled) {
    AD_CONTRACT_CHECK(detail::isCancelled(reason));

    setStatePreservingCancel(reason);
  }
}

// _____________________________________________________________________________
template <CancellationMode Mode>
CPP_member_def auto CancellationHandle<Mode>::startWatchDogInternal()
    -> CPP_ret(void)(requires WatchDogEnabled) {
  using enum CancellationState;
  std::unique_lock lock{watchDogState_.mutex_};
  // This function is only supposed to be run once.
  AD_CONTRACT_CHECK(!watchDogState_.running_);
  watchDogState_.running_ = true;
  watchDogThread_ = ad_utility::JThread{[this]() {
    std::unique_lock lock{watchDogState_.mutex_};
    do {
      auto state = cancellationState_.load(std::memory_order_relaxed);
      if (state == NOT_CANCELLED) {
        cancellationState_.compare_exchange_strong(state, WAITING_FOR_CHECK,
                                                   std::memory_order_relaxed);
      } else if (state == WAITING_FOR_CHECK) {
        // This variable needs to be set before compare exchange,
        // otherwise another thread might read an old value before
        // the new value is set. This might lead to redundant stores,
        // which is acceptable here.
        startTimeoutWindow_ = steady_clock::now();
        cancellationState_.compare_exchange_strong(state, CHECK_WINDOW_MISSED,
                                                   std::memory_order_relaxed);
      } else if (detail::isCancelled(state)) {
        // No need to keep the watchdog running if the handle was cancelled
        // already
        break;
      }
    } while (!watchDogState_.conditionVariable_.wait_for(
        lock, DESIRED_CANCELLATION_CHECK_INTERVAL,
        [this]() { return !watchDogState_.running_; }));
  }};
}

// _____________________________________________________________________________
template <CancellationMode Mode>
void CancellationHandle<Mode>::startWatchDog() {
  if constexpr (WatchDogEnabled) {
    startWatchDogInternal();
  }
}

// _____________________________________________________________________________
template <CancellationMode Mode>
CPP_member_def auto CancellationHandle<Mode>::setStatePreservingCancel(
    CancellationState newState) -> CPP_ret(void)(requires CancellationEnabled) {
  CancellationState state = cancellationState_.load(std::memory_order_relaxed);
  while (!detail::isCancelled(state)) {
    if (cancellationState_.compare_exchange_weak(state, newState,
                                                 std::memory_order_relaxed)) {
      break;
    }
  }
}

// _____________________________________________________________________________
template <CancellationMode Mode>
void CancellationHandle<Mode>::resetWatchDogState() {
  if constexpr (WatchDogEnabled) {
    setStatePreservingCancel(CancellationState::NOT_CANCELLED);
  }
}

// _____________________________________________________________________________
template <CancellationMode Mode>
CancellationHandle<Mode>::~CancellationHandle() {
  if constexpr (WatchDogEnabled) {
    {
      std::unique_lock lock{watchDogState_.mutex_};
      watchDogState_.running_ = false;
    }
    watchDogState_.conditionVariable_.notify_all();
  }
}

// Make sure to compile correctly.
template class CancellationHandle<CancellationMode::ENABLED>;
template class CancellationHandle<CancellationMode::NO_WATCH_DOG>;
template class CancellationHandle<CancellationMode::DISABLED>;
}  // namespace ad_utility
