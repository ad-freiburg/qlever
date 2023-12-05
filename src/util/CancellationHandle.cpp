//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#include "CancellationHandle.h"

#include "util/Exception.h"

namespace ad_utility {

template <bool WatchDogEnabled>
void CancellationHandle<WatchDogEnabled>::cancel(CancellationState reason) {
  AD_CONTRACT_CHECK(detail::isCancelled(reason));

  setStatePreservingCancel(reason);
}

// _____________________________________________________________________________
template <bool WatchDogEnabled>
void CancellationHandle<WatchDogEnabled>::startWatchDogInternal()
    requires WatchDogEnabled {
  using enum CancellationState;
  // Setting the atomic here does synchronize memory for assignment,
  // so this is safe without a mutex
  bool running = watchDogRunning_.exchange(true);
  // This function is only supposed to be run once.
  AD_CONTRACT_CHECK(!running);
  watchDogThread_ = ad_utility::JThread{[this]() {
    while (watchDogRunning_) {
      auto state = cancellationState_.load(std::memory_order_relaxed);
      if (state == NOT_CANCELLED) {
        cancellationState_.compare_exchange_strong(state, WAITING_FOR_CHECK,
                                                   std::memory_order_relaxed);
      } else if (state == WAITING_FOR_CHECK) {
        if (cancellationState_.compare_exchange_strong(
                state, CHECK_WINDOW_MISSED, std::memory_order_relaxed)) {
          startTimeoutWindow_ = steady_clock::now();
        }
      }
      std::this_thread::sleep_for(DESIRED_CANCELLATION_CHECK_INTERVAL);
    }
  }};
}

// _____________________________________________________________________________
template <bool WatchDogEnabled>
void CancellationHandle<WatchDogEnabled>::startWatchDog() {
  if constexpr (WatchDogEnabled) {
    startWatchDogInternal();
  }
}

// _____________________________________________________________________________
template <bool WatchDogEnabled>
void CancellationHandle<WatchDogEnabled>::setStatePreservingCancel(
    CancellationState newState) {
  CancellationState state = cancellationState_.load(std::memory_order_relaxed);
  while (!detail::isCancelled(state)) {
    if (cancellationState_.compare_exchange_weak(state, newState,
                                                 std::memory_order_relaxed)) {
      break;
    }
  }
}

// _____________________________________________________________________________
template <bool WatchDogEnabled>
void CancellationHandle<WatchDogEnabled>::resetWatchDogState() {
  if constexpr (WatchDogEnabled) {
    setStatePreservingCancel(CancellationState::NOT_CANCELLED);
  }
}

// _____________________________________________________________________________
template <bool WatchDogEnabled>
CancellationHandle<WatchDogEnabled>::~CancellationHandle() {
  if constexpr (WatchDogEnabled) {
    watchDogRunning_ = false;
  }
}

// Make sure to compile correctly.
template class CancellationHandle<true>;
template class CancellationHandle<false>;
}  // namespace ad_utility
