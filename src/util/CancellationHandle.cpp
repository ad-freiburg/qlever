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
  // Setting the atomic here does synchronize memory for assignment,
  // so this is safe without a mutex
  bool running = watchDogRunning_.exchange(true);
  AD_CONTRACT_CHECK(!running);
  watchDogThread_ = ad_utility::JThread{[this]() {
    while (watchDogRunning_.load(std::memory_order_relaxed)) {
      auto state = cancellationState_.load(std::memory_order_relaxed);
      if (state == CancellationState::NOT_CANCELLED) {
        cancellationState_.compare_exchange_strong(
            state, CancellationState::WAITING_FOR_CHECK);
      } else if (state == CancellationState::WAITING_FOR_CHECK) {
        if (cancellationState_.compare_exchange_strong(
                state, CancellationState::CHECK_WINDOW_MISSED)) {
          startTimeoutWindow_ =
              std::chrono::steady_clock::now().time_since_epoch().count();
        }
      }
      std::this_thread::sleep_for(detail::DESIRED_CHECK_INTERVAL);
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
    setStatePreservingCancel(CancellationState::WAITING_FOR_CHECK);
  }
}

// _____________________________________________________________________________
template <bool WatchDogEnabled>
CancellationHandle<WatchDogEnabled>::~CancellationHandle() {
  if constexpr (WatchDogEnabled) {
    watchDogRunning_.store(false, std::memory_order_relaxed);
  }
}

// _____________________________________________________________________________
template <bool WatchDogEnabled>
decltype(detail::DESIRED_CHECK_INTERVAL)
CancellationHandle<WatchDogEnabled>::computeCheckMissDuration() const
    requires WatchDogEnabled {
  using namespace std::chrono;
  using DurationType =
      std::remove_const_t<decltype(detail::DESIRED_CHECK_INTERVAL)>;
  return duration_cast<DurationType>(
      steady_clock::now() -
      steady_clock::time_point{steady_clock::duration{startTimeoutWindow_}});
}

// Make sure to compile correctly.
template class CancellationHandle<true>;
template class CancellationHandle<false>;
}  // namespace ad_utility
