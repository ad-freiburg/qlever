// Copyright 2023 - 2026 The QLever Authors, in particular:
//
// 2023 - 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "util/CancellationHandle.h"

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
CPP_member_def auto CancellationHandle<Mode>::advanceWatchDogState()
    -> CPP_ret(bool)(requires WatchDogEnabled) {
  using enum CancellationState;
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
  }
  // No need to keep the watchdog running if the handle was cancelled already.
  return !detail::isCancelled(state);
}

// _____________________________________________________________________________
template <CancellationMode Mode>
void CancellationHandle<Mode>::runWatchDogStep(
    [[maybe_unused]] std::weak_ptr<CancellationHandle> handle,
    [[maybe_unused]] std::shared_ptr<RunAfter> runAfter) {
  if constexpr (WatchDogEnabled) {
    {
      // Don't keep the handle alive until the next step.
      auto lockedHandle = handle.lock();
      if (!lockedHandle || !lockedHandle->advanceWatchDogState()) {
        return;
      }
    }
    (*runAfter)(DESIRED_CANCELLATION_CHECK_INTERVAL,
                [handle = std::move(handle), runAfter]() mutable {
                  runWatchDogStep(std::move(handle), std::move(runAfter));
                });
  }
}

// _____________________________________________________________________________
template <CancellationMode Mode>
void CancellationHandle<Mode>::registerWatchDog(
    [[maybe_unused]] std::weak_ptr<CancellationHandle> handle,
    [[maybe_unused]] RunAfter runAfter) {
  if constexpr (WatchDogEnabled) {
    {
      auto lockedHandle = handle.lock();
      AD_CONTRACT_CHECK(lockedHandle);
      bool wasRegistered = lockedHandle->watchDogRegistered_.exchange(true);
      AD_CONTRACT_CHECK(!wasRegistered,
                        "The watch dog of a `CancellationHandle` must only be "
                        "registered once.");
    }
    // The `RunAfter` is shared because each step calls it while the next step
    // already has to capture it.
    runWatchDogStep(std::move(handle),
                    std::make_shared<RunAfter>(std::move(runAfter)));
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

// Make sure to compile correctly.
template class CancellationHandle<CancellationMode::ENABLED>;
template class CancellationHandle<CancellationMode::NO_WATCH_DOG>;
template class CancellationHandle<CancellationMode::DISABLED>;
}  // namespace ad_utility
