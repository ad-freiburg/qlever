//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CANCELLATIONHANDLE_H
#define QLEVER_CANCELLATIONHANDLE_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <type_traits>

#include "util/CompilerExtensions.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/ParseableDuration.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

namespace ad_utility {
/// Enum to represent possible states of cancellation
enum class CancellationState {
  NOT_CANCELLED,
  WAITING_FOR_CHECK,
  CHECK_WINDOW_MISSED,
  MANUAL,
  TIMEOUT
};

namespace detail {

constexpr std::chrono::milliseconds DESIRED_CHECK_INTERVAL{50};

AD_ALWAYS_INLINE constexpr bool isCancelled(
    CancellationState cancellationState) {
  using enum CancellationState;
  static_assert(NOT_CANCELLED <= CHECK_WINDOW_MISSED);
  static_assert(WAITING_FOR_CHECK <= CHECK_WINDOW_MISSED);
  static_assert(MANUAL > CHECK_WINDOW_MISSED);
  static_assert(TIMEOUT > CHECK_WINDOW_MISSED);
  return cancellationState > CHECK_WINDOW_MISSED;
}

struct Empty {
  // Ignore potential assigment
  template <typename... Args>
  explicit Empty(const Args&...) {}
};
}  // namespace detail

/// An exception signalling an cancellation
class CancellationException : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
  CancellationException(CancellationState reason, std::string_view details)
      : std::runtime_error{absl::StrCat("Cancelled due to ",
                                        reason == CancellationState::TIMEOUT
                                            ? "timeout"
                                            : "manual cancellation",
                                        ". Stage: ", details)} {
    AD_CONTRACT_CHECK(detail::isCancelled(reason));
  }
};

// Ensure no locks are used
static_assert(std::atomic<CancellationState>::is_always_lock_free);

/// Thread safe wrapper around an atomic variable, providing efficient
/// checks for cancellation across threads.
template <bool WatchDogEnabled = areExpensiveChecksEnabled>
class CancellationHandle {
  static_assert(WatchDogEnabled == areExpensiveChecksEnabled);

  std::atomic<CancellationState> cancellationState_ =
      CancellationState::NOT_CANCELLED;

  template <typename T>
  using WatchDogOnly = std::conditional_t<WatchDogEnabled, T, detail::Empty>;
  [[no_unique_address]] WatchDogOnly<std::atomic_bool> watchDogRunning_ = false;
  [[no_unique_address]] WatchDogOnly<ad_utility::JThread> watchDogThread_;

  template <typename... ArgTypes>
  void pleaseWatchDog(CancellationState state,
                      const InvocableWithConvertibleReturnType<
                          std::string_view, ArgTypes...> auto& detailSupplier,
                      ArgTypes&&... argTypes) requires WatchDogEnabled {
    AD_CORRECTNESS_CHECK(watchDogRunning_.load(std::memory_order_relaxed));
    if (state == CancellationState::CHECK_WINDOW_MISSED) {
      LOG(WARN) << "Cancellation check missed deadline of "
                << ParseableDuration{detail::DESIRED_CHECK_INTERVAL}
                << ". Stage: "
                << std::invoke(detailSupplier, AD_FWD(argTypes)...)
                << std::endl;
    }

    cancellationState_.compare_exchange_strong(
        state, CancellationState::NOT_CANCELLED, std::memory_order_relaxed);
  }

  void startWatchDogInternal() requires WatchDogEnabled;

 public:
  /// Sets the cancellation flag so the next call to throwIfCancelled will throw
  void cancel(CancellationState reason);

  /// Overload for static exception messages, make sure the string is a constant
  /// expression, or computed in advance. If that's not the case do not use
  /// this overload and use the overload that takes a callable that creates
  /// the exception message (see below).
  AD_ALWAYS_INLINE void throwIfCancelled(std::string_view detail) {
    throwIfCancelled(std::identity{}, detail);
  }

  /// Throw an `CancellationException` when this handle has been cancelled. Do
  /// nothing otherwise. The arg types are passed to the `detailSupplier` only
  /// if an exception is about to be thrown. If no exception is thrown,
  /// `detailSupplier` will not be evaluated.
  template <typename... ArgTypes>
  AD_ALWAYS_INLINE void throwIfCancelled(
      const InvocableWithConvertibleReturnType<
          std::string_view, ArgTypes...> auto& detailSupplier,
      ArgTypes&&... argTypes) {
    auto state = cancellationState_.load(std::memory_order_relaxed);
    if (state == CancellationState::NOT_CANCELLED) [[likely]] {
      return;
    }
    if constexpr (WatchDogEnabled) {
      if (!detail::isCancelled(state)) {
        pleaseWatchDog(state, detailSupplier, AD_FWD(argTypes)...);
        return;
      }
    }
    throw CancellationException{
        state, std::invoke(detailSupplier, AD_FWD(argTypes)...)};
  }

  /// Return true if this cancellation handle has been cancelled, false
  /// otherwise. Note: Make sure to not use this value to set any other atomic
  /// value with relaxed memory ordering, as this may lead to out-of-thin-air
  /// values.
  AD_ALWAYS_INLINE bool isCancelled() const {
    return detail::isCancelled(
        cancellationState_.load(std::memory_order_relaxed));
  }

  void startWatchDog();

  ~CancellationHandle();
};
}  // namespace ad_utility

#endif  // QLEVER_CANCELLATIONHANDLE_H
