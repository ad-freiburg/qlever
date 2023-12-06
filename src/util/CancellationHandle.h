//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CANCELLATIONHANDLE_H
#define QLEVER_CANCELLATIONHANDLE_H

#include <absl/strings/str_cat.h>
#include <gtest/gtest_prod.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <type_traits>

#include "global/Constants.h"
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

#if !defined(QLEVER_DISABLE_QUERY_CANCELLATION_WATCH_DO) && LOGLEVEL >= WARN
constexpr bool ENABLE_QUERY_CANCELLATION_WATCH_DOG = true;
#else
#if LOGLEVEL < WARN
#warning \
    "QLEVER_ENABLE_QUERY_CANCELLATION_WATCH_DOG is ignored if WARN logging \
level is disabled"
#endif
constexpr bool ENABLE_QUERY_CANCELLATION_WATCH_DOG = false;
#endif

/// Helper function that safely checks if the passed `cancellationState`
/// represents one of the cancelled states with a single comparison for
/// efficiency.
AD_ALWAYS_INLINE constexpr bool isCancelled(
    CancellationState cancellationState) {
  using enum CancellationState;
  static_assert(NOT_CANCELLED <= CHECK_WINDOW_MISSED);
  static_assert(WAITING_FOR_CHECK <= CHECK_WINDOW_MISSED);
  static_assert(MANUAL > CHECK_WINDOW_MISSED);
  static_assert(TIMEOUT > CHECK_WINDOW_MISSED);
  return cancellationState > CHECK_WINDOW_MISSED;
}

/// Helper struct to allow to conditionally compile fields into a class.
struct Empty {
  // Ignore potential assigment
  template <typename... Args>
  explicit Empty(const Args&...) {}
};

/// Helper struct that imitates functionality similar to `std::stop_token`,
/// until it has broader compiler support.
struct PseudoStopToken {
  std::condition_variable conditionVariable_;
  std::mutex mutex_;
  bool running_;
  explicit PseudoStopToken(bool running) : running_{running} {}
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
template <bool WatchDogEnabled = detail::ENABLE_QUERY_CANCELLATION_WATCH_DOG>
class CancellationHandle {
  using steady_clock = std::chrono::steady_clock;
  std::atomic<CancellationState> cancellationState_ =
      CancellationState::NOT_CANCELLED;

  template <typename T>
  using WatchDogOnly = std::conditional_t<WatchDogEnabled, T, detail::Empty>;
  // TODO<Clang18> Use std::jthread and its builtin stop_token.
  [[no_unique_address]] WatchDogOnly<detail::PseudoStopToken> watchDogState_{
      false};
  [[no_unique_address]] WatchDogOnly<ad_utility::JThread> watchDogThread_;
  [[no_unique_address]] WatchDogOnly<std::atomic<steady_clock::time_point>>
      startTimeoutWindow_{steady_clock::now()};
  static_assert(std::atomic<steady_clock::time_point>::is_always_lock_free);

  /// Make sure internal state is set back to
  /// `CancellationState::NOT_CANCELLED`, in order to prevent logging warnings
  /// in the console that would otherwise be triggered by the watchdog.
  template <typename... ArgTypes>
  void pleaseWatchDog(CancellationState state,
                      const InvocableWithConvertibleReturnType<
                          std::string_view, ArgTypes...> auto& detailSupplier,
                      ArgTypes&&... argTypes) requires WatchDogEnabled {
    using DurationType =
        std::remove_const_t<decltype(DESIRED_CANCELLATION_CHECK_INTERVAL)>;

    if (state == CancellationState::CHECK_WINDOW_MISSED) {
      LOG(WARN) << "Cancellation check missed deadline of "
                << ParseableDuration{DESIRED_CANCELLATION_CHECK_INTERVAL}
                << " by "
                << ParseableDuration{duration_cast<DurationType>(
                       steady_clock::now() - startTimeoutWindow_.load())}
                << ". Stage: "
                << std::invoke(detailSupplier, AD_FWD(argTypes)...)
                << std::endl;
    }

    cancellationState_.compare_exchange_strong(
        state, CancellationState::NOT_CANCELLED, std::memory_order_relaxed);
  }

  /// Internal function that starts the watch dog. It will set this
  /// `CancellationHandle` instance into a state that will log a warning in the
  /// console if `throwIfCancelled` is not called frequently enough.
  void startWatchDogInternal() requires WatchDogEnabled;

  /// Helper function that sets the internal state atomically given that it has
  /// not been cancelled yet. Otherwise no-op.
  void setStatePreservingCancel(CancellationState newState);

 public:
  /// Sets the cancellation flag so the next call to throwIfCancelled will
  /// throw. No-op if this instance is already in a cancelled state.
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
  /// `detailSupplier` will not be evaluated. If `WatchDogEnabled` is true,
  /// this will log a warning if this check is not called frequently enough.
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

  /// Start the watch dog. Must only be called once per `CancellationHandle`
  /// instance. This allows the constructor to be used to cheaply
  /// default-initialize an instance of this class (as dummy non-null pointer
  /// for example).
  void startWatchDog();

  /// If this `CancellationHandle` is not cancelled, reset the internal
  /// `cancellationState_` to `CancellationState::NOT_CANCELED`.
  /// Useful to ignore expected gaps in the execution flow.
  void resetWatchDogState();

  // Explicit move-semantics
  CancellationHandle() = default;
  CancellationHandle(const CancellationHandle&) = delete;
  CancellationHandle& operator=(const CancellationHandle&) = delete;
  ~CancellationHandle();

  FRIEND_TEST(CancellationHandle, verifyWatchDogDoesChangeState);
  FRIEND_TEST(CancellationHandle,
              verifyResetWatchDogStateDoesProperlyResetState);
  FRIEND_TEST(CancellationHandle,
              verifyResetWatchDogStateIsNoOpWithoutWatchDog);
  FRIEND_TEST(CancellationHandle, verifyCheckDoesPleaseWatchDog);
  FRIEND_TEST(CancellationHandle, verifyCheckDoesNotOverrideCancelledState);
  FRIEND_TEST(CancellationHandle,
              verifyCheckAfterDeadlineMissDoesReportProperly);
  FRIEND_TEST(CancellationHandle, verifyWatchDogDoesNotChangeStateAfterCancel);
};

using SharedCancellationHandle = std::shared_ptr<CancellationHandle<>>;
}  // namespace ad_utility

#endif  // QLEVER_CANCELLATIONHANDLE_H
