//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_CANCELLATIONHANDLE_H
#define QLEVER_CANCELLATIONHANDLE_H

#include <absl/strings/str_cat.h>
#include <gtest/gtest_prod.h>

#include <atomic>
#include <condition_variable>
#include <exception>
#include <mutex>

#include "backports/type_traits.h"
#include "global/Constants.h"
#include "util/CompilerExtensions.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/ParseableDuration.h"
#include "util/SourceLocation.h"
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

/// Helper enum that selects which features to compile inside the
/// `CancellationHandle` class.
enum class CancellationMode { ENABLED, NO_WATCH_DOG, DISABLED };

/// Turn the `QUERY_CANCELLATION_MODE` macro into a constexpr variable.
constexpr CancellationMode CANCELLATION_MODE = []() {
  using enum CancellationMode;
#ifndef QUERY_CANCELLATION_MODE
  return ENABLED;
#else
  return QUERY_CANCELLATION_MODE;
#endif
}();

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
  // Ignore potential assignment
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

/// Helper function to print a warning if `executionStage` is not empty.
inline std::string printAdditionalDetails(std::string_view executionStage) {
  if (executionStage.empty()) {
    return ".";
  }
  return absl::StrCat(" at stage \"", executionStage, "\".");
}

constexpr auto printNothing = []() constexpr { return ""; };
}  // namespace detail

/// An exception signalling an cancellation
class CancellationException : public std::exception {
  std::string message_;

 public:
  explicit CancellationException(std::string message)
      : message_{std::move(message)} {}
  explicit CancellationException(CancellationState reason)
      : message_{reason == CancellationState::TIMEOUT
                     ? "Operation timed out."
                     : "Operation was manually cancelled."} {
    AD_CONTRACT_CHECK(detail::isCancelled(reason));
  }

  const char* what() const noexcept override { return message_.c_str(); }

  /// Set optional operation information, if not already set.
  void setOperation(std::string_view operation) {
    constexpr std::string_view operationPrefix = " Last operation: ";
    // Don't set operation twice.
    if (message_.find(operationPrefix) == std::string::npos) {
      message_ += operationPrefix;
      message_ += operation;
    }
  }
};

/// Trim everything but the filename of a given file path.
constexpr std::string_view trimFileName(std::string_view fileName) {
  // Safe to do, because unsigned overflow is not UB and in case of
  // npos this will overflow back to zero.
  static_assert(std::string_view::npos + 1 == 0);
  return fileName.substr(fileName.rfind('/') + 1);
}

// Ensure no locks are used
static_assert(std::atomic<CancellationState>::is_always_lock_free);

/// Thread safe wrapper around an atomic variable, providing efficient
/// checks for cancellation across threads.
template <detail::CancellationMode Mode = detail::CANCELLATION_MODE>
class CancellationHandle {
  using steady_clock = std::chrono::steady_clock;
  static constexpr bool WatchDogEnabled =
      Mode == detail::CancellationMode::ENABLED;
  static constexpr bool CancellationEnabled =
      Mode != detail::CancellationMode::DISABLED;

  [[no_unique_address]] std::conditional_t<
      CancellationEnabled, std::atomic<CancellationState>, detail::Empty>
      cancellationState_{CancellationState::NOT_CANCELLED};

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
  /// NOTE: The parameter state is expected to be one of `CHECK_WINDOW_MISSED`
  /// or `WAITING_FOR_CHECK`, otherwise it will violate the correctness check.
  CPP_template(typename StateFunc)(
      requires WatchDogEnabled CPP_and
          ad_utility::InvocableWithConvertibleReturnType<
              StateFunc,
              std::string_view>) void pleaseWatchDog(CancellationState state,
                                                     ad_utility::source_location
                                                         location,
                                                     const StateFunc&
                                                         stageInvocable) {
    using DurationType =
        std::remove_const_t<decltype(DESIRED_CANCELLATION_CHECK_INTERVAL)>;
    AD_CORRECTNESS_CHECK(!detail::isCancelled(state) &&
                         state != CancellationState::NOT_CANCELLED);

    bool windowMissed = state == CancellationState::CHECK_WINDOW_MISSED;
    // Because we know `state` will be one of `CHECK_WINDOW_MISSED` or
    // `WAITING_FOR_CHECK` at this point, we can skip the initial check.
    do {
      if (cancellationState_.compare_exchange_weak(
              state, CancellationState::NOT_CANCELLED,
              std::memory_order_relaxed)) {
        if (windowMissed) {
          LOG(WARN) << "No timeout check has been performed for at least "
                    << ParseableDuration{duration_cast<DurationType>(
                                             steady_clock::now() -
                                             startTimeoutWindow_.load()) +
                                         DESIRED_CANCELLATION_CHECK_INTERVAL}
                    << ", should be at most "
                    << ParseableDuration{DESIRED_CANCELLATION_CHECK_INTERVAL}
                    << ". Checked at " << trimFileName(location.file_name())
                    << ":" << location.line()
                    << detail::printAdditionalDetails(
                           std::invoke(stageInvocable))
                    << std::endl;
        }
        break;
      }
      // If state is NOT_CANCELLED this means another thread already reported
      // the missed deadline, so we don't report a second time or a cancellation
      // kicked in and there is no need to continue the loop.
    } while (!detail::isCancelled(state) &&
             state != CancellationState::NOT_CANCELLED);
  }

  /// Internal function that starts the watch dog. It will set this
  /// `CancellationHandle` instance into a state that will log a warning in the
  /// console if `throwIfCancelled` is not called frequently enough.
  CPP_member auto startWatchDogInternal()
      -> CPP_ret(void)(requires WatchDogEnabled);

  /// Helper function that sets the internal state atomically given that it has
  /// not been cancelled yet. Otherwise no-op.
  /// CPP_member auto
  CPP_member auto setStatePreservingCancel(CancellationState newState)
      -> CPP_ret(void)(requires CancellationEnabled);

 public:
  /// Sets the cancellation flag so the next call to throwIfCancelled will
  /// throw. No-op if this instance is already in a cancelled state.
  void cancel(CancellationState reason);

  /// Throw an `CancellationException` when this handle has been cancelled. Do
  /// nothing otherwise. If `WatchDogEnabled` is true, this will log a warning
  /// if this check is not called frequently enough. It will contain the
  /// filename and line of the caller of this method.
  CPP_template(typename Func = decltype(detail::printNothing))(
      requires ad_utility::InvocableWithConvertibleReturnType<Func,
                                                              std::string_view>)
      AD_ALWAYS_INLINE void throwIfCancelled(
          [[maybe_unused]] ad_utility::source_location location =
              ad_utility::source_location::current(),
          const Func& stageInvocable = detail::printNothing) {
    if constexpr (CancellationEnabled) {
      auto state = cancellationState_.load(std::memory_order_relaxed);
      if (state == CancellationState::NOT_CANCELLED) [[likely]] {
        return;
      }
      if constexpr (WatchDogEnabled) {
        if (!detail::isCancelled(state)) {
          pleaseWatchDog(state, location, stageInvocable);
          return;
        }
      }
      throw CancellationException{state};
    }
  }

  /// Return true if this cancellation handle has been cancelled, false
  /// otherwise. Note: Make sure to not use this value to set any other atomic
  /// value with relaxed memory ordering, as this may lead to out-of-thin-air
  /// values. If the watchdog is enabled, this will please it and print
  /// a warning with the filename and line of the caller.
  AD_ALWAYS_INLINE bool isCancelled(
      [[maybe_unused]] ad_utility::source_location location =
          ad_utility::source_location::current()) {
    if constexpr (CancellationEnabled) {
      auto state = cancellationState_.load(std::memory_order_relaxed);
      bool isCancelled = detail::isCancelled(state);
      if constexpr (WatchDogEnabled) {
        if (!isCancelled && state != CancellationState::NOT_CANCELLED) {
          pleaseWatchDog(state, location, detail::printNothing);
        }
      }
      return isCancelled;
    } else {
      return false;
    }
  }

  /// Start the watch dog. Must only be called once per `CancellationHandle`
  /// instance. This allows the constructor to be used to cheaply
  /// default-initialize an instance of this class (as dummy non-null pointer
  /// for example).
  void startWatchDog();

  /// If this `CancellationHandle` is not cancelled, reset the internal
  /// `cancellationState_` to `CancellationState::NOT_CANCELED`.
  /// Useful to ignore expected gaps in the execution flow, but typically
  /// indicates that there's code that cannot be interrupted, so use with care!
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
  FRIEND_TEST(CancellationHandle, verifyPleaseWatchDogReportsOnlyWhenNecessary);
  FRIEND_TEST(CancellationHandle, verifyIsCancelledDoesPleaseWatchDog);
  FRIEND_TEST(CancellationHandle,
              verifyPleaseWatchDogDoesNotAcceptInvalidState);
  FRIEND_TEST(CancellationHandle, verifyWatchDogEndsEarlyIfCancelled);
};

using SharedCancellationHandle = std::shared_ptr<CancellationHandle<>>;
}  // namespace ad_utility

#endif  // QLEVER_CANCELLATIONHANDLE_H
