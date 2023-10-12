//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ABORTIONHANDLE_H
#define QLEVER_ABORTIONHANDLE_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <type_traits>

namespace ad_utility {

/// An exception signalling a timeout
class TimeoutException : public std::exception {
 public:
  TimeoutException(std::string message) : message_{std::move(message)} {}
  const char* what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

/// Enum to represent possible states of abortion
enum class AbortionState { NOT_ABORTED, CANCELLED, TIMEOUT };

static_assert(std::atomic<AbortionState>::is_always_lock_free);

/// Thread safe wrapper around an atomic variable, providing efficient
/// checks for abortion across threads.
class AbortionHandle {
  std::atomic<AbortionState> abortionState_ = AbortionState::NOT_ABORTED;

 public:
  /// Sets the abortion flag so the next call to throwIfAborted will throw
  void abort(AbortionState reason);

  void throwIfAborted(std::string_view detail) const;

  template <typename Func, typename... ArgTypes>
  void throwIfAborted(const Func& detailSupplier,
                      ArgTypes&&... argTypes) const {
    auto state = abortionState_.load(std::memory_order_relaxed);
    switch (state) {
      [[likely]] case AbortionState::NOT_ABORTED:
        return;
      case AbortionState::CANCELLED:
        throw std::runtime_error{absl::StrCat(
            "Cancelled",
            std::invoke(detailSupplier, std::forward<ArgTypes>(argTypes)...))};
      case AbortionState::TIMEOUT:
        throw TimeoutException{absl::StrCat(
            "Timeout",
            std::invoke(detailSupplier, std::forward<ArgTypes>(argTypes)...))};
    }
  }
};
}  // namespace ad_utility

#endif  // QLEVER_ABORTIONHANDLE_H
