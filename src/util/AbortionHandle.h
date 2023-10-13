//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_ABORTIONHANDLE_H
#define QLEVER_ABORTIONHANDLE_H

#include <absl/strings/str_cat.h>

#include <atomic>
#include <type_traits>

#include "util/Exception.h"

// Inlining is super important for performance here, and clang considers
// throwIfAborted for too costly to inline, so we override this behaviour
// to shave of some nanoseconds
#ifdef __CLANG__
#define ALWAYS_INLINE [[clang::always_inline]]
#else
#ifdef __GNUC__
#define ALWAYS_INLINE [[gnu::always_inline]]
#else
#warning Unknown compiler, inlining might not be guaranteed
#define ALWAYS_INLINE
#endif
#endif

namespace ad_utility {
/// Enum to represent possible states of abortion
enum class AbortionState { NOT_ABORTED, CANCELLED, TIMEOUT };

/// An exception signalling an abortion
class AbortionException : public std::runtime_error {
 public:
  explicit AbortionException(const std::string& message)
      : std::runtime_error{message} {}
  AbortionException(AbortionState reason, std::string_view details)
      : std::runtime_error{absl::StrCat(
            "Aborted due to ",
            reason == AbortionState::TIMEOUT ? "timeout" : "cancellation",
            ". Stage: ", details)} {
    AD_CONTRACT_CHECK(reason != AbortionState::NOT_ABORTED);
  }
};

// Ensure no locks are used
static_assert(std::atomic<AbortionState>::is_always_lock_free);

/// Helper identity function
constexpr auto identity(std::string_view value) { return value; }

/// Thread safe wrapper around an atomic variable, providing efficient
/// checks for abortion across threads.
class AbortionHandle {
  std::atomic<AbortionState> abortionState_ = AbortionState::NOT_ABORTED;

 public:
  /// Sets the abortion flag so the next call to throwIfAborted will throw
  void abort(AbortionState reason);

  /// Overload for static exception messages, make sure the string is a constant
  /// expression, or computed in advance. If that's not the case do not use
  /// this overload and instead use the "original" variant.
  ALWAYS_INLINE void throwIfAborted(std::string_view detail) const {
    throwIfAborted(&identity, detail);
  }

  /// Throw an `AbortionException` when this handle has been aborted. Do
  /// nothing otherwise
  template <typename... ArgTypes>
  ALWAYS_INLINE void throwIfAborted(
      const std::invocable<ArgTypes...> auto& detailSupplier,
      ArgTypes&&... argTypes) const {
    auto state = abortionState_.load(std::memory_order_relaxed);
    if (state == AbortionState::NOT_ABORTED) [[likely]] {
      return;
    }
    throw AbortionException{
        state,
        std::invoke(detailSupplier, std::forward<ArgTypes>(argTypes)...)};
  }
};
}  // namespace ad_utility

// Make sure this macro doesn't leak from here
#undef ALWAYS_INLINE

#endif  // QLEVER_ABORTIONHANDLE_H
