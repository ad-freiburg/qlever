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

// Inlining is super important for performance here, and clang considers
// throwIfCancelled for too costly to inline, so we override this behaviour
// to shave of some nanoseconds

namespace ad_utility {
/// Enum to represent possible states of cancellation
enum class CancellationState { NOT_CANCELLED, MANUAL, TIMEOUT };

/// An exception signalling an cancellation
class CancellationException : public std::runtime_error {
 public:
  explicit CancellationException(const std::string& message)
      : std::runtime_error{message} {}
  CancellationException(CancellationState reason, std::string_view details)
      : std::runtime_error{absl::StrCat("Cancelled due to ",
                                        reason == CancellationState::TIMEOUT
                                            ? "timeout"
                                            : "manual cancellation",
                                        ". Stage: ", details)} {
    AD_CONTRACT_CHECK(reason != CancellationState::NOT_CANCELLED);
  }
};

// Ensure no locks are used
static_assert(std::atomic<CancellationState>::is_always_lock_free);

/// Thread safe wrapper around an atomic variable, providing efficient
/// checks for cancellation across threads.
class CancellationHandle {
  std::atomic<CancellationState> cancellationState_ =
      CancellationState::NOT_CANCELLED;

 public:
  /// Sets the cancellation flag so the next call to throwIfCancelled will throw
  void cancel(CancellationState reason);

  /// Overload for static exception messages, make sure the string is a constant
  /// expression, or computed in advance. If that's not the case do not use
  /// this overload and instead use the "original" variant.
  AD_ALWAYS_INLINE void throwIfCancelled(std::string_view detail) const {
    throwIfCancelled(std::identity{}, detail);
  }

  /// Throw an `CancellationException` when this handle has been cancelled. Do
  /// nothing otherwise
  template <typename... ArgTypes>
  AD_ALWAYS_INLINE void throwIfCancelled(const auto& detailSupplier,
                                         ArgTypes&&... argTypes) const {
    auto state = cancellationState_.load(std::memory_order_relaxed);
    if (state == CancellationState::NOT_CANCELLED) [[likely]] {
      return;
    }
    throw CancellationException{
        state, std::invoke(detailSupplier, AD_FWD(argTypes)...)};
  }
};
}  // namespace ad_utility

#endif  // QLEVER_CANCELLATIONHANDLE_H
