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
#include "util/TypeTraits.h"

namespace ad_utility {
/// Enum to represent possible states of cancellation
enum class CancellationState { NOT_CANCELLED, MANUAL, TIMEOUT };

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
  /// this overload and use the overload that takes a callable that creates
  /// the exception message (see below).
  AD_ALWAYS_INLINE void throwIfCancelled(std::string_view detail) const {
    throwIfCancelled(std::identity{}, detail);
  }

  /// Throw an `CancellationException` when this handle has been cancelled. Do
  /// nothing otherwise. The arg types are passed to the `detailSupplier` only
  /// if an exception is about to be thrown. If no exception is thrown,
  /// `detailSupplier` will not be evaluated.
  template <typename... ArgTypes>
  AD_ALWAYS_INLINE void throwIfCancelled(
      const InvocableWithReturnType<std::string_view, ArgTypes...> auto&
          detailSupplier,
      ArgTypes&&... argTypes) const {
    auto state = cancellationState_.load(std::memory_order_relaxed);
    if (state == CancellationState::NOT_CANCELLED) [[likely]] {
      return;
    }
    throw CancellationException{
        state, std::invoke(detailSupplier, AD_FWD(argTypes)...)};
  }

  /// Return true if this cancellation handle has been cancelled, false
  /// otherwise. Note: Make sure to not use this value to set any other atomic
  /// value with relaxed memory ordering, as this may lead to out-of-thin-air
  /// values.
  AD_ALWAYS_INLINE bool isCancelled() const {
    return cancellationState_.load(std::memory_order_relaxed) !=
           CancellationState::NOT_CANCELLED;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_CANCELLATIONHANDLE_H
