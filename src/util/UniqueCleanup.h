//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UNIQUECLEANUP_H
#define QLEVER_UNIQUECLEANUP_H

#include <concepts>
#include <functional>
#include <memory>

#include "util/ResetWhenMoved.h"

namespace ad_utility::unique_cleanup {

/// Wrapper class that allows to call a function
/// just before the wrapper value T is destroyed.
template <std::move_constructible T>
class UniqueCleanup {
  /// Boolean indicating if object was not moved out of
  ResetWhenMoved<bool, false> active_ = true;
  /// Wrapped value
  T value_;
  /// Cleanup function. Only run once
  std::function<void(T&&)> function_;

 public:
  /// Accepts a value and a function that is called just before destruction.
  /// \param value Value to wrap.
  /// \param function The function to run just before destruction.
  ///                 Note: Make sure the function doesn't capture the this
  ///                 pointer as this may lead to segfaults because the pointer
  ///                 will point to the old object after moving.
  UniqueCleanup(T&& value, std::function<void(T&&)> function)
      : value_{std::move(value)}, function_{std::move(function)} {}

  T& operator*() noexcept { return value_; }
  const T& operator*() const noexcept { return value_; }

  T* operator->() noexcept { return &value_; }
  const T* operator->() const noexcept { return &value_; }

  UniqueCleanup(const UniqueCleanup&) noexcept = delete;
  UniqueCleanup& operator=(const UniqueCleanup&) noexcept = delete;

  UniqueCleanup(UniqueCleanup&& cleanupDeleter) noexcept = default;

  UniqueCleanup& operator=(UniqueCleanup&& cleanupDeleter) noexcept = default;

  ~UniqueCleanup() {
    if (active_) {
      std::invoke(function_, std::move(value_));
    }
  }
};

}  // namespace ad_utility::unique_cleanup

#endif  // QLEVER_UNIQUECLEANUP_H
