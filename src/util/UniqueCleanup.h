//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UNIQUECLEANUP_H
#define QLEVER_UNIQUECLEANUP_H

#include <concepts>
#include <functional>
#include <memory>

namespace ad_utility::unique_cleanup {

/// Wrapper class that allows to call a function
/// just before the wrapper value T is destroyed.
template <std::move_constructible T>
class UniqueCleanup {
  using type = std::remove_reference_t<T>;

  /// Boolean indicating if object was not moved out of
  bool active_ = true;
  /// Wrapped value
  type value_;
  /// Cleanup function. Only run once
  std::function<void(type&&)> function_;

 public:
  /// Accepts a value and a function that is called just before destruction.
  /// \param value Value to wrap.
  /// \param function The function to run just before destruction.
  ///                 Note: Make sure the function doesn't capture the this
  ///                 pointer as this may lead to segfaults because the pointer
  ///                 will point to the old object after moving.
  UniqueCleanup(type&& value, std::function<void(type&&)> function)
      : value_{std::move(value)}, function_{std::move(function)} {}

  type& operator*() noexcept { return value_; }
  const type& operator*() const noexcept { return value_; }

  type* operator->() noexcept { return &value_; }
  const type* operator->() const noexcept { return &value_; }

  UniqueCleanup(const UniqueCleanup&) noexcept = delete;
  UniqueCleanup& operator=(const UniqueCleanup&) noexcept = delete;

  UniqueCleanup(UniqueCleanup&& cleanupDeleter) noexcept
      : value_{std::move(cleanupDeleter.value_)},
        function_{std::move(cleanupDeleter.function_)} {
    // Make sure active is false after moving from another object
    cleanupDeleter.active_ = false;
  }

  UniqueCleanup& operator=(UniqueCleanup&& cleanupDeleter) noexcept {
    value_ = std::move(cleanupDeleter.value_);
    function_ = std::move(cleanupDeleter.function_);
    active_ = cleanupDeleter.active_;
    cleanupDeleter.active_ = false;
    return *this;
  }

  ~UniqueCleanup() {
    if (active_) {
      function_(std::move(value_));
    }
  }
};

}  // namespace ad_utility::unique_cleanup

#endif  // QLEVER_UNIQUECLEANUP_H
