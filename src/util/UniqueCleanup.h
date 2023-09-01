//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UNIQUECLEANUP_H
#define QLEVER_UNIQUECLEANUP_H

#include <concepts>
#include <functional>
#include <memory>

namespace ad_utility::unique_cleanup {

/// Special type of deleter class that allows to call a function
/// just before the wrapper value T is destroyed.
template <std::move_constructible T>
class UniqueCleanup {
  using type = std::remove_reference_t<T>;

  bool active_ = true;
  type value_;
  std::function<void(type&&)> function_;

 public:
  UniqueCleanup(type&& value, std::function<void(type&&)> function)
      : value_{std::forward<type>(value)}, function_{std::move(function)} {}

  type& operator*() noexcept { return value_; }
  const type& operator*() const noexcept { return value_; }

  type* operator->() noexcept { return &value_; }
  const type* operator->() const noexcept { return &value_; }

  UniqueCleanup(const UniqueCleanup&) noexcept = delete;
  UniqueCleanup& operator=(const UniqueCleanup&) noexcept = delete;

  UniqueCleanup(UniqueCleanup&& cleanupDeleter) noexcept
      : value_{std::move(cleanupDeleter.value_)},
        function_{std::move(cleanupDeleter.function_)} {
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
