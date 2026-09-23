//   Copyright 2023, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_UNIQUECLEANUP_H
#define QLEVER_UNIQUECLEANUP_H

#include "backports/concepts.h"
#include "backports/functional.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/ResetWhenMoved.h"

namespace ad_utility::unique_cleanup {

// Wrapper class that allows to call a function just before the wrapped value
// T is destroyed or overwritten by a move assignment.
CPP_template(typename T, typename Func = std::function<void(T&&)>)(
    requires ql::concepts::move_constructible<T>) class UniqueCleanup {
  // False once the cleanup has run or was cancelled, or if this object was
  // moved from.
  ResetWhenMoved<bool, false> active_ = true;
  // Wrapped value.
  T value_;
  // Cleanup function. Only run once.
  Func function_;

 public:
  // Wrap the `value` and the `function` to run on it as its cleanup. NOTE:
  // The `function` must not capture the `this` pointer of the object owning
  // this `UniqueCleanup`, because after a move that pointer would point to the
  // moved-from object.
  UniqueCleanup(T value, Func function)
      : value_{std::move(value)}, function_{std::move(function)} {}

  T& operator*() noexcept { return value_; }
  const T& operator*() const noexcept { return value_; }

  T* operator->() noexcept { return &value_; }
  const T* operator->() const noexcept { return &value_; }

  UniqueCleanup(const UniqueCleanup&) noexcept = delete;
  UniqueCleanup& operator=(const UniqueCleanup&) noexcept = delete;

  UniqueCleanup(UniqueCleanup&& cleanupDeleter) noexcept = default;

  // Runs the cleanup of the overwritten value (if active) before taking over
  // the value of `other`.
  UniqueCleanup& operator=(UniqueCleanup&& other) noexcept {
    if (this != &other) {
      runCleanup();
      active_ = std::move(other.active_);
      value_ = std::move(other.value_);
      function_ = std::move(other.function_);
    }
    return *this;
  }

  // Return true if the cleanup has neither run nor been cancelled, and this
  // object has not been moved from.
  bool isActive() const noexcept { return active_; }

  // Disable the cleanup call without executing it.
  void cancel() && { active_ = false; }

  // Run the cleanup right away and disable it. Unlike in the destructor, an
  // exception thrown by the cleanup is propagated. Returns the result of the
  // cleanup. The object must be active.
  decltype(auto) runNow() && {
    AD_CONTRACT_CHECK(active_);
    active_ = false;
    return std::invoke(std::move(function_), std::move(value_));
  }

  ~UniqueCleanup() {
    ad_utility::terminateIfThrows([this]() { runCleanup(); },
                                  "The cleanup of a `UniqueCleanup` failed");
  }

 private:
  void runCleanup() {
    if (std::exchange(active_.value_, false)) {
      std::invoke(std::move(function_), std::move(value_));
    }
  }
};

}  // namespace ad_utility::unique_cleanup

#endif  // QLEVER_UNIQUECLEANUP_H
