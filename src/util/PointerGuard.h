//   Copyright 2024, University of Freiburg,
//   Chair of Algorithms and Data Structures.
//   Author: Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>

#ifndef QLEVER_POINTERGUARD_H
#define QLEVER_POINTERGUARD_H

#include <memory>
#include <thread>

#include "util/Exception.h"

namespace ad_utility {

/// Helper class that allows to block destruction until a shared pointer has no
/// references left.
template <typename T>
class PointerGuard {
  std::shared_ptr<T> pointer_{};

 public:
  // Define constructors and delete copy with potential surprising behaviour.
  explicit PointerGuard(std::shared_ptr<T> pointer)
      : pointer_{std::move(pointer)} {
    // Make sure pointer does actually point to something
    AD_CONTRACT_CHECK(pointer_);
  }
  PointerGuard(PointerGuard&&) noexcept = default;
  PointerGuard(const PointerGuard&) noexcept = delete;
  PointerGuard& operator=(PointerGuard&&) noexcept = default;
  PointerGuard& operator=(const PointerGuard&) noexcept = delete;

  /// Get a weak pointer representation of the underlying shared pointer
  std::weak_ptr<T> getWeak() noexcept { return pointer_; }

  /// Get a reference to the underlying object
  T& get() noexcept { return *pointer_; }

  /// Destructor that blocks until the passed pointer no longer has any
  /// references to it.
  ~PointerGuard() {
    std::weak_ptr weakPtr{pointer_};
    pointer_.reset();
    while (!weakPtr.expired()) {
      // Sleep shortly between checks
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
};
}  // namespace ad_utility

#endif  // QLEVER_POINTERGUARD_H
