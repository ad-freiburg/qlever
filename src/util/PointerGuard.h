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
  std::weak_ptr<T> pointer_{};

 public:
  // Define constructors and delete copy with potential surprising behaviour.
  constexpr PointerGuard() noexcept = default;
  PointerGuard(PointerGuard&&) noexcept = default;
  PointerGuard(const PointerGuard&) noexcept = delete;
  PointerGuard& operator=(PointerGuard&&) noexcept = default;
  PointerGuard& operator=(const PointerGuard&) noexcept = delete;

  /// Set the internal weak pointer to the passed value.
  /// Usually this should be the constructor,
  /// but due to the nature of this class it should usually be declared before
  /// any potential shared pointer to guard, otherwise the destructor would
  /// block infinitely, resulting in a deadlock. So that's why 2-step
  /// initialization is necessary here.
  void set(std::weak_ptr<T> pointer) { pointer_ = std::move(pointer); }

  /// Destructor that blocks until the passed pointer no longer has any
  /// references to it.
  ~PointerGuard() {
    while (!pointer_.expired()) {
      // Sleep shortly between checks
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }
};
}  // namespace ad_utility

#endif  // QLEVER_POINTERGUARD_H
