//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once
#include <utility>

namespace ad_utility {
template <typename T, T DefaultValue>
struct ResetWhenMoved {
  T value_ = DefaultValue;
  ResetWhenMoved() = default;
  operator T() const { return value_; }
  operator T&() { return value_; }
  ResetWhenMoved(T t) : value_{std::move(t)} {}

  ResetWhenMoved(const ResetWhenMoved&) = default;
  ResetWhenMoved& operator=(const ResetWhenMoved&) = default;

  constexpr static bool isNoexcept = std::is_nothrow_move_constructible_v<T> &&
                                     std::is_nothrow_move_assignable_v<T>;

  ResetWhenMoved(ResetWhenMoved&& other) noexcept(isNoexcept)
      : value_{std::exchange(other.value_, DefaultValue)} {}
  ResetWhenMoved& operator=(ResetWhenMoved&& other) noexcept(isNoexcept) {
    value_ = std::exchange(other.value_, DefaultValue);
    return *this;
  }
};
}  // namespace ad_utility
