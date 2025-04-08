//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_RESETWHENMOVED_H
#define QLEVER_SRC_UTIL_RESETWHENMOVED_H

#include <utility>

namespace ad_utility {
// This class stores a single value of type `T` ( and is implicitly convertible
// to and from `T`. Each time an object of type `ResetWhenMoved<T>` is moved
// into another `ResetWhenMoved<T>` or into a plain `T`, its value changes back
// to the `DefaultValue`. This class can be used as a simplification to
// implement data structures that store raw integers or pointers that have to be
// reset when the data structure is moved. When using `ResetWhenMoved` for such
// members, then often the defaulted move constructors and move assignment
// operators will have the correct semantics. For example usages see
// `MmapVector.h` and `IdTable.h`.
template <typename T, T DefaultValue>
struct ResetWhenMoved {
  T value_ = DefaultValue;
  ResetWhenMoved() = default;

  // Implicit conversion to and from `T`.
  ResetWhenMoved(T t) : value_{std::move(t)} {}
  operator T() const& { return value_; }
  operator T&() & { return value_; }
  // When moving into a `T`, the `value_` is reset to the `DefaultValue`.
  operator T() && { return std::exchange(value_, DefaultValue); }

  // The copy constructor and copy assignment both just copy the value.
  ResetWhenMoved(const ResetWhenMoved&) = default;
  ResetWhenMoved& operator=(const ResetWhenMoved&) = default;

  constexpr static bool isNoexcept = std::is_nothrow_move_constructible_v<T> &&
                                     std::is_nothrow_move_assignable_v<T>;

  // The move constructor and move assignment operators reset the `value_`
  ResetWhenMoved(ResetWhenMoved&& other) noexcept(isNoexcept)
      : value_{std::exchange(other.value_, DefaultValue)} {}
  ResetWhenMoved& operator=(ResetWhenMoved&& other) noexcept(isNoexcept) {
    value_ = std::exchange(other.value_, DefaultValue);
    return *this;
  }
};
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_RESETWHENMOVED_H
