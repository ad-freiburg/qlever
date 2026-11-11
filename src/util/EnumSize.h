// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ENUMSIZE_H
#define QLEVER_SRC_UTIL_ENUMSIZE_H

#include <array>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "backports/keywords.h"
#include "backports/reflection.h"

// The number of enumerators of a "regular" enum, that is, an enum the values of
// which are `0, 1, ..., N - 1` in declaration order. The size is stated once
// next to the enum via one of the following macros, and then is available as
// `qlever::enumSize<E>`. All the values of the enum are available as
// `qlever::allEnumValues<E>`. Example:
//
//   namespace ad_utility {
//   enum class Color { Red, Green, Blue };
//   QLEVER_ENUM_SIZE(Color, 3);
//   struct S {
//     enum struct Kind { A, B };
//     QLEVER_NESTED_ENUM_SIZE(Kind, 2);
//   };
//   }  // namespace ad_utility
//   static_assert(qlever::enumSize<ad_utility::Color> == 3);
//
// `QLEVER_ENUM_SIZE` has to be used in the namespace of the enum,
// `QLEVER_NESTED_ENUM_SIZE` in the class of an enum that is a class member. If
// reflection is supported (see `backports/reflection.h`), both macros check at
// compile time that the enum is regular and has exactly `N` enumerators, such
// that the stated size can never get out of sync with the enum.
//
// Implementation note: The macros define a function `qleverEnumSize` that is
// found via argument-dependent lookup (for a nested enum it is a hidden friend
// of the enclosing class), so unlike an explicit specialization of a variable
// template they can be used inside any namespace and inside classes.

namespace qlever {

namespace enumSizeDetail {
// The tag that marks the `qleverEnumSize` functions defined by the macros, s.t.
// they can't accidentally be confused with other functions.
struct EnumSizeTag {};

#ifdef QLEVER_HAS_REFLECTION
// Return true iff the enumerators of `E` (in declaration order) have the values
// `0, 1, ..., N - 1`.
template <typename E, std::size_t N>
consteval bool isRegularEnumWithSize() {
  auto enumerators = std::meta::enumerators_of(^^E);
  if (enumerators.size() != N) {
    return false;
  }
  std::size_t i = 0;
  for (auto enumerator : enumerators) {
    auto value = static_cast<std::underlying_type_t<E>>(
        std::meta::extract<E>(std::meta::constant_of(enumerator)));
    // Note: `static_cast<std::size_t>(value) != i` would also always work (a
    // negative `value` wraps around to a huge number that never equals `i`),
    // but `std::cmp_not_equal` is cleaner, as it involves no conversions for
    // signed underlying types.
    if (std::cmp_not_equal(value, i)) {
      return false;
    }
    ++i;
  }
  return true;
}
#endif
}  // namespace enumSizeDetail

// The number of enumerators of the enum `E`, see above for details.
template <typename E>
inline constexpr std::size_t enumSize =
    qleverEnumSize(enumSizeDetail::EnumSizeTag{}, E{});

// All the values of the enum `E` in increasing order, see above for details.
template <typename E>
inline constexpr std::array<E, enumSize<E>> allEnumValues = []() {
  std::array<E, enumSize<E>> result{};
  for (std::size_t i = 0; i < result.size(); ++i) {
    result[i] = static_cast<E>(i);
  }
  return result;
}();

}  // namespace qlever

#ifdef QLEVER_HAS_REFLECTION
#define QL_IMPL_CHECK_ENUM_SIZE(E, N)                                    \
  static_assert(::qlever::enumSizeDetail::isRegularEnumWithSize<E, N>(), \
                "The enum `" #E "` must have exactly " #N                \
                " enumerators with the values 0, 1, ..., " #N            \
                " - 1 in declaration order, see `QLEVER_ENUM_SIZE` in "  \
                "`util/EnumSize.h`")
#else
#define QL_IMPL_CHECK_ENUM_SIZE(E, N) \
  static_assert(std::is_enum_v<E>, "`" #E "` must be an enum")
#endif

#define QL_IMPL_ENUM_SIZE(FRIEND_SPEC, E, N)                            \
  [[maybe_unused]] FRIEND_SPEC QL_CONSTEVAL std::size_t qleverEnumSize( \
      ::qlever::enumSizeDetail::EnumSizeTag, E) {                       \
    return N;                                                           \
  }                                                                     \
  QL_IMPL_CHECK_ENUM_SIZE(E, N)

// State that the enum `E` (which has to be declared in the current namespace)
// has the `N` values `0, 1, ..., N - 1`. This makes `qlever::enumSize<E>`
// (which is `N`) and `qlever::allEnumValues<E>` (an array of all the `N`
// values) available. See above for details.
#define QLEVER_ENUM_SIZE(E, N) QL_IMPL_ENUM_SIZE(, E, N)

// The same as `QLEVER_ENUM_SIZE`, but for an enum `E` that is declared inside
// the current class.
#define QLEVER_NESTED_ENUM_SIZE(E, N) QL_IMPL_ENUM_SIZE(friend, E, N)

#endif  // QLEVER_SRC_UTIL_ENUMSIZE_H
