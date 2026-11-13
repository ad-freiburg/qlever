// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
// Author: Krzysztof Tyburski <Krzysztof.Tyburski@partner.bmw.de>

/*
 * Three-Way comparison backport for C++17/C++20 compatibility
 *
 * This header provides macros for implementing comparison operators that work
 * consistently across C++17 and C++20. In C++20, it uses the spaceship operator
 * (<=>), while in C++17 it provides equivalent functionality using traditional
 * comparison operators.
 *
 * Available Macros:
 *
 * DEFAULTED COMPARISON MACROS (using std::tie for member comparison):
 * - QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR(Class, members...): Friend operators
 * for all comparisons
 * - QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_CONSTEXPR(Class, members...):
 * Constexpr version
 * - QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(Class, members...): Member
 * operators
 * - QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(Class, members...):
 * Constexpr member operators
 *
 * EQUALITY-ONLY MACROS (for classes that only need == and !=):
 * - QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(Class, members...): Friend equality
 * operators
 * - QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_CONSTEXPR(Class, members...):
 * Constexpr version
 * - QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Class, members...): Member
 * equality operators
 * - QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR(Class, members...):
 * Constexpr member version
 *
 * MACROS FOR DERIVED CLASSES:
 * The macros above must only be used in classes without base classes, because
 * the C++20 defaulted operators always compare the base class subobjects first,
 * while the C++17 implementations only compare the listed members. For a
 * derived class use the following variants (which only exist as member
 * operators) instead. They take the (single, public, non-virtual) base class as
 * their second argument and consistently compare the base class first, then the
 * listed members. The base class has to define the corresponding operators
 * itself, which it typically does by using one of the macros above. If the name
 * of the base class contains a comma (e.g. `Base<A, B>`), then a type alias for
 * it has to be used, else the preprocessor will not parse the macro arguments
 * correctly.
 * - QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_DERIVED(Class, Base,
 * members...)
 * - QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR_DERIVED(Class, Base,
 * members...)
 * - QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_DERIVED(Class, Base,
 * members...)
 * - QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_DERIVED(Class, Base,
 * members...)
 *
 * CUSTOM COMPARISON MACROS (for implementing custom comparison logic):
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR(Class): Friend operator
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR(Class):
 * Constexpr version
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(Class): Member operator
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(Class):
 * Constexpr member version
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_QL_CONSTEXPR(Class): Member
 * operators that are constexpr in C++20 mode only, mirroring `QL_CONSTEXPR`
 * (see `backports/keywords.h`). Use this if the `compareThreeWay` that the
 * operators call is itself declared `QL_CONSTEXPR`, which is needed when it
 * cannot be constexpr in C++17 (for example because GCC 8 rejects an `if`
 * statement in a relaxed-constexpr function).
 *
 * Note: For a class without any data members, the `members...` argument of the
 * defaulted macros is empty, and the macro has to be invoked with a trailing
 * comma, for example `QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(Empty, )`.
 * Reason: before C++20, a variadic macro requires at least one argument for its
 * `...` parameter (GCC and Clang accept zero arguments as an extension, but
 * warn about it with `-Wpedantic` and reject it with `-pedantic-errors`). The
 * trailing comma supplies a single empty argument and thus makes the invocation
 * valid C++17. It has no effect on the generated code in either standard mode.
 *
 * Note: The DEFAULTED macros (including the `..._DERIVED` variants) have to
 * list ALL the data members of the class in declaration order, and none of them
 * may be a C array, because in C++20 mode the operators are simply defaulted,
 * which compares all the members, while in C++17 mode only the listed members
 * are compared (via `std::tie`). The same holds for the base class: the
 * non-derived macros may only be used for classes without base classes, the
 * `..._DERIVED` variants only for classes with exactly the one given base
 * class. With C++26 reflection (see `backports/reflection.h`), these conditions
 * are checked at compile time (see `QL_IMPL_DEFAULTED_COMPARISON` below).
 *
 * Note: Custom three-way comparison macros assume the presence of a
 * compareThreeWay function (either as a member or free function) that returns
 * a comparison ordering (e.g., ql::strong_ordering, ql::weak_ordering, or
 * ql::partial_ordering).
 *
 * TEMPLATE VARIANTS (for template classes):
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_TEMPLATE(template_spec, Class)
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR_TEMPLATE(template_spec, Class)
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_TEMPLATE(template_spec, Class)
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_TEMPLATE(template_spec,
 * Class)
 *
 * COMPARISON FUNCTION:
 * - ql::compareThreeWay(lhs, rhs): Generic three-way comparison function
 *
 * Examples and usage patterns can be found in:
 * test/backports/ThreeWayComparisonTest.cpp
 */

#ifndef QLEVER_SRC_BACKPORTS_THREE_WAY_COMPARISON_H
#define QLEVER_SRC_BACKPORTS_THREE_WAY_COMPARISON_H

#include <absl/types/compare.h>

#include <cmath>
#include <tuple>
#include <type_traits>

#include "backports/reflection.h"

#ifdef QLEVER_HAS_REFLECTION
#include <algorithm>
#include <array>
#include <string>
#include <string_view>
#endif

namespace ql {
using absl::partial_ordering;
using absl::strong_ordering;
using absl::weak_ordering;
}  // namespace ql

#ifdef QLEVER_CPP_17

namespace ql {

template <typename LT, typename RT, typename = void>
struct HasLess : std::false_type {};
template <typename LT, typename RT>
struct HasLess<LT, RT,
               std::void_t<decltype(std::declval<LT>() < std::declval<RT>())>>
    : std::true_type {};

template <typename LT, typename RT, typename = void>
struct HasGreater : std::false_type {};
template <typename LT, typename RT>
struct HasGreater<
    LT, RT, std::void_t<decltype(std::declval<LT>() > std::declval<RT>())>>
    : std::true_type {};

template <typename LT, typename RT, typename = void>
struct HasLessEqual : std::false_type {};
template <typename LT, typename RT>
struct HasLessEqual<
    LT, RT, std::void_t<decltype(std::declval<LT>() <= std::declval<RT>())>>
    : std::true_type {};

template <typename LT, typename RT, typename = void>
struct HasGreaterEqual : std::false_type {};
template <typename LT, typename RT>
struct HasGreaterEqual<
    LT, RT, std::void_t<decltype(std::declval<LT>() >= std::declval<RT>())>>
    : std::true_type {};

template <typename LT, typename RT, typename = void>
struct HasAllComparisonOperators
    : std::conjunction<HasLess<LT, RT>, HasGreater<LT, RT>,
                       HasLessEqual<LT, RT>, HasGreaterEqual<LT, RT>> {};

template <typename LT, typename RT>
struct HasAllComparisonOperators<
    LT, RT,
    std::enable_if_t<std::is_arithmetic_v<LT> && std::is_arithmetic_v<RT>>>
    : std::true_type {};

template <typename LT, typename RT>
constexpr bool HasAllComparisonOperators_v =
    HasAllComparisonOperators<LT, RT>::value;

namespace adl {
struct adl_compare_tag {};
}  // namespace adl

template <typename LT, typename RT>
auto tryAdlCompareThreeWay(const LT& lhs, const RT& rhs, adl::adl_compare_tag)
    -> decltype(compareThreeWay(lhs, rhs));

template <typename LT, typename RT, typename = void>
struct HasExternalCompareThreeWay : std::false_type {};
template <typename LT, typename RT>
struct HasExternalCompareThreeWay<
    LT, RT,
    std::void_t<decltype(tryAdlCompareThreeWay(
        std::declval<const LT&>(), std::declval<const RT&>(),
        adl::adl_compare_tag{}))>> : std::true_type {};

template <typename C, typename T, typename = void>
struct HasMemberCompareThreeWay : std::false_type {};
template <typename C, typename T>
struct HasMemberCompareThreeWay<
    C, T,
    std::void_t<decltype(std::declval<const C>().compareThreeWay(
        std::declval<const T&>()))>> : std::true_type {};

template <typename LT, typename RT>
constexpr bool hasExternalCompareThreeWayV =
    HasExternalCompareThreeWay<LT, RT>::value;
template <typename LT, typename RT>
constexpr bool hasMemberCompareThreeWayV =
    HasMemberCompareThreeWay<LT, RT>::value;

template <typename LT, typename RT>
struct HasAnyCompareThreeWay
    : std::disjunction<HasExternalCompareThreeWay<LT, RT>,
                       HasMemberCompareThreeWay<LT, RT>> {};

template <typename LT, typename RT>
constexpr bool hasAnyCompareThreeWayV = HasAnyCompareThreeWay<LT, RT>::value;

// Helper function for threeway comparisons
template <typename OrderingType, typename T1, typename T2>
constexpr auto compareThreeWayCommon(const T1& lhs, const T2& rhs) {
  if (lhs < rhs) {
    return OrderingType::less;
  } else if (lhs > rhs) {
    return OrderingType::greater;
  }
  return OrderingType::equivalent;
}

template <
    typename LT, typename RT,
    std::enable_if_t<
        std::disjunction_v<
            std::conjunction<std::is_arithmetic<LT>, std::is_arithmetic<RT>>,
            HasAnyCompareThreeWay<LT, RT>, HasAllComparisonOperators<LT, RT>>,
        int> = 0>
constexpr auto compareThreeWay(const LT& lhs, const RT& rhs) {
  if constexpr (std::is_floating_point_v<LT> || std::is_floating_point_v<RT>) {
    if constexpr (std::is_floating_point_v<LT>) {
      if (std::isnan(lhs)) {
        return ql::partial_ordering::unordered;
      }
    }
    if constexpr (std::is_floating_point_v<RT>) {
      if (std::isnan(rhs)) {
        return ql::partial_ordering::unordered;
      }
    }

    using CommonType = std::common_type_t<LT, RT>;
    return compareThreeWayCommon<ql::partial_ordering>(
        static_cast<CommonType>(lhs), static_cast<CommonType>(rhs));
  } else if constexpr (hasAnyCompareThreeWayV<LT, RT>) {
    if constexpr (hasMemberCompareThreeWayV<LT, RT>) {
      return lhs.compareThreeWay(rhs);
    } else {
      return compareThreeWay(lhs, rhs);
    }
  } else {
    return compareThreeWayCommon<ql::strong_ordering>(lhs, rhs);
  }
}

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
constexpr auto compareThreeWay(const ql::strong_ordering& lhs,
                               const T& /*rhs*/) {
  // Return unchanged
  return lhs;
}

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
constexpr auto compareThreeWay(const T& /*lhs*/,
                               const ql::strong_ordering& rhs) {
  // Invert ordering
  if (rhs < 0) {
    return ql::strong_ordering::greater;
  } else if (rhs > 0) {
    return ql::strong_ordering::less;
  }
  return ql::strong_ordering::equal;
}

}  // namespace ql

#define QL_IMPL_CUSTOM_THREEWAY_OPERATOR(TEMPLATE_SPEC, CONSTEXPR_SPEC, T)     \
  TEMPLATE_SPEC friend CONSTEXPR_SPEC bool operator<(const T& a, const T& b) { \
    return compareThreeWay(a, b) < 0;                                          \
  }                                                                            \
  TEMPLATE_SPEC friend CONSTEXPR_SPEC bool operator<=(const T& a,              \
                                                      const T& b) {            \
    return compareThreeWay(a, b) <= 0;                                         \
  }                                                                            \
  TEMPLATE_SPEC friend CONSTEXPR_SPEC bool operator>(const T& a, const T& b) { \
    return compareThreeWay(a, b) > 0;                                          \
  }                                                                            \
  TEMPLATE_SPEC friend CONSTEXPR_SPEC bool operator>=(const T& a,              \
                                                      const T& b) {            \
    return compareThreeWay(a, b) >= 0;                                         \
  }

#define QL_IMPL_CUSTOM_THREEWAY_OPERATOR_LOCAL(TEMPLATE_SPEC, CONSTEXPR_SPEC, \
                                               T)                             \
  TEMPLATE_SPEC CONSTEXPR_SPEC bool operator<(const T& other) const {         \
    return this->compareThreeWay(other) < 0;                                  \
  }                                                                           \
  TEMPLATE_SPEC CONSTEXPR_SPEC bool operator<=(const T& other) const {        \
    return this->compareThreeWay(other) <= 0;                                 \
  }                                                                           \
  TEMPLATE_SPEC CONSTEXPR_SPEC bool operator>(const T& other) const {         \
    return this->compareThreeWay(other) > 0;                                  \
  }                                                                           \
  TEMPLATE_SPEC CONSTEXPR_SPEC bool operator>=(const T& other) const {        \
    return this->compareThreeWay(other) >= 0;                                 \
  }

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR(T) \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR(, , T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR(T) \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR(, constexpr, T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_TEMPLATE(TEMPLATE_SPEC, T) \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR(TEMPLATE_SPEC, , T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR_TEMPLATE(TEMPLATE_SPEC, \
                                                              T)             \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR(TEMPLATE_SPEC, constexpr, T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(T) \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR_LOCAL(, , T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_TEMPLATE(TEMPLATE_SPEC, T) \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR_LOCAL(TEMPLATE_SPEC, , T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T) \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR_LOCAL(, constexpr, T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_QL_CONSTEXPR(T) \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR_LOCAL(, , T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_TEMPLATE( \
    TEMPLATE_SPEC, T)                                                \
  QL_IMPL_CUSTOM_THREEWAY_OPERATOR_LOCAL(TEMPLATE_SPEC, constexpr, T)

#else

namespace ql {

template <typename LT, typename RT>
constexpr inline auto compareThreeWay(const LT& lhs, const RT& rhs) {
  return lhs <=> rhs;
}

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
constexpr inline auto compareThreeWay(const ql::strong_ordering& lhs,
                                      const T& /*rhs*/) {
  // Return unchanged
  return lhs;
}

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
constexpr inline auto compareThreeWay(const T& /*lhs*/,
                                      const ql::strong_ordering& rhs) {
  // Invert ordering
  return 0 <=> rhs;
}

}  // namespace ql

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR(T)           \
  friend auto operator<=>(const T& lhs, const T& rhs) { \
    return compareThreeWay(lhs, rhs);                   \
  }

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR(T)           \
  friend constexpr auto operator<=>(const T& lhs, const T& rhs) { \
    return compareThreeWay(lhs, rhs);                             \
  }

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_TEMPLATE(TEMPLATE_SPEC, T) \
  TEMPLATE_SPEC QL_DEFINE_CUSTOM_THREEWAY_OPERATOR(T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR_TEMPLATE(TEMPLATE_SPEC, \
                                                              T)             \
  TEMPLATE_SPEC QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR(T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(T) \
  auto operator<=>(const T& other) const {          \
    return this->compareThreeWay(other);            \
  }

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_TEMPLATE(TEMPLATE_SPEC, T) \
  TEMPLATE_SPEC QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T) \
  constexpr auto operator<=>(const T& other) const {          \
    return this->compareThreeWay(other);                      \
  }

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_QL_CONSTEXPR(T) \
  QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T)

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_TEMPLATE( \
    TEMPLATE_SPEC, T)                                                \
  TEMPLATE_SPEC QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T)

#endif

// The implementation of the DEFAULTED comparison macros (including the variants
// for derived classes). All of them are one-line wrappers around
// `QL_IMPL_DEFAULTED_COMPARISON(KIND, SCOPE, CONSTEXPR_SPEC, T, BASE, ...)`,
// where `KIND` is `EQUALITY` or `THREEWAY`, `SCOPE` is `FRIEND` or `LOCAL`,
// `CONSTEXPR_SPEC` is `constexpr` or empty, `BASE` is the base class (empty for
// a class without base classes), and `...` are the compared members.
//
// There are two implementations:
// 1. In C++20 mode, the operators are simply defaulted, which compares the base
//    class subobject and then all the data members in declaration order. The
//    `BASE` and the list of members are ignored.
// 2. In C++17 mode, the operators compare the `std::tie` of the base class
//    subobject and the listed members. This is only equivalent to the
//    defaulted operators of 1. if the `BASE` is the only base class and the
//    listed members are exactly the data members in declaration order, and
//    none of them is a C array (which `std::tie` would compare by address).
//    With reflection (see `backports/reflection.h`), implementation 2. is also
//    used instead of 1. (with `operator<=>` instead of the four relational
//    operators), and these conditions are checked at compile time in the body
//    of `membersAsTie()`. That body is part of every operator, so for a class
//    template the check happens when one of the operators is used.
#if defined(QLEVER_CPP_17) || defined(QLEVER_HAS_REFLECTION)

#ifdef QLEVER_HAS_REFLECTION
namespace ql::reflection {
// Return the empty string iff the stringified list of members of one of the
// defaulted comparison macros (`listedMembers`) contains exactly the data
// members of `T` in declaration order, none of them is a C array, and the
// direct base classes of `T` are exactly the `Bases`. Otherwise return an error
// message.
template <typename T, typename... Bases>
consteval std::string_view checkComparedMembers(
    std::string_view listedMembers) {
  constexpr auto ctx = std::meta::access_context::unchecked();
  auto error = [](std::string_view message) {
    return makeErrorMessage<T>(
        "comparison macro (see `backports/three_way_comparison.h`)", message);
  };
  auto bases = std::meta::bases_of(^^T, ctx);
  std::array<std::meta::info, sizeof...(Bases)> expectedBases{^^Bases...};
  // The `BASE` that is passed to the macro may be a type alias (which is
  // required if its name contains a comma, see above). `std::meta::dealias`
  // strips such aliases, such that the base is compared as the actual type.
  //
  // NOTE: This uses `std::ranges` instead of `ql::ranges`, because this code
  // only exists in C++26 mode, and `backports/algorithm.h` is too heavy for
  // this widely included header.
  bool basesMatch = std::ranges::equal(
      bases, expectedBases, [](std::meta::info base, std::meta::info expected) {
        return std::meta::is_same_type(std::meta::type_of(base),
                                       std::meta::dealias(expected));
      });
  if (!basesMatch) {
    return error(
        "The base classes of the class have to be exactly the base class that "
        "is passed to the macro (use the `..._DERIVED` variants for a class "
        "with a single base class, and no macro for multiple base classes)");
  }
  for (auto member : std::meta::nonstatic_data_members_of(^^T, ctx)) {
    if (std::meta::is_array_type(std::meta::type_of(member))) {
      return error("The data member `" +
                   std::string{std::meta::identifier_of(member)} +
                   "` is a C array, which is compared by address in C++17 "
                   "mode, use a `std::array` instead");
    }
  }
  auto actualMembers = namesOfDataMembers<T>();
  if (splitListOfIdentifiers(listedMembers) != actualMembers) {
    return error("The listed members `" + std::string{listedMembers} +
                 "` are not exactly the data members of the class in "
                 "declaration order, which are `" +
                 joinNames(actualMembers) + "`");
  }
  return {};
}
}  // namespace ql::reflection

// Expand to `, BASE` if `BASE` is not empty, else to nothing.
#define QL_IMPL_COMMA_AND_BASE(...) __VA_OPT__(, __VA_ARGS__)
#define QL_IMPL_CHECK_COMPARED_MEMBERS(T, BASE, ...)                          \
  QL_STATIC_ASSERT_NO_ERROR_MESSAGE(                                          \
      ::ql::reflection::checkComparedMembers<T QL_IMPL_COMMA_AND_BASE(BASE)>( \
          #__VA_ARGS__))
// In C++26 mode, the operators are always `constexpr`, like the defaulted
// operators of C++20 mode, which are implicitly `constexpr` whenever the
// members allow it (and since C++23 a `constexpr` function may be one that can
// never be constant-evaluated).
#define QL_IMPL_TIE_CONSTEXPR(CONSTEXPR_SPEC) constexpr
#else
#define QL_IMPL_CHECK_COMPARED_MEMBERS(T, BASE, ...)
#define QL_IMPL_TIE_CONSTEXPR(CONSTEXPR_SPEC) CONSTEXPR_SPEC
#endif

// The base class subobject (if `BASE` is not empty) as a (possibly empty)
// argument list for `std::tie`. Note that the cast is required to actually call
// the operators of the base class, because the members and operators of the
// derived class hide those of the base class.
#define QL_IMPL_BASE_SUBOBJECT(...) \
  __VA_OPT__(static_cast<const __VA_ARGS__&>(*this))

// A single operator `OP` with return type `RET` that compares the results of
// `membersAsTie()`, either as a `FRIEND` or as a `LOCAL` (member) function.
#define QL_IMPL_TIE_OPERATOR_FRIEND(CONSTEXPR_SPEC, T, RET, OP)   \
  friend CONSTEXPR_SPEC RET operator OP(const T& a, const T& b) { \
    return a.membersAsTie() OP b.membersAsTie();                  \
  }
#define QL_IMPL_TIE_OPERATOR_LOCAL(CONSTEXPR_SPEC, T, RET, OP) \
  CONSTEXPR_SPEC RET operator OP(const T& other) const {       \
    return this->membersAsTie() OP other.membersAsTie();       \
  }

// The set of operators for each `KIND`. With reflection (C++26), `operator==`
// and `operator<=>` suffice, because the others are rewritten in terms of them.
// clang-format off
#ifdef QLEVER_HAS_REFLECTION
#define QL_IMPL_TIE_OPERATORS_EQUALITY(SCOPE, CONSTEXPR_SPEC, T) \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, ==)
#define QL_IMPL_TIE_OPERATORS_THREEWAY(SCOPE, CONSTEXPR_SPEC, T) \
  QL_IMPL_TIE_OPERATORS_EQUALITY(SCOPE, CONSTEXPR_SPEC, T)      \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, auto, <=>)
#else
#define QL_IMPL_TIE_OPERATORS_EQUALITY(SCOPE, CONSTEXPR_SPEC, T) \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, ==)     \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, !=)
#define QL_IMPL_TIE_OPERATORS_THREEWAY(SCOPE, CONSTEXPR_SPEC, T) \
  QL_IMPL_TIE_OPERATORS_EQUALITY(SCOPE, CONSTEXPR_SPEC, T)      \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, <)      \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, <=)     \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, >)      \
  QL_IMPL_TIE_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, >=)
#endif
// clang-format on

#define QL_IMPL_DEFAULTED_COMPARISON(KIND, SCOPE, CONSTEXPR_SPEC, T, BASE, \
                                     ...)                                  \
  QL_IMPL_TIE_CONSTEXPR(CONSTEXPR_SPEC) auto membersAsTie() const {        \
    QL_IMPL_CHECK_COMPARED_MEMBERS(T, BASE, __VA_ARGS__)                   \
    return std::tuple_cat(std::tie(QL_IMPL_BASE_SUBOBJECT(BASE)),          \
                          std::tie(__VA_ARGS__));                          \
  }                                                                        \
  QL_IMPL_TIE_OPERATORS_##KIND(SCOPE, QL_IMPL_TIE_CONSTEXPR(CONSTEXPR_SPEC), T)

#else

#define QL_IMPL_DEFAULTED_OPERATOR_FRIEND(CONSTEXPR_SPEC, T, RET, OP) \
  friend CONSTEXPR_SPEC RET operator OP(const T&, const T&) = default;
#define QL_IMPL_DEFAULTED_OPERATOR_LOCAL(CONSTEXPR_SPEC, T, RET, OP) \
  CONSTEXPR_SPEC RET operator OP(const T&) const = default;
#define QL_IMPL_DEFAULTED_OPERATORS_EQUALITY(SCOPE, CONSTEXPR_SPEC, T) \
  QL_IMPL_DEFAULTED_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, bool, ==)
// Note: A defaulted `operator<=>` also implicitly declares a defaulted
// `operator==`.
#define QL_IMPL_DEFAULTED_OPERATORS_THREEWAY(SCOPE, CONSTEXPR_SPEC, T) \
  QL_IMPL_DEFAULTED_OPERATOR_##SCOPE(CONSTEXPR_SPEC, T, auto, <=>)

#define QL_IMPL_DEFAULTED_COMPARISON(KIND, SCOPE, CONSTEXPR_SPEC, T, BASE, \
                                     ...)                                  \
  QL_IMPL_DEFAULTED_OPERATORS_##KIND(SCOPE, CONSTEXPR_SPEC, T)

#endif

// The public DEFAULTED macros, see the documentation at the beginning of this
// file.
#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(THREEWAY, FRIEND, , T, , __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_CONSTEXPR(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(THREEWAY, FRIEND, constexpr, T, , __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(THREEWAY, LOCAL, , T, , __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(THREEWAY, LOCAL, constexpr, T, , __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(EQUALITY, FRIEND, , T, , __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_CONSTEXPR(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(EQUALITY, FRIEND, constexpr, T, , __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(EQUALITY, LOCAL, , T, , __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR(T, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(EQUALITY, LOCAL, constexpr, T, , __VA_ARGS__)

// The variants of the macros above for derived classes.
#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_DERIVED(T, BASE, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(THREEWAY, LOCAL, , T, BASE, __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_DERIVED(T, BASE, \
                                                                      ...)     \
  QL_IMPL_DEFAULTED_COMPARISON(THREEWAY, LOCAL, constexpr, T, BASE, __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_DERIVED(T, BASE, ...) \
  QL_IMPL_DEFAULTED_COMPARISON(EQUALITY, LOCAL, , T, BASE, __VA_ARGS__)
#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR_DERIVED(T, BASE, \
                                                                      ...)     \
  QL_IMPL_DEFAULTED_COMPARISON(EQUALITY, LOCAL, constexpr, T, BASE, __VA_ARGS__)

#endif  // QLEVER_SRC_BACKPORTS_THREE_WAY_COMPARISON_H
