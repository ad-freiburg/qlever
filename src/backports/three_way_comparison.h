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
 * CUSTOM COMPARISON MACROS (for implementing custom comparison logic):
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR(Class): Friend operator
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_CONSTEXPR(Class):
 * Constexpr version
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL(Class): Member operator
 * - QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(Class):
 * Constexpr member version
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

#define QL_DEFINE_CLASS_MEMBERS_AS_TIE(CONSTEXPR_SPEC, ...) \
  CONSTEXPR_SPEC auto membersAsTie() const { return std::tie(__VA_ARGS__); }

#define QL_IMPL_DEFAULTED_EQUALITY_OPERATOR(CONSTEXPR_SPEC, T)    \
  friend CONSTEXPR_SPEC bool operator==(const T& a, const T& b) { \
    return a.membersAsTie() == b.membersAsTie();                  \
  }                                                               \
  friend CONSTEXPR_SPEC bool operator!=(const T& a, const T& b) { \
    return a.membersAsTie() != b.membersAsTie();                  \
  }

#define QL_IMPL_DEFAULTED_THREEWAY_OPERATOR(CONSTEXPR_SPEC, T)    \
  friend CONSTEXPR_SPEC bool operator<(const T& a, const T& b) {  \
    return a.membersAsTie() < b.membersAsTie();                   \
  }                                                               \
  friend CONSTEXPR_SPEC bool operator<=(const T& a, const T& b) { \
    return a.membersAsTie() <= b.membersAsTie();                  \
  }                                                               \
  friend CONSTEXPR_SPEC bool operator>(const T& a, const T& b) {  \
    return a.membersAsTie() > b.membersAsTie();                   \
  }                                                               \
  friend CONSTEXPR_SPEC bool operator>=(const T& a, const T& b) { \
    return a.membersAsTie() >= b.membersAsTie();                  \
  }

#define QL_IMPL_DEFAULTED_EQUALITY_OPERATOR_LOCAL(CONSTEXPR_SPEC, T) \
  CONSTEXPR_SPEC bool operator==(const T& other) const {             \
    return this->membersAsTie() == other.membersAsTie();             \
  }                                                                  \
  CONSTEXPR_SPEC bool operator!=(const T& other) const {             \
    return this->membersAsTie() != other.membersAsTie();             \
  }

#define QL_IMPL_DEFAULTED_THREEWAY_OPERATOR_LOCAL(CONSTEXPR_SPEC, T) \
  CONSTEXPR_SPEC bool operator<(const T& other) const {              \
    return this->membersAsTie() < other.membersAsTie();              \
  }                                                                  \
  CONSTEXPR_SPEC bool operator<=(const T& other) const {             \
    return this->membersAsTie() <= other.membersAsTie();             \
  }                                                                  \
  CONSTEXPR_SPEC bool operator>(const T& other) const {              \
    return this->membersAsTie() > other.membersAsTie();              \
  }                                                                  \
  CONSTEXPR_SPEC bool operator>=(const T& other) const {             \
    return this->membersAsTie() >= other.membersAsTie();             \
  }

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_CONSTEXPR(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(constexpr, __VA_ARGS__)        \
  QL_IMPL_DEFAULTED_THREEWAY_OPERATOR(constexpr, T)             \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR(constexpr, T)

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(, __VA_ARGS__)       \
  QL_IMPL_DEFAULTED_THREEWAY_OPERATOR(, T)            \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR(, T)

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_CONSTEXPR(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(constexpr, __VA_ARGS__)        \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR(constexpr, T)

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(, __VA_ARGS__)       \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR(, T)

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(constexpr, __VA_ARGS__)              \
  QL_IMPL_DEFAULTED_THREEWAY_OPERATOR_LOCAL(constexpr, T)             \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR_LOCAL(constexpr, T)

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(, __VA_ARGS__)             \
  QL_IMPL_DEFAULTED_THREEWAY_OPERATOR_LOCAL(, T)            \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR_LOCAL(, T)

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(constexpr, __VA_ARGS__)              \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR_LOCAL(constexpr, T)

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(T, ...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE(, __VA_ARGS__)             \
  QL_IMPL_DEFAULTED_EQUALITY_OPERATOR_LOCAL(, T)

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

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_CONSTEXPR(T, ...) \
  friend constexpr auto operator<=>(const T&, const T&) = default;

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR(T, ...) \
  friend auto operator<=>(const T&, const T&) = default;

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_CONSTEXPR(T, ...) \
  friend constexpr bool operator==(const T&, const T&) = default;

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR(T, ...) \
  friend bool operator==(const T&, const T&) = default;

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T, ...) \
  constexpr auto operator<=>(const T&) const = default;

#define QL_DEFINE_DEFAULTED_THREEWAY_OPERATOR_LOCAL(T, ...) \
  auto operator<=>(const T&) const = default;

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL_CONSTEXPR(T, ...) \
  constexpr bool operator==(const T&) const = default;

#define QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(T, ...) \
  bool operator==(const T&) const = default;

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

#define QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR_TEMPLATE( \
    TEMPLATE_SPEC, T)                                                \
  TEMPLATE_SPEC QL_DEFINE_CUSTOM_THREEWAY_OPERATOR_LOCAL_CONSTEXPR(T)

#endif

#endif  // QLEVER_SRC_BACKPORTS_THREE_WAY_COMPARISON_H
