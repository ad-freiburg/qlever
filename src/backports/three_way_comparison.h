//
// Created by kalmbacj on 9/2/25.
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef THREE_WAY_COMPARISON_H
#define THREE_WAY_COMPARISON_H

#include <cmath>
#include <tuple>
#include <type_traits>

#ifdef QLEVER_CPP_17

namespace ql {

using strong_ordering = std::strong_ordering;
using partial_ordering = std::partial_ordering;
using weak_ordering = std::weak_ordering;

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

template <typename LT, typename RT>
struct HasAllComparisonOperators
    : std::conjunction<HasLess<LT, RT>, HasGreater<LT, RT>,
                       HasLessEqual<LT, RT>, HasGreaterEqual<LT, RT>> {};

template <typename LT, typename RT>
constexpr bool HasAllComparisonOperators_v =
    HasAllComparisonOperators<LT, RT>::value;

namespace adl {
struct adl_compare_tag {};
}  // namespace adl

template <typename LT, typename RT>
auto try_adl_compareThreeWay(const LT& lhs, const RT& rhs, adl::adl_compare_tag)
    -> decltype(compareThreeWay(lhs, rhs));

template <typename LT, typename RT, typename = void>
struct HasExternalCompareThreeWay : std::false_type {};
template <typename LT, typename RT>
struct HasExternalCompareThreeWay<
    LT, RT,
    std::void_t<decltype(try_adl_compareThreeWay(
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
constexpr bool hasExternalCompareThreeWay_v =
    HasExternalCompareThreeWay<LT, RT>::value;
template <typename LT, typename RT>
constexpr bool hasMemberCompareThreeWay_v =
    HasMemberCompareThreeWay<LT, RT>::value;

template <typename LT, typename RT>
struct HasAnyCompareThreeWay
    : std::disjunction<HasExternalCompareThreeWay<LT, RT>,
                       HasMemberCompareThreeWay<LT, RT>> {};

template <typename LT, typename RT>
constexpr bool hasAnyCompareThreeWay_v = HasAnyCompareThreeWay<LT, RT>::value;

template <typename LT, typename RT>
constexpr std::enable_if_t<
    std::conjunction_v<std::is_floating_point<LT>, std::is_floating_point<RT>>,
    ql::partial_ordering>
compareThreeWay(const LT& lhs, const RT& rhs) {
  if (std::isnan(lhs) || std::isnan(rhs)) {
    return ql::partial_ordering::unordered;
  }
  if (lhs < rhs) {
    return ql::partial_ordering::less;
  }
  if (lhs > rhs) {
    return ql::partial_ordering::greater;
  }
  return ql::partial_ordering::equivalent;
}

template <typename LT, typename RT,
          std::enable_if_t<std::conjunction_v<HasAllComparisonOperators<LT, RT>,
                                              std::negation<std::disjunction<
                                                  HasAnyCompareThreeWay<LT, RT>,
                                                  std::is_floating_point<LT>,
                                                  std::is_floating_point<RT>>>>,
                           int> = 0>
constexpr auto compareThreeWay(const LT& lhs, const RT& rhs) {
  if (lhs < rhs) {
    return ql::strong_ordering::less;
  } else if (lhs > rhs) {
    return ql::strong_ordering::greater;
  }
  return ql::strong_ordering::equal;
}

template <typename LT, typename RT,
          std::enable_if_t<ql::HasAnyCompareThreeWay<LT, RT>::value, int> = 0>
constexpr auto compareThreeWay(const LT& lhs, const RT& rhs) {
  if constexpr (hasMemberCompareThreeWay_v<LT, RT>) {
    return lhs.compareThreeWay(rhs);
  } else {
    return compareThreeWay(lhs, rhs);
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
  return std::strong_ordering::equal;
}

}  // namespace ql

#define QL_CONSTEXPR_IF(C) constexpr C

#define QL_DEFINE_CLASS_MEMBERS_AS_TIE_(CONSTEXPR, ...) \
  CONSTEXPR auto membersAsTie() const { return std::make_tuple(__VA_ARGS__); }

#define QL_DEFINE_CLASS_MEMBERS_AS_TIE(...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE_(, __VA_ARGS__)

#define QL_DEFINE_CLASS_MEMBERS_AS_TIE_CONSTEXPR(...) \
  QL_DEFINE_CLASS_MEMBERS_AS_TIE_(constexpr, __VA_ARGS__)

#define CPP_equality_operator(CONSTEXPR, T)                  \
  friend CONSTEXPR bool operator==(const T& a, const T& b) { \
    return a.membersAsTie() == b.membersAsTie();             \
  }                                                          \
  friend CONSTEXPR bool operator!=(const T& a, const T& b) { \
    return a.membersAsTie() != b.membersAsTie();             \
  }

#define CPP_threeway_operator(CONSTEXPR, T)                  \
  friend CONSTEXPR bool operator<(const T& a, const T& b) {  \
    return a.membersAsTie() < b.membersAsTie();              \
  }                                                          \
  friend CONSTEXPR bool operator<=(const T& a, const T& b) { \
    return a.membersAsTie() <= b.membersAsTie();             \
  }                                                          \
  friend CONSTEXPR bool operator>(const T& a, const T& b) {  \
    return a.membersAsTie() > b.membersAsTie();              \
  }                                                          \
  friend CONSTEXPR bool operator>=(const T& a, const T& b) { \
    return a.membersAsTie() >= b.membersAsTie();             \
  }

#define CPP_equality_operator_local(T)                   \
  bool operator==(const T& other) const {                \
    return this->membersAsTie() == other.membersAsTie(); \
  }                                                      \
  bool operator!=(const T& other) const {                \
    return this->membersAsTie() != other.membersAsTie(); \
  }

#define CPP_threeway_operator_local(T)                   \
  bool operator<(const T& other) const {                 \
    return this->membersAsTie() < other.membersAsTie();  \
  }                                                      \
  bool operator<=(const T& other) const {                \
    return this->membersAsTie() <= other.membersAsTie(); \
  }                                                      \
  bool operator>(const T& other) const {                 \
    return this->membersAsTie() > other.membersAsTie();  \
  }                                                      \
  bool operator>=(const T& other) const {                \
    return this->membersAsTie() >= other.membersAsTie(); \
  }

#define QL_DEFINE_THREEWAY_OPERATOR(T) CPP_threeway_operator(, T)

#define QL_DEFINE_EQUALITY_OPERATOR(T) CPP_equality_operator(, T)

#define QL_DEFINE_THREEWAY_OPERATOR_CONSTEXPR(T) \
  CPP_threeway_operator(constexpr, T)

#define QL_DEFINE_EQUALITY_OPERATOR_CONSTEXPR(T) \
  CPP_equality_operator(constexpr, T)

#define QL_DEFINE_THREEWAY_OPERATOR_LOCAL(T) CPP_threeway_operator_local(T)

#define QL_DEFINE_EQUALITY_OPERATOR_LOCAL(T) CPP_equality_operator_local(T)

#define CPP_threeway_operator_custom(CONSTEXPR, T, SIGNATURE, BODY)          \
  static CONSTEXPR auto compareThreeWay SIGNATURE BODY friend CONSTEXPR bool \
  operator<(const T& a, const T& b) {                                        \
    return compareThreeWay(a, b) < 0;                                        \
  }                                                                          \
  friend CONSTEXPR bool operator<=(const T& a, const T& b) {                 \
    return compareThreeWay(a, b) <= 0;                                       \
  }                                                                          \
  friend CONSTEXPR bool operator>(const T& a, const T& b) {                  \
    return compareThreeWay(a, b) > 0;                                        \
  }                                                                          \
  friend CONSTEXPR bool operator>=(const T& a, const T& b) {                 \
    return compareThreeWay(a, b) >= 0;                                       \
  }

#define CPP_threeway_operator_custom_local(CONSTEXPR, T, SIGNATURE, BODY) \
  CONSTEXPR auto compareThreeWay SIGNATURE BODY CONSTEXPR bool operator<( \
      const T& other) const {                                             \
    return this->compareThreeWay(other) < 0;                              \
  }                                                                       \
  CONSTEXPR bool operator<=(const T& other) const {                       \
    return this->compareThreeWay(other) <= 0;                             \
  }                                                                       \
  CONSTEXPR bool operator>(const T& other) const {                        \
    return this->compareThreeWay(other) > 0;                              \
  }                                                                       \
  CONSTEXPR bool operator>=(const T& other) const {                       \
    return this->compareThreeWay(other) >= 0;                             \
  }

#define CPP_threeway_operator_custom_local_tmpl(TMPL, CONSTEXPR, T, SIGNATURE, \
                                                BODY)                          \
  TMPL CONSTEXPR auto compareThreeWay SIGNATURE BODY TMPL CONSTEXPR bool       \
  operator<(const T& other) const {                                            \
    return this->compareThreeWay(other) < 0;                                   \
  }                                                                            \
  TMPL CONSTEXPR bool operator<=(const T& other) const {                       \
    return this->compareThreeWay(other) <= 0;                                  \
  }                                                                            \
  TMPL CONSTEXPR bool operator>(const T& other) const {                        \
    return this->compareThreeWay(other) > 0;                                   \
  }                                                                            \
  TMPL CONSTEXPR bool operator>=(const T& other) const {                       \
    return this->compareThreeWay(other) >= 0;                                  \
  }

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM(T, SIGNATURE, BODY) \
  CPP_threeway_operator_custom(, T, SIGNATURE, BODY)

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_CONSTEXPR(T, SIGNATURE, BODY) \
  CPP_threeway_operator_custom(constexpr, T, SIGNATURE, BODY)

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_LOCAL(T, SIGNATURE, BODY) \
  CPP_threeway_operator_custom_local(, T, SIGNATURE, BODY)

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_LOCAL_TMPL(TMPL, T, SIGNATURE, \
                                                      BODY)               \
  CPP_threeway_operator_custom_local_tmpl(TMPL, , T, SIGNATURE, BODY)

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_CONSTEXPR_LOCAL(T, SIGNATURE, BODY) \
  CPP_threeway_operator_custom_local(constexpr, T, SIGNATURE, BODY)

#define QL_DECLARE_THREEWAY_OPERATOR_CUSTOM_LOCAL(T, SIGNATURE) \
  auto compareThreeWay SIGNATURE;                               \
  bool operator<(const T& other) const {                        \
    return this->compareThreeWay(other) < 0;                    \
  }                                                             \
  bool operator<=(const T& other) const {                       \
    return this->compareThreeWay(other) <= 0;                   \
  }                                                             \
  bool operator>(const T& other) const {                        \
    return this->compareThreeWay(other) > 0;                    \
  }                                                             \
  bool operator>=(const T& other) const {                       \
    return this->compareThreeWay(other) >= 0;                   \
  }

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_LOCAL_IMPL(T, SIGNATURE, BODY) \
  auto T::compareThreeWay SIGNATURE BODY

#else

namespace ql {

using strong_ordering = std::strong_ordering;
using partial_ordering = std::partial_ordering;
using weak_ordering = std::weak_ordering;

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

#define QL_DEFINE_CLASS_MEMBERS_AS_TIE(...)

#define QL_DEFINE_CLASS_MEMBERS_AS_TIE_CONSTEXPR(...)

#define QL_DEFINE_THREEWAY_OPERATOR(T) \
  auto operator<=>(const T&) const = default;

#define QL_DEFINE_EQUALITY_OPERATOR(T) \
  bool operator==(const T&) const = default;

#define QL_DEFINE_THREEWAY_OPERATOR_CONSTEXPR(T) \
  constexpr auto operator<=>(const T&) const = default;

#define QL_DEFINE_EQUALITY_OPERATOR_CONSTEXPR(T) \
  constexpr bool operator==(const T&) const = default;

#define QL_DEFINE_THREEWAY_OPERATOR_LOCAL(T) QL_DEFINE_THREEWAY_OPERATOR(T)

#define QL_DEFINE_EQUALITY_OPERATOR_LOCAL(T) QL_DEFINE_EQUALITY_OPERATOR(T)

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM(T, SIGNATURE, BODY) \
  friend auto operator<=> SIGNATURE BODY

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_CONSTEXPR(T, SIGNATURE, BODY) \
  friend constexpr auto operator<=> SIGNATURE BODY

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_LOCAL(T, SIGNATURE, BODY) \
  auto operator<=> SIGNATURE BODY

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_LOCAL_TMPL(TMPL, T, SIGNATURE, \
                                                      BODY)               \
  TMPL auto operator<=> SIGNATURE BODY

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_CONSTEXPR_LOCAL(T, SIGNATURE, BODY) \
  constexpr auto operator<=> SIGNATURE BODY

#define QL_DECLARE_THREEWAY_OPERATOR_CUSTOM_LOCAL(T, SIGNATURE) \
  auto operator<=> SIGNATURE;

#define QL_DEFINE_THREEWAY_OPERATOR_CUSTOM_LOCAL_IMPL(T, SIGNATURE, BODY) \
  auto T::operator<=> SIGNATURE BODY

#endif

#endif  // THREE_WAY_COMPARISON_H
