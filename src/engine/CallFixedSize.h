// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

// TODO<joka921, clang> Clang 13 issues a lot of strange and false positive
// warnings (this bug was fixed in clang 14 and newer. This suppresses all
// warnings for this file. Remove this pragma as soon as we no longer support
// clang 13.
#ifdef __clang__
#pragma clang system_header
#endif

#include <functional>
#include <optional>

#include "util/Exception.h"
#include "util/Forward.h"

// The macros in this file provide automatic generation of if clauses that
// enable transformation of runtime variables to a limited range of compile
// time values and a default case. There are currently two possibilities to use this interface:
// 1. Using the macros CALL_FIXED_SIZE_1, CALL_FIXED_SIZE_2 and CALL_FIXED_SIZE_3.
//    `CALL_FIXED_SIZE_1(i, func, additionalArguments...)` calls
//     `std::invoke(func<i'>, additionalArguments...)` where `i' = i <=5? i : 0`
//    `i` must be an integer.
//   `CALL_FIXED_SIZE_2` and `CALL_FIXED_SIZE_3` work similarly, but with 2 or 3
//   integer arguments that are converted to template parameters.
// 2. Using the templated functions `callFixedSize1`, `callFixedSize2` and `callFixedSize3`,
//    that all live inside the `ad_utility` namespace.
//    `callFixedSize1(i, lambda, args...)` calls
//    `lambda(std::integer_constant<int, i'>, args...)`
// The first version allows a convenient syntax for most cases.
// The second version provides a macro-free typesafe interface that can be also
// used in the corner cases where the first version fails. It also is easier
// to maintain (e.g. to set the maximal value the template arguments to more
// than 5.

namespace ad_utility {
namespace detail {
template <int i>
static constexpr auto INT = std::integral_constant<int, i>{};
// Internal helper function that calls the `lambda` with `i`(see above)
// as an explicit template parameter. Requires that `i <= maxValue`.
template <int maxValue>
decltype(auto) callLambdaWithStaticInt2(int i, auto&& lambda, auto&&... args) {
  AD_CHECK(i <= maxValue);
  using Result = decltype(lambda.template operator()<1>());
  using Storage = std::conditional_t<std::is_void_v<Result>, int, std::optional<Result>>;

  Storage result;
  auto applyIf = [&result, lambda = AD_FWD(lambda),
                  &... args = AD_FWD(args)]<int I>(int i) mutable {
    if (i == I) {
      if constexpr (std::is_void_v<Result>) {
        lambda.template operator()<I>(AD_FWD(args)...);
      } else {
        result = lambda.template operator()<I>(AD_FWD(args)...);
      }
    }
  };

  auto f = [&applyIf, i ]<int... Is>(std::integer_sequence<int, Is...>) {
    (..., applyIf.template operator()<Is>(i));
  };
  f(std::make_integer_sequence<int, maxValue>{});

  if constexpr (!std::is_void_v<Result>) {
    return std::move(result.value());
  }
}

constexpr int prime(int x, int maxValue) { return x < maxValue ? x : 0; }
}

// =============================================================================
// One Variable
// =============================================================================
// The main implementation for the case of one template variable.
// A good starting point for understanding the mechanisms.
decltype(auto) callFixedSize1(int i, auto&& functor, auto&&... args) {
  using namespace detail;
  auto lambda = [&]<int I>() -> decltype(auto) {
    return std::invoke(AD_FWD(functor), detail::INT<I>, AD_FWD(args)...);
  };
  return callLambdaWithStaticInt2<6>(prime(i, 6), std::move(lambda));
};

// Main implementation for the two variable case.
decltype(auto) callFixedSize2(int i, int j, auto&& functor, auto&&... args) {
  using namespace detail;
  static constexpr int numValues = 6;
  auto p = [](int x) { return prime(x, numValues); };
  int value = p(i) * numValues + p(j);
  auto lambda = [functor = AD_FWD(functor), &args...]<int I>() mutable -> decltype(auto) {
    return AD_FWD(functor)(INT<I / numValues>, INT<I % numValues>, AD_FWD(args)...);
  };
  return callLambdaWithStaticInt2<numValues * numValues>(value, std::move(lambda));
}

// Three variables.
decltype(auto) callFixedSize3(int i, int j, int k, auto&& functor, auto&&... args) {
  using namespace detail;
  static constexpr int numValues = 6;
  auto p = [](int x) { return prime(x, numValues); };
  int value = p(i) * numValues * numValues + p(j) * numValues + p(k);
  auto lambda = [functor = AD_FWD(functor), &args...]<int I>() mutable -> decltype(auto) {
    return AD_FWD(functor)(INT<I / numValues / numValues>, INT<(I / numValues) % numValues>,
                           INT<I % numValues>, AD_FWD(args)...);
  };
  return callLambdaWithStaticInt2<numValues * numValues * numValues>(value, std::move(lambda));
}
}  // namespace ad_utility

// The definitions of the lambdas.
#define CALL_FIXED_SIZE_1(i, func, ...)                                                     \
  ad_utility::callFixedSize1(                                                               \
      i, [](auto I, auto&&... args) -> decltype(auto) { return func<I>(AD_FWD(args)...); }, \
      __VA_ARGS__)

#define CALL_FIXED_SIZE_2(i, j, func, ...)                   \
  ad_utility::callFixedSize2(                                \
      i, j,                                                  \
      [](auto I, auto J, auto&&... args) -> decltype(auto) { \
        return std::invoke(func<I, J>, AD_FWD(args)...);     \
      },                                                     \
      __VA_ARGS__)

#define CALL_FIXED_SIZE_3(i, j, k, func, ...)                         \
  ad_utility::callFixedSize3(                                         \
      i, j, k,                                                        \
      [&](auto I, auto J, auto K, auto&&... args) -> decltype(auto) { \
        return std::invoke(func<I, J, K>, AD_FWD(args)...);           \
      },                                                              \
      __VA_ARGS__)
