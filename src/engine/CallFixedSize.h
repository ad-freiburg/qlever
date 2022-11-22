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

#include "global/Constants.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/Metaprogramming.h"

// The functions and macros are able to call functions that take a set of integers as
// template parameters with integers that are only known at runtime. To make this
// work the possible compile time integers have to be in a range [0, ..., MAX] where
// MAX is a compile-time constant. For runtime integers that are > MAX, the function
// is called with 0 as a compile-time parameter. This behavior is useful for the
// `IdTables` (see `IdTable.h`) where `0` is a special value that menas "the number of columns is
// only known at runtime". Note that it is relatively easy to customize this behavior such that for
// example integers that are > MAX lead to a runtime-error. This would make it possible to use these
// facilities also for a "static switch"
// TODO<joka921> Also implement such a behavior.

// There are currently two possibilities to use this interface:
// 1. Using the macros CALL_FIXED_SIZE_1, CALL_FIXED_SIZE_2 and CALL_FIXED_SIZE_3.
//    `CALL_FIXED_SIZE_1(i, func, additionalArguments...)` calls
//     `std::invoke(func<f(i)>, additionalArguments...)` where f(i) = i
//     <=DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE? i : 0`
//   `CALL_FIXED_SIZE_2` and `CALL_FIXED_SIZE_3` work similarly, but with 2 or 3
//   integer arguments that are converted to template parameters.
// 2. Using the templated functions `callFixedSize1`, `callFixedSize2` and `callFixedSize3`,
//    that all live inside the `ad_utility` namespace.
//    `callFixedSize1(i, lambda, args...)` calls
//    `lambda(std::integer_constant<int, i'>, args...)`
// The first version allows a convenient syntax for most cases.
// The second version provides a macro-free typesafe interface that can be also
// used in the corner cases where the first version fails. It also allows to
// configure the maximal value, and it is easy to implement `callFixedSize4` etc.
// for a higher number of parameters.

// For simple examples that illustrate the possibilities and limitations of both
// the macro and the template function interface see `CallFixedSizeTest.cpp`.

namespace ad_utility {
namespace detail {
// Internal helper function that calls
// `lambda.template operator()<i>(args...)`.
// Requires that `i <= maxValue`, else an `AD_CHECK` fails.
template <int maxValue>
auto callLambdaWithStaticInt(int i, auto&& lambda, auto&&... args) {
  AD_CHECK(i <= maxValue);

  // We store the result of the actual computation in a `std::optional`.
  // If the `lambda` returns void we don't store anything, but we still need
  // a type for the `result` variable. We choose `int` as a dummy for this case.

  using Result = decltype(lambda.template operator()<1>());
  static constexpr bool resultIsVoid = std::is_void_v<Result>;
  using Storage = std::conditional_t<std::is_void_v<Result>, int, std::optional<Result>>;
  Storage result;
  // Lambda: If the compile time parameter `I` and the runtime parameter `i`
  // are equal, then call the `lambda` with `I` as a template parameter and
  // store the result in `result` (unless it is `void`).
  auto applyIf = [&result, lambda = AD_FWD(lambda), ... args = AD_FWD(args)]<int I>(int i) mutable {
    if (i == I) {
      if constexpr (resultIsVoid) {
        lambda.template operator()<I>(AD_FWD(args)...);
      } else {
        result = lambda.template operator()<I>(AD_FWD(args)...);
      }
    }
  };

  // Lambda: call `applyIf` for all the compile-time integers `Is...`. The
  // runtime parameter always is `i`.
  auto f = [&applyIf, i ]<int... Is>(std::integer_sequence<int, Is...>) {
    (..., applyIf.template operator()<Is>(i));
  };

  // Call f for all compile-time integers in `[0, ..., maxValue]`.
  // Exactly one of them is equal to `i`, and so the lambda will be executed
  // exactly once.
  f(std::make_integer_sequence<int, maxValue + 1>{});

  if constexpr (!resultIsVoid) {
    return std::move(result.value());
  }
}

// The default function that maps `x` to the range `[0, ..., maxValue]`
constexpr int mapToZeroIfTooLarge(int x, int maxValue) { return x <= maxValue ? x : 0; }

// Uniquely encode the `array` into a single integer
// in the range `[0,...,(maxValue + 1) ^ NumIntegers - 1]` where `^` denotes
// exponentiation. Requires that all elements in the `array` are `<= maxValue`,
// else a runtime check fails.
template <size_t NumIntegers>
requires requires { NumIntegers > 0; }
int arrayToSingleInteger(const std::array<int, NumIntegers>& array, const int maxValue) {
  AD_CHECK(std::ranges::all_of(array, [maxValue](int i) { return i <= maxValue; }));
  int value = 0;
  // TODO<joka921, clang16> use `std::views`.
  for (size_t i = 0; i < array.size(); ++i) {
    value *= maxValue + 1;
    value += array[i];
  }
  return value;
}

// The inverse function of `arrayToSingleInteger`. Maps a single integer
// `value` that is in the range `[0, ..., (maxValue + 1) ^ NumIntegers - 1
// back to an array of `NumIntegers` many integers that are each in the range
// `[0, ..., (maxValue)]`
template <size_t NumIntegers>
constexpr std::array<int, NumIntegers> integerToArray(int value, int maxValue) {
  std::array<int, NumIntegers> res;
  int numValues = maxValue + 1;
  // TODO<joka921, clang16> use views for reversion.
  for (size_t i = 0; i < res.size(); ++i) {
    res[res.size() - 1 - i] = value % numValues;
    value /= numValues;
  }
  return res;
};
}

// This function implements the main functionality.
// It calls `functor(INT<f(ints[0])>, INT<f(ints[1])>, ..., args...)`
// where `INT<N>` is `std::integral_constant<int, N>` and `f` is `mapToZeroIfTooLarge`.
template <size_t NumIntegers, int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSizeN(std::array<int, NumIntegers> ints, auto&& functor, auto&&... args) {
  static_assert(NumIntegers > 0);
  using namespace detail;
  // TODO<joka921, C++23> Use `std::bind_back`
  auto p = [](int i) { return mapToZeroIfTooLarge(i, MaxValue); };
  std::ranges::transform(ints, ints.begin(), p);
  int value = arrayToSingleInteger(ints, MaxValue);

  // Lambda: Apply the `functor` when the `ints` are given as template arguments
  // specified via a `std::integer_sequence`.
  auto applyOnIndexSequence = [&args..., functor = AD_FWD(functor) ]<int... ints>(
      std::integer_sequence<int, ints...>) mutable {
    return AD_FWD(functor)(std::integral_constant<int, ints>{}..., AD_FWD(args)...);
  };

  // Lambda: Apply the `functor` when the `ints` are given as a single compile
  // time integer `I` that was obtained via `arrayToSingleInteger`.
  auto applyOnSingleInteger = [&applyOnIndexSequence]<int I>() mutable -> decltype(auto) {
    constexpr static auto arr = integerToArray<NumIntegers>(I, MaxValue);
    return applyOnIndexSequence(ad_utility::toIntegerSequence<arr>());
  };

  // The only step that remains is to lift our single runtime `value` which
  // is in the range `[0, (MaxValue +1)^ NumIntegers]` to a compile-time
  // value and to use this compile time constant on `applyOnSingleInteger`.
  // This can be done via a single call to `callLambdaWithStaticInt`.
  return callLambdaWithStaticInt<ad_utility::pow(MaxValue + 1, NumIntegers)>(
      value, std::move(applyOnSingleInteger));
}

// Simpler named function for the case of 1 compile time integer.
template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSize1(int i, auto&& functor, auto&&... args) {
  return callFixedSizeN<1, MaxValue>(std::array{i}, AD_FWD(functor), AD_FWD(args)...);
};

// Simpler named function for the case of 2 compile time integers.
template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSize2(int i, int j, auto&& functor, auto&&... args) {
  return callFixedSizeN<2, MaxValue>(std::array{i, j}, AD_FWD(functor), AD_FWD(args)...);
}

// Simpler named function for the case of 3 compile time integers.
template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE>
decltype(auto) callFixedSize3(int i, int j, int k, auto&& functor, auto&&... args) {
  return callFixedSizeN<3, MaxValue>(std::array{i, j, k}, AD_FWD(functor), AD_FWD(args)...);
}
}  // namespace ad_utility

// The definitions of the macros for an easier syntax.
#define CALL_FIXED_SIZE_1(i, func, ...)                                        \
  ad_utility::callFixedSize1(i, [](auto I, auto&&... args) -> decltype(auto) { \
    return std::invoke(func<I>, AD_FWD(args)...);                              \
  } __VA_OPT__(, ) __VA_ARGS__)

#define CALL_FIXED_SIZE_2(i, j, func, ...)                                                \
  ad_utility::callFixedSize2(i, j, [](auto I, auto J, auto&&... args) -> decltype(auto) { \
    return std::invoke(func<I, J>, AD_FWD(args)...);                                      \
  } __VA_OPT__(, ) __VA_ARGS__)

#define CALL_FIXED_SIZE_3(i, j, k, func, ...)                                                \
  ad_utility::callFixedSize3(i, j, k,                                                        \
                             [&](auto I, auto J, auto K, auto&&... args) -> decltype(auto) { \
                               return std::invoke(func<I, J, K>, AD_FWD(args)...);           \
                             } __VA_OPT__(, ) __VA_ARGS__)
