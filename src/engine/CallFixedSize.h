// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)

#pragma once

#include <functional>
#include <optional>

#include "global/Constants.h"
#include "util/ConstexprUtils.h"
#include "util/DisableWarningsClang13.h"
#include "util/Exception.h"
#include "util/Forward.h"

// The functions and macros are able to call functions that take a set of integers as
// template parameters with integers that are only known at runtime. To make this
// work, the possible compile time integers have to be in a range [0, ..., MAX] where
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
// the macro and the template function interface, see `CallFixedSizeTest.cpp`.

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
  using Storage = std::conditional_t<resultIsVoid, int, std::optional<Result>>;
  Storage result;
  // Lambda: If the compile time parameter `I` and the runtime parameter `i`
  // are equal, then call the `lambda` with `I` as a template parameter and
  // store the result in `result` (unless it is `void`).
  DISABLE_WARNINGS_CLANG_13
  auto applyIf = [&result, lambda = AD_FWD(lambda), ... args = AD_FWD(args)]<int I>(int i) mutable {
    ENABLE_WARNINGS_CLANG_13
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
template <std::integral Int, size_t NumIntegers>
requires requires { NumIntegers > 0; }
int arrayToSingleInteger(const std::array<Int, NumIntegers>& array, const int maxValue) {
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
template <std::integral Int, size_t NumIntegers>
constexpr std::array<Int, NumIntegers> integerToArray(Int value, Int maxValue) {
  std::array<Int, NumIntegers> res;
  Int numValues = maxValue + 1;
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
template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE, size_t NumIntegers,
          std::integral Int>
decltype(auto) callFixedSize(std::array<Int, NumIntegers> ints, auto&& functor, auto&&... args) {
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
    return AD_FWD(functor).template operator()<ints...>(AD_FWD(args)...);
  };

  // Lambda: Apply the `functor` when the `ints` are given as a single compile
  // time integer `I` that was obtained via `arrayToSingleInteger`.
  DISABLE_WARNINGS_CLANG_13
  auto applyOnSingleInteger = [&applyOnIndexSequence]<int I>() mutable -> decltype(auto) {
    ENABLE_WARNINGS_CLANG_13
    constexpr static auto arr = integerToArray<Int, NumIntegers>(I, MaxValue);
    return applyOnIndexSequence(ad_utility::toIntegerSequence<arr>());
  };

  // The only step that remains is to lift our single runtime `value` which
  // is in the range `[0, (MaxValue +1)^ NumIntegers]` to a compile-time
  // value and to use this compile time constant on `applyOnSingleInteger`.
  // This can be done via a single call to `callLambdaWithStaticInt`.
  return callLambdaWithStaticInt<ad_utility::pow(MaxValue + 1, NumIntegers)>(
      value, std::move(applyOnSingleInteger));
}

}  // namespace ad_utility

// The definitions of the macro for an easier syntax.
#define CALL_FIXED_SIZE(integers, func, ...)                    \
  ad_utility::callFixedSize(                                    \
      integers, []<int... Is>(auto&&... args)->decltype(auto) { \
        return std::invoke(func<Is...>, AD_FWD(args)...);       \
      } __VA_OPT__(, ) __VA_ARGS__)
