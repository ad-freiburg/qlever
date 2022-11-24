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

template <int maxValue, size_t NumValues, std::integral Int>
auto callLambdaWithStaticInt2(std::array<Int, NumValues> i, auto&& lambda, auto&&... args) {
  AD_CHECK(std::ranges::all_of(i, [](auto el) { return el <= maxValue; }));
  using Arr = std::array<Int, NumValues>;

  // Call the `lambda` when the correct compile-time `Int`s are given as a
  // `std::integer_sequence`.
  auto applyOnIntegerSequence = [&]<Int... Is>(std::integer_sequence<Int, Is...>) {
    return lambda.template operator()<Is...>(AD_FWD(args)...);
  };

  // We store the result of the actual computation in a `std::optional`.
  // If the `lambda` returns void we don't store anything, but we still need
  // a type for the `result` variable. We choose `int` as a dummy for this case.
  using Result = decltype(applyOnIntegerSequence(ad_utility::toIntegerSequence<Arr{}>()));
  static constexpr bool resultIsVoid = std::is_void_v<Result>;
  using Storage = std::conditional_t<resultIsVoid, int, std::optional<Result>>;
  Storage result;

  // Lambda: If the compile time parameter `I` and the runtime parameter `i`
  // are equal, then call the `lambda` with `I` as a template parameter and
  // store the result in `result` (unless it is `void`).
  DISABLE_WARNINGS_CLANG_13
  auto applyIf = [&applyOnIntegerSequence, &i, &result, lambda = AD_FWD(lambda),
                  &args...]<Arr I>() mutable {
    ENABLE_WARNINGS_CLANG_13
    if (i == I) {
      if constexpr (resultIsVoid) {
        applyOnIntegerSequence(ad_utility::toIntegerSequence<I>());
      } else {
        result = applyOnIntegerSequence(ad_utility::toIntegerSequence<I>());
      }
    }
  };

  // Lambda: call `applyIf` for all the compile-time integers `Is...`. The
  // runtime parameter always is `i`.
  auto f = [&applyIf]<Arr... Is>(std::integer_sequence<Arr, Is...>) {
    (..., applyIf.template operator()<Is>());
  };

  // Call f for all combinations of compileTimeIntegers in `M x NumValues` where
  // `M` is `[0, ..., maxValue]` and `x` denotes the cartesian product of sets.
  // Exactly one of these combinations is equal to `i`, and so the lambda will
  // be executed exactly once.
  f(ad_utility::toIntegerSequenceCartesianProductEtc<static_cast<Int>(maxValue + 1), NumValues>());

  if constexpr (!resultIsVoid) {
    return std::move(result.value());
  }
}

// The default function that maps `x` to the range `[0, ..., maxValue]`
constexpr int mapToZeroIfTooLarge(int x, int maxValue) { return x <= maxValue ? x : 0; }
}

// This function implements the main functionality.
// It calls `functor(INT<f(ints[0])>, INT<f(ints[1])>, ..., args...)`
// where `INT<N>` is `std::integral_constant<int, N>` and `f` is `mapToZeroIfTooLarge`.
template <int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE, size_t NumIntegers,
          std::integral Int>
decltype(auto) callFixedSize(std::array<Int, NumIntegers> ints, auto&& functor, auto&&... args) {
  static_assert(NumIntegers > 0);
  // TODO<joka921, C++23> Use `std::bind_back`
  auto p = [](int i) { return detail::mapToZeroIfTooLarge(i, MaxValue); };
  std::ranges::transform(ints, ints.begin(), p);

  // The only step that remains is to lift our single runtime `value` which
  // is in the range `[0, (MaxValue +1)^ NumIntegers]` to a compile-time
  // value and to use this compile time constant on `applyOnSingleInteger`.
  // This can be done via a single call to `callLambdaWithStaticInt`.
  return detail::callLambdaWithStaticInt2<MaxValue>(ints, AD_FWD(functor), AD_FWD(args)...);
}

}  // namespace ad_utility

// The definitions of the macro for an easier syntax.
#define CALL_FIXED_SIZE(integers, func, ...)                    \
  ad_utility::callFixedSize(                                    \
      integers, []<int... Is>(auto&&... args)->decltype(auto) { \
        return std::invoke(func<Is...>, AD_FWD(args)...);       \
      } __VA_OPT__(, ) __VA_ARGS__)
