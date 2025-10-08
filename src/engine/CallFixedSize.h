// Copyright 2019, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Florian Kramer (florian.kramer@mail.uni-freiburg.de)
//
// Copyright 2025, Bayerische Motoren Werke Aktiengesellschaft (BMW AG)

#ifndef QLEVER_SRC_ENGINE_CALLFIXEDSIZE_H
#define QLEVER_SRC_ENGINE_CALLFIXEDSIZE_H

#include <optional>

#include "backports/concepts.h"
#include "global/Constants.h"
#include "util/ConstexprUtils.h"
#include "util/Exception.h"
#include "util/Forward.h"
#include "util/ValueIdentity.h"

// The functions and macros are able to call functions that take a set of
// integers as template parameters with integers that are only known at runtime.
// To make this work, the possible compile time integers have to be in a range
// [0, ..., MAX] where MAX is a compile-time constant. For runtime integers that
// are > MAX, the function is called with 0 as a compile-time parameter. This
// behavior is useful for the `IdTables` (see `IdTable.h`) where `0` is a
// special value that means "the number of columns is only known at runtime".
// Note that it is relatively easy to customize this behavior such that for
// example integers that are > MAX lead to a runtime-error. This would make it
// possible to use these facilities also for a "static switch"
// TODO<joka921> Also implement such a behavior.

// There are currently two possibilities to use this interface:
// 1. Using the macros CALL_FIXED_SIZE_1, CALL_FIXED_SIZE_2 and
// CALL_FIXED_SIZE_3.
//    `CALL_FIXED_SIZE_1(i, func, additionalArguments...)` calls
//     `std::invoke(func<f(i)>, additionalArguments...)` where f(i) = i
//     <=DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE? i : 0`
//   `CALL_FIXED_SIZE_2` and `CALL_FIXED_SIZE_3` work similarly, but with 2 or 3
//   integer arguments that are converted to template parameters.
// 2. Using the templated functions `callFixedSize1`, `callFixedSize2` and
// `callFixedSize3`,
//    that all live inside the `ad_utility` namespace.
//    `callFixedSize1(i, lambda, args...)` calls
//    `lambda(std::integer_constant<int, i'>, args...)`
// The first version allows a convenient syntax for most cases.
// The second version provides a macro-free typesafe interface that can be also
// used in the corner cases where the first version fails. It also allows to
// configure the maximal value, and it is easy to implement `callFixedSize4`
// etc. for a higher number of parameters.

// For simple examples that illustrate the possibilities and limitations of both
// the macro and the template function interface, see `CallFixedSizeTest.cpp`.

namespace ad_utility {

namespace detail {

// Call the `lambda` when the correct compile-time `Int`s are given as a
// `std::integer_sequence`.
CPP_variadic_template(typename Int, Int... Is, typename F, typename... Args)(
    requires ql::concepts::integral<
        Int>) auto applyOnIntegerSequence(ad_utility::ValueSequence<Int, Is...>,
                                          F&& lambda, Args&&... args) {
  return lambda.template operator()<Is...>(AD_FWD(args)...);
};

// Internal helper functions that calls `lambda.template operator()<I,
// J,...)(args)` where `I, J, ...` are the elements in the `array`. Requires
// that each element in the `array` is `<= maxValue`.
CPP_variadic_template(int maxValue, size_t NumValues, typename Int, typename F,
                      typename... Args)(
    requires(ql::concepts::integral<
             Int>)) auto callLambdaForIntArray(std::array<Int, NumValues> array,
                                               F&& lambda, Args&&... args) {
  AD_CONTRACT_CHECK(
      ql::ranges::all_of(array, [](auto el) { return el <= maxValue; }));
  using ArrayType = std::array<Int, NumValues>;
  auto apply = [&](auto integerSequence) {
    return applyOnIntegerSequence(integerSequence, AD_FWD(lambda),
                                  AD_FWD(args)...);
  };

  // We store the result of the actual computation in a `std::optional`.
  // If the `lambda` returns void we don't store anything, but we still need
  // a type for the `result` variable. We choose `int` as a dummy for this case.
  using Result = decltype(apply(toIntegerSequence<ArrayType{}>()));
  static constexpr bool resultIsVoid = std::is_void_v<Result>;
  using Storage = std::conditional_t<resultIsVoid, int, std::optional<Result>>;
  Storage result;

  // Lambda: If the compile time parameter `I` and the runtime parameter `array`
  // are equal, then call the `lambda` with `I` as a template parameter and
  // store the result in `result` (unless it is `void`).
  auto applyIf = ApplyAsValueIdentity{[&](auto valueIdentity) {
    static constexpr auto Array = valueIdentity.value;
    const auto& seq = toIntegerSequence<Array>();
    if (array == Array) {
      if constexpr (resultIsVoid) {
        apply(seq);
      } else {
        result = apply(seq);
      }
    }
  }};

  // Lambda: call `applyIf` for all the compile-time integers `Is...`. The
  // runtime parameter always is `array`.
  auto f = [&](auto&& valueSequence) {
    forEachValueInValueSequence(AD_FWD(valueSequence), AD_FWD(applyIf));
  };

  // Call f for all combinations of compileTimeIntegers in `M x NumValues` where
  // `M` is `[0, ..., maxValue]` and `x` denotes the cartesian product of sets.
  // Exactly one of these combinations is equal to `array`, and so the lambda
  // will be executed exactly once.
  f(ad_utility::cartesianPowerAsIntegerArray<static_cast<Int>(maxValue + 1),
                                             NumValues>());

  if constexpr (!resultIsVoid) {
    return std::move(result.value());
  }
}

// Overload for a single int.
CPP_variadic_template(int maxValue, typename Int, typename F,
                      typename... Args)(requires(
    ql::concepts::integral<Int>)) auto callLambdaForIntArray(Int i, F&& lambda,
                                                             Args&&... args) {
  return callLambdaForIntArray<maxValue>(std::array{i}, AD_FWD(lambda),
                                         AD_FWD(args)...);
}

// The default function that maps `x` to the range `[0, ..., maxValue]`
constexpr int mapToZeroIfTooLarge(int x, int maxValue) {
  return x <= maxValue ? x : 0;
}
}  // namespace detail

// This function implements the main functionality.
// It calls `functor(INT<f(ints[0])>, INT<f(ints[1])>, ..., args...)`
// where `INT<N>` is `std::integral_constant<int, N>` and `f` is
// `mapToZeroIfTooLarge`.
CPP_variadic_template(int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE,
                      size_t NumIntegers, typename Int, typename F,
                      typename... Args)(
    requires std::is_integral_v<Int>) decltype(auto)
    callFixedSize(std::array<Int, NumIntegers> ints, F&& functor,
                  Args&&... args) {
  static_assert(NumIntegers > 0);
  // TODO<joka921, C++23> Use `std::bind_back`
  auto p = [](int i) { return detail::mapToZeroIfTooLarge(i, MaxValue); };
  ql::ranges::transform(ints, ints.begin(), p);

  // The only step that remains is to lift our single runtime `value` which
  // is in the range `[0, (MaxValue +1)^ NumIntegers]` to a compile-time
  // value and to use this compile time constant on `applyOnSingleInteger`.
  // This can be done via a single call to `callLambdaForIntArray`.
  return detail::callLambdaForIntArray<MaxValue>(ints, AD_FWD(functor),
                                                 AD_FWD(args)...);
}

// Overload for a single integer.
CPP_variadic_template(int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE,
                      typename Int, typename F, typename... Args)(
    requires std::is_integral_v<Int>) decltype(auto)
    callFixedSize(Int i, F&& functor, Args&&... args) {
  return callFixedSize<MaxValue>(std::array{i}, AD_FWD(functor),
                                 AD_FWD(args)...);
}

// Template function `callFixedSizeVi` is a wrapper around `callFixedSize`.
// It wraps the functor in an `ApplyAsValueIdentity`, passing the integers
// as ValueIdentity objects to the functor rather than as template parameters.
CPP_variadic_template(int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE,
                      size_t NumIntegers, typename Int, typename F,
                      typename... Args)(
    requires std::is_integral_v<Int>) decltype(auto)
    callFixedSizeVi(std::array<Int, NumIntegers> ints, F&& functor,
                    Args&&... args) {
  return callFixedSize<MaxValue>(ints, ApplyAsValueIdentity{AD_FWD(functor)},
                                 AD_FWD(args)...);
}

// Overload for a single integer.
CPP_variadic_template(int MaxValue = DEFAULT_MAX_NUM_COLUMNS_STATIC_ID_TABLE,
                      typename Int, typename F, typename... Args)(
    requires std::is_integral_v<Int>) decltype(auto)
    callFixedSizeVi(Int i, F&& functor, Args&&... args) {
  return callFixedSizeVi<MaxValue>(std::array{i}, AD_FWD(functor),
                                   AD_FWD(args)...);
}

}  // namespace ad_utility

// The definitions of the macro for an easier syntax.
// The first argument (an array of integers) has to be put in parentheses if
// it is directly instantiated in the call, for example
// `CALL_FIXED_SIZE((std::array{1, 2}), &funcThatTakesTwoIntegers, argument);`
// This is necessary because macros do not correctly parse the curly braces.
// TODO<joka921, C++23> In C++23 `std::array` and other aggregates can be
// initialized with parentheses such that we can write
// `CALL_FIXED_SIZE(std::array(1, 2), ...`. For a single integer you can also
// simply write `CALL_FIXED_SIZE(1, function, params)`, this is handled by
// the corresponding overload of `ad_utility::callFixedSize`.
#define CALL_FIXED_SIZE(integers, func, ...)                 \
  ad_utility::callFixedSize(                                 \
      integers,                                              \
      ad_utility::ApplyAsValueIdentityTuple(                 \
          [](auto argsTuple, auto... Is) -> decltype(auto) { \
            return std::apply(func<static_cast<int>(Is)...>, \
                              std::move(argsTuple));         \
          }),                                                \
      ##__VA_ARGS__)

#endif  // QLEVER_SRC_ENGINE_CALLFIXEDSIZE_H
