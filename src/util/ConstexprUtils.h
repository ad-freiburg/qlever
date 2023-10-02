//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <ranges>

#include "util/Exception.h"
#include "util/Forward.h"

// Various helper functions for compile-time programming.

namespace ad_utility {

// Compute `base ^ exponent` where `^` denotes exponentiation. This is consteval
// because for all runtime calls, a better optimized algorithm from the standard
// library should be chosen.
// TODO<joka921> why can't this be consteval when the result is boudn to a
// `constexpr` variable?
constexpr auto pow(auto base, int exponent) {
  if (exponent < 0) {
    throw std::runtime_error{"negative exponent"};
  }
  decltype(base) result = 1;
  for (int i = 0; i < exponent; ++i) {
    result *= base;
  }
  return result;
};

/*
 * @brief A compile time for loop, which passes the loop index to the
 *  given loop body.
 *
 * @tparam Function The loop body should be a templated function, with one
 *  size_t template argument and no more. It also shouldn't take any function
 *  arguments. Should be passed per deduction.
 * @tparam ForLoopIndexes The indexes, that the for loop goes over. Should be
 *  passed per deduction.
 *
 * @param loopBody The body of the for loop.
 */
template <typename Function, size_t... ForLoopIndexes>
void ConstexprForLoop(const std::index_sequence<ForLoopIndexes...>&,
                      const Function& loopBody) {
  ((loopBody.template operator()<ForLoopIndexes>()), ...);
}

// A `constexpr` switch statement. Chooses the `MatchingCase` in `FirstCase,
// Cases...` that is equal to `value` and then calls
// `function.operator()<MatchingCase>(args...)`.
template <auto FirstCase, std::same_as<decltype(FirstCase)> auto... Cases>
auto ConstexprSwitch =
    [](auto&& function,
       const std::equality_comparable_with<decltype(FirstCase)> auto& value,
       auto&&... args) -> decltype(auto) requires(requires {
  AD_FWD(function).template operator()<FirstCase>(AD_FWD(args)...);
} && (... &&
      requires {
        AD_FWD(function).template operator()<Cases>(AD_FWD(args)...);
      })) {
  if (value == FirstCase) {
    return AD_FWD(function).template operator()<FirstCase>(AD_FWD(args)...);
  } else if constexpr (sizeof...(Cases) > 0) {
    return ConstexprSwitch<Cases...>(AD_FWD(function), value, AD_FWD(args)...);
  } else {
    AD_FAIL();
  }
};

/*
 * @brief 'Converts' a run time value of `size_t` to a compile time value and
 * then calls `function.template operator()<value>()`. `value < MaxValue` must
 * be true, else an exception is thrown. *
 *
 * @tparam MaxValue The maximal value, that the function parameter value could
 *  take.
 * @tparam Function The given function should be a templated function, with one
 *  size_t template argument and no more. It also shouldn't take any function
 *  arguments. This parameter should be passed per deduction.
 *
 * @param value Value that you need as a compile time value.
 * @param function The templated function, which you wish to execute. Must be
 *  a function object (for example a lambda expression) that has an
 *  `operator()` which is templated on a single `size_t`.
 */
template <size_t MaxValue, typename Function>
void RuntimeValueToCompileTimeValue(const size_t& value, Function function) {
  AD_CONTRACT_CHECK(value <= MaxValue);  // Is the value valid?
  ConstexprForLoop(std::make_index_sequence<MaxValue + 1>{},
                   [&function, &value]<size_t Index>() {
                     if (Index == value) {
                       function.template operator()<Index>();
                     }
                   });
}

/*
@brief Returns the index of the first given type, that passes the given check.

@tparam checkFunction A `constexpr` function, that takes one template
parameter and return a bool.
@tparam Args The list of types, that should be checked over.

@return An index out of `[0, sizeof...(Args))`, if there was type, that passed
the check. Otherwise, returns `sizeof...(Args)`.
*/
template <auto checkFunction, typename... Args>
constexpr size_t getIndexOfFirstTypeToPassCheck() {
  size_t index = 0;

  auto l = [&index]<typename T>() {
    if constexpr (checkFunction.template operator()<T>()) {
      return true;
    } else {
      ++index;
      return false;
    }
  };

  ((l.template operator()<Args>()) || ...);

  return index;
}

// An `ad_utility::ValueSequence<T, values...>` has the same functionality as
// `std::integer_sequence`. This replacement is needed to compile QLever with
// libc++, because libc++ strictly enforces the `std::integral` constraint for
// `std::integer_sequence`, and we also need non-integral types as values, for
// example `std::array<...>`.
namespace detail {
template <typename T, T... values>
struct ValueSequenceImpl {};
};  // namespace detail

template <typename T, T... values>
using ValueSequence = detail::ValueSequenceImpl<T, values...>;

namespace detail {
// The implementation for the `toIntegerSequence` function (see below).
// For the ideas and alternative implementations see
// https://stackoverflow.com/questions/56799396/
template <auto Array, size_t... indexes>
constexpr auto toIntegerSequenceHelper(std::index_sequence<indexes...>) {
  return ValueSequence<typename decltype(Array)::value_type,
                       std::get<indexes>(Array)...>{};
}
}  // namespace detail

// Convert a compile-time `std::array` to a `ValueSequence` that
// contains the same values. This is useful because arrays can be easily created
// in constexpr functions using `normal` syntax, whereas `ValueSequence`s are
// useful for working with templates that take several ints as template
// parameters. For an example usage see `CallFixedSize.h`
template <auto Array>
auto toIntegerSequence() {
  return detail::toIntegerSequenceHelper<Array>(
      std::make_index_sequence<Array.size()>{});
  // return typename detail::ToIntegerSequenceImpl<Array>::type{};
}

// Map a single integer `value` that is in the range `[0, ..., (maxValue + 1) ^
// NumIntegers - 1 to an array of `NumIntegers` many integers that are each in
// the range
// `[0, ..., (maxValue)]`
template <std::integral Int, size_t NumIntegers>
constexpr std::array<Int, NumIntegers> integerToArray(Int value,
                                                      Int numValues) {
  std::array<Int, NumIntegers> res;
  for (auto& el : res | std::views::reverse) {
    el = value % numValues;
    value /= numValues;
  }
  return res;
};

// Return a `std::array<std::array<Int, Num>, pow(Upper, Num)>` (where `Int` is
// the type of `Upper`) that contains each
// value from `[0, ..., Upper - 1] ^ Num` exactly once. `^` denotes the
// cartesian power.
template <std::integral auto Upper, size_t Num>
constexpr auto cartesianPowerAsArray() {
  using Int = decltype(Upper);
  constexpr auto numValues = pow(Upper, Num);
  std::array<std::array<Int, Num>, numValues> arr;
  for (Int i = 0; i < numValues; ++i) {
    arr[i] = integerToArray<Int, Num>(i, Upper);
  }
  return arr;
}

// Return a `std::integer_sequence<Int,...>` that contains each
// value from `[0, ..., Upper - 1] X Num` exactly once. `X` denotes the
// cartesian product of sets. The elements of the `integer_sequence` are
// of type `std::array<Int, Num>` where `Int` is the type of `Upper`.
template <std::integral auto Upper, size_t Num>
auto cartesianPowerAsIntegerArray() {
  return toIntegerSequence<cartesianPowerAsArray<Upper, Num>()>();
}

}  // namespace ad_utility
