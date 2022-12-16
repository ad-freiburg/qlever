//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include "util/Exception.h"

// Various helper functions for compile-time programming

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

namespace detail {

// The implementation for the `toIntegerSequence` function (see below).
// For the ideas and alternative implementations see
// https://stackoverflow.com/questions/56799396/
template <auto Array, size_t... indexes>
constexpr auto toIntegerSequenceHelper(std::index_sequence<indexes...>) {
  return std::integer_sequence<typename decltype(Array)::value_type,
                               std::get<indexes>(Array)...>{};
}
}  // namespace detail

// Convert a compile-time `std::array` to a `std::integer_sequence` that
// contains the same values. This is useful because arrays can be easily created
// in constexpr functions using `normal` syntax, whereas `integer_sequences` are
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
  // TODO<joka921, clang16> use views for reversion.
  for (size_t i = 0; i < res.size(); ++i) {
    res[res.size() - 1 - i] = value % numValues;
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
