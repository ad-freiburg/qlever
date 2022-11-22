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
consteval auto pow(auto base, int exponent) {
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
  return std::integer_sequence<int, std::get<indexes>(Array)...>{};
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

}  // namespace ad_utility
