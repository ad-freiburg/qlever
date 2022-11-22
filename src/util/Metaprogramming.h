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
  for (size_t i = 0; i < exponent; ++i) {
    result *= base;
  }
  return result;
};

namespace detail {

// The implementation for the `toIntegerSequence` function (see below).
// For the ideas and alternative implementations see
// https://stackoverflow.com/questions/56799396/
template <const auto Arr,
          typename Seq = std::make_index_sequence<std::size(Arr)>>
struct ToIntegerSequenceImpl;

template <typename T, std::size_t N, const std::array<T, N> Arr,
          std::size_t... Is>
struct ToIntegerSequenceImpl<Arr, std::index_sequence<Is...>> {
  using type = std::integer_sequence<T, Arr[Is]...>;
};
}  // namespace detail

// Convert a compile-time `std::array` to a `std::integer_sequence` that
// contains the same values. This is useful because arrays can be easily created
// in constexpr functions using `normal` syntax, whereas `integer_sequences` are
// useful for working with templates that take several ints as template
// parameters. For an example usage see `CallFixedSize.h`
template <auto Array>
auto toIntegerSequence() {
  return typename detail::ToIntegerSequenceImpl<Array>::type{};
}

}  // namespace ad_utility
