//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "gtest/gtest.h"
#include "util/Metaprogramming.h"

using namespace ad_utility;

TEST(Metaprogramming, pow) {
  static_assert(pow(0, 0) == 1);
  static_assert(pow(0.0, 0) == 1);
  static_assert(pow(0, 1) == 0);
  static_assert(pow(0.0, 1) == 0);
  static_assert(pow(0, 15) == 0);
  static_assert(pow(0.0, 15) == 0);
  static_assert(pow(1, 0) == 1);
  static_assert(pow(1.0, 0) == 1);
  static_assert(pow(15, 0) == 1);
  static_assert(pow(15.0, 0) == 1);

  static_assert(pow(1, 12) == 1);
  static_assert(pow(1.0, 12) == 1);

  static_assert(pow(2, 10) == 1024);
  static_assert(pow(1.5, 4) == 1.5 * 1.5 * 1.5 * 1.5);

  static_assert(pow(-1, 2) == 1);
  static_assert(pow(-1, 3) == -1);

  static_assert(pow(-2, 2) == 4);
  static_assert(pow(-2, 3) == -8);
}

// Easier testing for equality of `std::integer_sequence`. They must have the
// same underlying type and the same values.
// Note: we cannot call this `operator==` because the operator would only be
// found by argument-dependent lookup if it was in the `std::` namespace, and
// adding functions there is by default forbidden.
template <typename T, typename U, T... Ts, U... Us>
bool compare(const std::integer_sequence<T, Ts...>&,
             const std::integer_sequence<U, Us...>&) {
  if constexpr (std::is_same_v<U, T> && sizeof...(Us) == sizeof...(Ts)) {
    return std::array<T, sizeof...(Ts)>{Ts...} ==
           std::array<U, sizeof...(Us)>{Us...};
  } else {
    return false;
  }
}

TEST(Metaprogramming, toIntegerSequence) {
  ASSERT_TRUE(compare(std::integer_sequence<int>{},
                      (toIntegerSequence<std::array<int, 0>{}>())));
  ASSERT_TRUE(compare(std::integer_sequence<int, 3, 2>{},
                      (toIntegerSequence<std::array{3, 2}>())));
  ASSERT_TRUE(compare(std::integer_sequence<int, -12>{},
                      (toIntegerSequence<std::array{-12}>())));
  ASSERT_TRUE(compare(std::integer_sequence<int, 5, 4, 3, 2, 1>{},
                      (toIntegerSequence<std::array{5, 4, 3, 2, 1}>())));

  // Mismatching types.
  ASSERT_FALSE(compare(std::integer_sequence<float>{},
                       (toIntegerSequence<std::array<int, 0>{}>())));
  ASSERT_FALSE(compare(std::integer_sequence<unsigned, 5, 4>{},
                       (toIntegerSequence<std::array{5, 4}>())));

  // Mismatching values
  ASSERT_FALSE(compare(std::integer_sequence<int, 3, 2>{},
                       (toIntegerSequence<std::array{3, 3}>())));
  ASSERT_FALSE(compare(std::integer_sequence<int, -12>{},
                       (toIntegerSequence<std::array{-12, 4}>())));
  ASSERT_FALSE(compare(std::integer_sequence<int, -12, 4>{},
                       (toIntegerSequence<std::array{-12}>())));
}