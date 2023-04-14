//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <utility>

#include "gtest/gtest.h"
#include "util/ConstexprUtils.h"

using namespace ad_utility;

TEST(ConstexprUtils, pow) {
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

TEST(ConstexprUtils, toIntegerSequence) {
  ASSERT_TRUE(compare(std::integer_sequence<int>{},
                      (toIntegerSequence<std::array<int, 0>{}>())));
  ASSERT_TRUE(compare(std::integer_sequence<int, 3, 2>{},
                      (toIntegerSequence<std::array{3, 2}>())));
  ASSERT_TRUE(compare(std::integer_sequence<int, -12>{},
                      (toIntegerSequence<std::array{-12}>())));
  ASSERT_TRUE(compare(std::integer_sequence<int, 5, 3, 3, 4, -1>{},
                      (toIntegerSequence<std::array{5, 3, 3, 4, -1}>())));

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

TEST(ConstexprUtils, cartesianPowerAsArray) {
  std::array<std::array<int, 1>, 4> a{std::array{0}, std::array{1},
                                      std::array{2}, std::array{3}};
  ASSERT_EQ(a, (cartesianPowerAsArray<4, 1>()));

  std::array<std::array<int, 2>, 4> b{std::array{0, 0}, std::array{0, 1},
                                      std::array{1, 0}, std::array{1, 1}};
  ASSERT_EQ(b, (cartesianPowerAsArray<2, 2>()));

  std::array<std::array<int, 3>, 8> c{std::array{0, 0, 0}, std::array{0, 0, 1},
                                      std::array{0, 1, 0}, std::array{0, 1, 1},
                                      std::array{1, 0, 0}, std::array{1, 0, 1},
                                      std::array{1, 1, 0}, std::array{1, 1, 1}};
  ASSERT_EQ(c, (cartesianPowerAsArray<2, 3>()));
}

TEST(ConstexprUtils, cartesianPowerAsIntegerArray) {
  std::integer_sequence<std::array<int, 1>, std::array{0}, std::array{1},
                        std::array{2}, std::array{3}>
      a;
  ASSERT_TRUE(compare(a, (cartesianPowerAsIntegerArray<4, 1>())));

  std::integer_sequence<std::array<int, 2>, std::array{0, 0}, std::array{0, 1},
                        std::array{1, 0}, std::array{1, 1}>
      b;
  ASSERT_TRUE(compare(b, (cartesianPowerAsIntegerArray<2, 2>())));

  std::integer_sequence<
      std::array<int, 3>, std::array{0, 0, 0}, std::array{0, 0, 1},
      std::array{0, 1, 0}, std::array{0, 1, 1}, std::array{1, 0, 0},
      std::array{1, 0, 1}, std::array{1, 1, 0}, std::array{1, 1, 1}>
      c;
  ASSERT_TRUE(compare(c, (cartesianPowerAsIntegerArray<2, 3>())));
}

TEST(ConstexprUtils, ConstexprForLoop) {
  size_t i{0};

  // Add `i` up to one hundred.
  ConstexprForLoop(std::make_index_sequence<100>{}, [&i]<size_t>() { i++; });
  ASSERT_EQ(i, 100);

  // Add up 2, 5, and 9 at run time.
  i = 0;
  ConstexprForLoop(std::index_sequence<2, 5, 9>{},
                   [&i]<size_t NumberToAdd>() { i += NumberToAdd; });
  ASSERT_EQ(i, 16);

  // Shouldn't do anything, because the index sequence is empty.
  i = 0;
  ConstexprForLoop(std::index_sequence<>{},
                   [&i]<size_t NumberToAdd>() { i += NumberToAdd; });
  ASSERT_EQ(i, 0);
}

TEST(ConstexprUtils, RuntimeValueToCompileTimeValue) {
  // Create one function, that sets `i` to x, for every possible
  // version of x in [0,100].
  size_t i = 1;
  auto setI = [&i]<size_t Number>() { i = Number; };
  for (size_t d = 0; d <= 100; d++) {
    RuntimeValueToCompileTimeValue<100>(d, setI);
    ASSERT_EQ(i, d);
  }

  // Should cause an exception, if the given value is bigger than the
  // `MaxValue`.
  ASSERT_ANY_THROW(RuntimeValueToCompileTimeValue<5>(10, setI));
}
