// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <random>

#include "util/ComparisonOfIntAndDouble.h"

using ad_utility::compareIntAndDouble;

namespace {
constexpr int64_t twoToThe53 = int64_t{1} << 53;
constexpr double twoToThe63 = 9223372036854775808.0;
constexpr double inf = std::numeric_limits<double>::infinity();
constexpr int64_t minInt = std::numeric_limits<int64_t>::min();
constexpr int64_t maxInt = std::numeric_limits<int64_t>::max();

// The sign of the result of `compareIntAndDouble`.
int sign(int64_t i, double d) {
  int result = compareIntAndDouble(i, d);
  return (result > 0) - (result < 0);
}
}  // namespace

// _____________________________________________________________________________
TEST(ComparisonOfIntAndDouble, compareIntAndDouble) {
  EXPECT_EQ(sign(0, 0.0), 0);
  EXPECT_EQ(sign(0, -0.0), 0);
  EXPECT_EQ(sign(0, 0.5), -1);
  EXPECT_EQ(sign(0, -0.5), 1);
  EXPECT_EQ(sign(1, 0.5), 1);
  EXPECT_EQ(sign(-1, -0.5), -1);
  EXPECT_EQ(sign(-1, -1.5), 1);
  EXPECT_EQ(sign(5, 5.0), 0);
  EXPECT_EQ(sign(-5, -5.0), 0);

  // Above `2^53`, where the built-in comparison is wrong.
  auto big = static_cast<double>(twoToThe53);
  EXPECT_EQ(static_cast<double>(twoToThe53 + 1), big);
  EXPECT_EQ(sign(twoToThe53, big), 0);
  EXPECT_EQ(sign(twoToThe53 + 1, big), 1);
  EXPECT_EQ(sign(twoToThe53 - 1, big), -1);
  EXPECT_EQ(sign(-twoToThe53 - 1, -big), -1);
  EXPECT_EQ(sign(twoToThe53 + 1, big + 2), -1);

  // The limits of `int64_t`.
  EXPECT_EQ(sign(maxInt, twoToThe63), -1);
  EXPECT_EQ(sign(maxInt, std::nextafter(twoToThe63, 0.0)), 1);
  EXPECT_EQ(sign(minInt, -twoToThe63), 0);
  EXPECT_EQ(sign(minInt, std::nextafter(-twoToThe63, -inf)), 1);
  EXPECT_EQ(sign(minInt + 1, -twoToThe63), 1);
  EXPECT_EQ(sign(maxInt, inf), -1);
  EXPECT_EQ(sign(minInt, -inf), 1);
  EXPECT_EQ(sign(0, std::numeric_limits<double>::denorm_min()), -1);
  EXPECT_EQ(sign(0, -std::numeric_limits<double>::denorm_min()), 1);
}

// Compare random integers and nearby doubles against `long double`, which
// represents both exactly if its mantissa has at least 64 bits.
TEST(ComparisonOfIntAndDouble, compareIntAndDoubleRandom) {
  if constexpr (std::numeric_limits<long double>::digits < 64) {
    GTEST_SKIP() << "`long double` can't represent every `int64_t`";
  }
  std::mt19937_64 rng{42};
  // Shift by at least one bit, such that `-i` below can't overflow.
  std::uniform_int_distribution<int> numBits{1, 63};
  std::uniform_int_distribution<int> offset{-3, 3};
  for (size_t k = 0; k < 100'000; ++k) {
    auto i = static_cast<int64_t>(rng() >> numBits(rng));
    if (rng() % 2) {
      i = -i;
    }
    double d = static_cast<double>(i);
    for (int j = offset(rng); j != 0; j += (j > 0 ? -1 : 1)) {
      d = std::nextafter(d, j > 0 ? inf : -inf);
    }
    auto li = static_cast<long double>(i);
    auto ld = static_cast<long double>(d);
    int expected = (li > ld) - (li < ld);
    ASSERT_EQ(sign(i, d), expected) << i << ' ' << d;
  }
}
