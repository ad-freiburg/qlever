//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <iostream>

#include "../src/util/Generator.h"
#include "../src/util/NBitInteger.h"

auto testToFrom = []<size_t N>(int64_t x) {
  using I = ad_utility::NBitInteger<N>;
  if (x >= I::min() && x <= I::max()) {
    ASSERT_EQ(x, I::fromNBit(I::toNBit(x)));
  } else {
    ASSERT_NE(x, I::fromNBit(I::toNBit(x)));
  }
};

auto testMinMax = []<size_t N>() {
  using I = ad_utility::NBitInteger<N>;
  ASSERT_EQ(I::max(), (1ll << (N - 1)) - 1);
  ASSERT_EQ(I::min(), -1 * (1ll << (N - 1)));
};

template <size_t N>
cppcoro::generator<int64_t> valuesNearLimits() {
  using I = ad_utility::NBitInteger<N>;
  constexpr static auto min = I::min();
  constexpr static auto max = I::max();

  constexpr static auto globalMin = std::numeric_limits<int64_t>::min();
  constexpr static auto globalMax = std::numeric_limits<int64_t>::max();

  constexpr static auto lower = min - globalMin > 100 ? min - 100 : globalMin;
  constexpr static auto upper = globalMax - max > 100 ? max + 100 : globalMax;

  for (auto i = lower; i < min + 100; ++i) {
    co_yield i;
  }
  for (auto i = max - 100; i < upper; ++i) {
    co_yield i;
  }
}

auto testNearLimitsUnaryFunction = []<size_t N>(auto function) {
  for (auto i : valuesNearLimits<N>()) {
    function.template operator()<N>(i);
  }
};

auto testNearLimitsBinaryFunction = []<size_t N>(auto function) {
  for (auto i : valuesNearLimits<N>()) {
    for (auto j : valuesNearLimits<N>()) function.template operator()<N>(i, j);
  }
};

auto testTranslationNearLimits = []<size_t N>() {
  testNearLimitsUnaryFunction.operator()<N>(testToFrom);
};

template <size_t N>
void addition(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultInside = I::fromNBit(I::toNBit(I::toNBit(a) + I::toNBit(b)));
  auto resultOutside = I::fromNBit(I::toNBit(a + b));
  ASSERT_EQ(resultInside, resultOutside);
}

template <size_t N>
void subtraction(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultInside = I::fromNBit(I::toNBit(I::toNBit(a) - I::toNBit(b)));
  auto resultOutside = I::fromNBit(I::toNBit(a - b));
  ASSERT_EQ(resultInside, resultOutside);
}

template <size_t N>
void multiplication(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultInside = I::fromNBit(I::toNBit(I::toNBit(a) * I::toNBit(b)));
  auto resultOutside = I::fromNBit(I::toNBit(a * b));
  ASSERT_EQ(resultInside, resultOutside);
}

auto testNumeric = []<size_t N>(int64_t a, int64_t b) {
  addition<N>(a, b);
  subtraction<N>(a, b);
  multiplication<N>(a, b);
  // Division in general does not have an easily predictabel behaviors in the
  // presence of overflows.
};

auto testNumericNearLimits = []<size_t N>() {
  testNearLimitsBinaryFunction.operator()<N>(testNumeric);
};

void testAll(auto function, auto... args) {
  [&]<size_t... Ns>(std::index_sequence<Ns...>) {
    (..., function.template operator()<Ns + 1>(args...));
  }
  (std::make_index_sequence<64>());
}

// A generator that yields 100 values near int64_t::min(), 100 values near 0,
// 100 values near int64_t::max();
auto closeToCornercases = []() -> cppcoro::generator<int64_t> {
  int64_t max = std::numeric_limits<int64_t>::max();
  for (auto i = max - 100; i < max; ++i) {
    co_yield i + 1;
  }

  for (auto i = -100; i < 100; ++i) {
    co_yield i;
  }

  int64_t min = std::numeric_limits<int64_t>::min();
  for (auto i = min; i <= min + 100; ++i) {
    co_yield i;
  }
};

TEST(NBitInteger, SingleBit) {
  using N = ad_utility::NBitInteger<1>;
  ASSERT_EQ(N::min(), -1);
  ASSERT_EQ(N::max(), 0);

  testAll(testMinMax);
  testAll(testTranslationNearLimits);
  testAll(testNumericNearLimits);

  for (auto i : closeToCornercases()) {
    testAll(testToFrom, i);
    for (auto j : closeToCornercases()) {
      testAll(testNumeric, i, j);
    }
  }
}
