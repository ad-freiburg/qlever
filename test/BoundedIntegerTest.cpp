//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <iostream>

#include "../src/util/BoundedInteger.h"
#include "../src/util/Generator.h"

auto testChange = []<size_t N>(int64_t x) {
  using I = ad_utility::NBitInteger<N>;
  if (x >= I::MinInteger() && x <= I::MaxInteger()) {
    ASSERT_EQ(x, I::fromNBit(I::toNBit(x)));
  } else {
    ASSERT_NE(x, I::fromNBit(I::toNBit(x)));
  }
};

template <size_t N>
cppcoro::generator<int64_t> valuesNearLimits() {
  using I = ad_utility::NBitInteger<N>;
  constexpr static auto min = I::MinInteger();
  constexpr static auto max = I::MaxInteger();

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

auto testNearLimitsUnary = []<size_t N>(auto function) {
  for (auto i : valuesNearLimits<N>()) {
    function.template operator()<N>(i);
  }
};

auto testNearLimitsBinary = []<size_t N>(auto function) {
  for (auto i : valuesNearLimits<N>()) {
    for (auto j : valuesNearLimits<N>()) function.template operator()<N>(i, j);
  }
};

auto testChangeNearLimits = []<size_t N>() {
  testNearLimitsUnary.operator()<N>(testChange);
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
};

auto testNumericNearLimits = []<size_t N>() {
  testNearLimitsBinary.operator()<N>(testNumeric);
};

template <size_t... Ns>
void testAllImpl(std::index_sequence<Ns...>, auto function, auto... args) {
  (..., function.template operator()<Ns + 1>(args...));
}

void testAll(auto function, auto... args) {
  testAllImpl(std::make_index_sequence<64>(), function, args...);
}

auto closeToCornercases = []() -> cppcoro::generator<int64_t> {
  int64_t lower = std::numeric_limits<int64_t>::max() - 100;
  int64_t upper = std::numeric_limits<int64_t>::max();

  for (auto i = lower - 1; i < upper; ++i) {
    co_yield i + 1;
  }

  for (auto i = -100; i < 100; ++i) {
    co_yield i;
  }

  lower = std::numeric_limits<int64_t>::min();
  upper = lower + 101;

  for (auto i = lower; i <= upper; ++i) {
    co_yield i;
  }
};

TEST(NBitInteger, SingleBit) {
  using N = ad_utility::NBitInteger<1>;
  ASSERT_EQ(N::MinInteger(), -1);
  ASSERT_EQ(N::MaxInteger(), 0);

  testAll(testChangeNearLimits);
  testAll(testNumericNearLimits);

  for (auto i : closeToCornercases()) {
    testAll(testChange, i);
    for (auto j : closeToCornercases()) {
      testAll(testNumeric, i, j);
    }
  }
}
