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

// Check that for any valid `NBitInteger x` (obtained via `toNBit`),
// `toNBit(fromNBit(x))` is the identity function. Note: Calling `fromNbit` on
// an arbitrary integer is not allowed in general, since it might violate the
// invariants of `NBitInteger`.
auto testFromTo = []<size_t N>(int64_t x) {
  using I = ad_utility::NBitInteger<N>;
  auto to = I::toNBit(x);
  auto toFromTo = I::toNBit(I::fromNBit(to));
  ASSERT_EQ(to, toFromTo);
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

  constexpr static auto aBitLessThanMin =
      min - globalMin > 100 ? min - 100 : globalMin;
  constexpr static auto aBitMoreThanMin = min + 100;
  constexpr static auto aBitLessThanMax = max - 100;
  constexpr static auto aBitMoreThanMax =
      globalMax - max > 100 ? max + 100 : globalMax;

  for (auto i = aBitLessThanMin; i < aBitMoreThanMin; ++i) {
    co_yield i;
  }
  for (auto i = aBitLessThanMax; i < aBitMoreThanMax; ++i) {
    co_yield i;
  }
}

auto testUnaryFunctionNearLimits = []<size_t N>(auto unaryFunction) {
  for (auto i : valuesNearLimits<N>()) {
    unaryFunction.template operator()<N>(i);
  }
};

auto testBinaryFunctionNearLimits = []<size_t N>(auto binaryFunction) {
  for (auto i : valuesNearLimits<N>()) {
    for (auto j : valuesNearLimits<N>())
      binaryFunction.template operator()<N>(i, j);
  }
};

auto testTranslationNearLimits = []<size_t N>() {
  testUnaryFunctionNearLimits.operator()<N>(testToFrom);
  testUnaryFunctionNearLimits.operator()<N>(testFromTo);
};

template <size_t N>
void addition(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultNBit = I::fromNBit(I::toNBit(I::toNBit(a) + I::toNBit(b)));
  auto resultInt64 = I::fromNBit(I::toNBit(a + b));
  ASSERT_EQ(resultNBit, resultInt64);
}

template <size_t N>
void subtraction(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultNBit = I::fromNBit(I::toNBit(I::toNBit(a) - I::toNBit(b)));
  auto resultInt64 = I::fromNBit(I::toNBit(a - b));
  ASSERT_EQ(resultNBit, resultInt64);
}

template <size_t N>
void multiplication(int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto resultNBit = I::fromNBit(I::toNBit(I::toNBit(a) * I::toNBit(b)));
  auto resultInt64 = I::fromNBit(I::toNBit(a * b));
  ASSERT_EQ(resultNBit, resultInt64);
}

auto testNumeric = []<size_t N>(int64_t a, int64_t b) {
  addition<N>(a, b);
  subtraction<N>(a, b);
  multiplication<N>(a, b);
  // Division in general does not have an easily predictabel behaviors in the
  // presence of overflows.
};

auto testNumericNearLimits = []<size_t N>() {
  testBinaryFunctionNearLimits.operator()<N>(testNumeric);
};

void testAllN(auto function, auto... args) {
  // Call function<N>(args) for all N in 1..64.
  // Note that the `(std::make_index_sequence...)` is the argument to the
  // unnamed lambda (immediately invoked lambda). Clang format wants the
  // argument on a separate line for some reason.
  [&]<size_t... Ns>(std::index_sequence<Ns...>) {
    (..., function.template operator()<Ns + 1>(args...));
  }
  (std::make_index_sequence<64>());
}

// A generator that yields 100 values near int64_t::min(), 100 values near 0,
// 100 values near int64_t::max();
auto valuesNearCornercasesInt64 = []() -> cppcoro::generator<int64_t> {
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

TEST(NBitInteger, AllTests) {
  using N = ad_utility::NBitInteger<1>;
  ASSERT_EQ(N::min(), -1);
  ASSERT_EQ(N::max(), 0);

  testAllN(testMinMax);
  testAllN(testTranslationNearLimits);
  testAllN(testNumericNearLimits);

  for (auto i : valuesNearCornercasesInt64()) {
    testAllN(testToFrom, i);
    for (auto j : valuesNearCornercasesInt64()) {
      testAllN(testNumeric, i, j);
    }
  }
}
