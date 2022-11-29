//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <iostream>

#include "../src/util/Generator.h"
#include "../src/util/NBitInteger.h"

// Enabling cheaper unit tests when building in Debug mode
#ifdef QLEVER_RUN_EXPENSIVE_TESTS
static constexpr int numElements = 100;
#else
static constexpr int numElements = 5;
#endif

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
  if (N == 64) {
    ASSERT_EQ(I::max(), std::numeric_limits<int64_t>::max());
    ASSERT_EQ(I::min(), std::numeric_limits<int64_t>::min());
  } else {
    ASSERT_EQ(I::max(), (1ll << (N - 1)) - 1);
    ASSERT_EQ(I::min(), -1 * (1ll << (N - 1)));
  }
};

template <size_t N>
cppcoro::generator<int64_t> valuesNearLimits() {
  using I = ad_utility::NBitInteger<N>;
  constexpr static auto min = I::min();
  constexpr static auto max = I::max();

  constexpr static auto globalMin = std::numeric_limits<int64_t>::min();
  constexpr static auto globalMax = std::numeric_limits<int64_t>::max();

  constexpr static auto aBitLessThanMin =
      min - globalMin > numElements ? min - numElements : globalMin;
  constexpr static auto aBitMoreThanMin = min + numElements;
  constexpr static auto aBitLessThanMax = max - numElements;
  constexpr static auto aBitMoreThanMax =
      globalMax - max > numElements ? max + numElements : globalMax;

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

// Test that the result of `from(to(f(a, b)))` is equal to `from(to(f(to(a),
// to(b))))`, when `to` is `NBitInteger<N>::toNBit` and `from` is the
// corresponding `fromNBit`. This tests the (well-defined) behavior of the
// `NBitInteger`s in the presence of overflows. The `wouldOverflow(a, b)`
// function must return `true` if `f(a, b)` would overlow, leading to undefined
// behavior. In those cases, the result `i(f(u(a), u(b))` is evaluated instead
// of `f(a, b)`, where `i` is a cast to int64_t and `u` is a cast to `uint64_t`.
// The behavior of this operation is well-defined because unsigned integer
// overflow is defined and signed-unsigned conversions are defined (signed
// integers are 2s complement).
template <size_t N, auto f, auto wouldOverflow>
auto testTwoNumbers = [](int64_t a, int64_t b) {
  using I = ad_utility::NBitInteger<N>;
  auto aU = static_cast<uint64_t>(a);
  auto bU = static_cast<uint64_t>(b);
  auto resultNBit = I::fromNBit(I::toNBit(f(I::toNBit(a), I::toNBit(b))));
  auto resultInt64 =
      wouldOverflow(a, b)
          ? I::fromNBit(I::toNBit(static_cast<int64_t>(f(aU, bU))))
          : I::fromNBit(I::toNBit(f(a, b)));
  ASSERT_EQ(resultNBit, resultInt64);
};

// Return true if `a + b` would overflow.
bool additionWouldOverflow(int64_t a, int64_t b) {
  if ((a < 0) != (b < 0)) {
    return false;
  }
  if (a == 0 || b == 0) {
    return false;
  }
  auto intMin = std::numeric_limits<int64_t>::min();
  if (a == intMin || b == intMin) {
    return true;
  }
  auto abs = [](int64_t x) { return static_cast<uint64_t>(x < 0 ? -x : x); };
  // TODO<joka921> This has currently one false positive for the case
  // that the numbers add up to -min<int64_t> but this is not too important
  // for the sake of these unit tests.
  auto unsignedSum = abs(a) + abs(b);
  return unsignedSum <= std::max(abs(a), abs(b)) ||
         unsignedSum > std::numeric_limits<int64_t>::max();
}

// Return true if `a - b` would overflow.
bool subtractionWouldOverflow(int64_t a, int64_t b) {
  auto intMin = std::numeric_limits<int64_t>::min();
  if (a == intMin) {
    return b > 0;
  } else if (a == 0) {
    return b == intMin;
  } else {
    return additionWouldOverflow(-a, b);
  }
}

// Return true if `a * b` would overflow.
bool multiplicationWouldOverflow(int64_t a, int64_t b) {
  auto intMin = std::numeric_limits<int64_t>::min();
  if (a == 1 || b == 1) {
    return false;
  }
  if (a == intMin) {
    return (b != 0) && (b != 1);
  }
  if (b == intMin) {
    return (a != 0) && (a != 1);
  }
  auto abs = [](int64_t x) { return static_cast<uint64_t>(x < 0 ? -x : x); };
  auto productOfAbs = abs(a) * abs(b);
  if (static_cast<double>(productOfAbs) * 2 <
      static_cast<double>(abs(a)) * static_cast<double>(abs(b))) {
    return true;
  }
  return productOfAbs > std::numeric_limits<int64_t>::max();
  // TODO<joka921> Again the (non-overflowing) case of a positive and a negative
  // number the product of which ist `numeric_limits<int64_t>::min()` is
  // (wrongly) reported as overflowing.
  return true;
}

template <size_t N>
void addition(int64_t a, int64_t b) {
  return testTwoNumbers<N, std::plus<>{}, &additionWouldOverflow>(a, b);
}

template <size_t N>
void subtraction(int64_t a, int64_t b) {
  return testTwoNumbers<N, std::minus<>{}, &subtractionWouldOverflow>(a, b);
}

template <size_t N>
void multiplication(int64_t a, int64_t b) {
  return testTwoNumbers<N, std::multiplies<>{}, &multiplicationWouldOverflow>(
      a, b);
}

auto testNumeric = []<size_t N>(int64_t a, int64_t b) {
  addition<N>(a, b);
  subtraction<N>(a, b);
  multiplication<N>(a, b);
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
  for (auto i = max - numElements; i < max; ++i) {
    co_yield i + 1;
  }

  for (auto i = -numElements; i < numElements; ++i) {
    co_yield i;
  }

  int64_t min = std::numeric_limits<int64_t>::min();
  for (auto i = min; i <= min + numElements; ++i) {
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
