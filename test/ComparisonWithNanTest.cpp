//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>

#include "util/ComparisonWithNan.h"

namespace {
static constexpr double NaN = std::numeric_limits<double>::quiet_NaN();
static constexpr double inf = std::numeric_limits<double>::infinity();
static constexpr double negInf = -inf;
auto lt = ad_utility::makeComparatorForNans(std::less{});
auto le = ad_utility::makeComparatorForNans(std::less_equal{});
auto eq = ad_utility::makeComparatorForNans(std::equal_to{});
auto ne = ad_utility::makeComparatorForNans(std::not_equal_to{});
auto ge = ad_utility::makeComparatorForNans(std::greater_equal{});
auto gt = ad_utility::makeComparatorForNans(std::greater{});
}  // namespace

// ___________________________________________________________
TEST(ComparisonWithNan, Sorting) {
  std::vector<double> input{NaN, 3.0, -3.0, NaN, negInf, NaN, inf};
  std::vector<double> expected{negInf, -3.0, 3.0, inf, NaN, NaN, NaN};
  std::ranges::sort(input, lt);
  ASSERT_EQ(input.size(), expected.size());
  for (size_t i = 0; i < input.size(); ++i) {
    auto a = input[i];
    auto b = expected[i];
    EXPECT_TRUE((a == b) || (std::isnan(a) && std::isnan(b)));
  }
}

// Test several invariants of the relations `<, <=, ==, !=, >, >=` for two
// arbitrary inputs `a, b`.
void testInvariants(auto a, auto b) {
  // `==` and `!=` are symmetric.
  ASSERT_EQ(eq(a, b), eq(b, a));
  ASSERT_EQ(ne(a, b), ne(b, a));
  // `==` is the opposite of `!=`, `<` is the opposite of `>=` and `<=` is the
  // opposite of `>`.
  ASSERT_NE(eq(a, b), ne(a, b));
  ASSERT_NE(lt(a, b), ge(a, b));
  ASSERT_NE(lt(b, a), ge(b, a));
  ASSERT_NE(le(a, b), gt(a, b));
  ASSERT_NE(le(b, a), gt(b, a));
}

// Run exhaustive tests for numbers `a, b` where `a < b`.
void testLess(auto a, auto b) {
  ASSERT_TRUE(lt(a, b));
  ASSERT_TRUE(le(a, b));
  ASSERT_FALSE(eq(a, b));
  testInvariants(a, b);
}

// Run exhaustive tests for numbers `a, b` where `a == b`.
void testEqual(auto a, auto b) {
  ASSERT_FALSE(lt(a, b));
  ASSERT_TRUE(le(a, b));
  ASSERT_TRUE(eq(a, b));
  testInvariants(a, b);
}

// ___________________________________________________________
TEST(ComparisonWithNan, NoFloatingPointOrNaN) {
  testLess(3, 4);
  testLess(-2, 3);
  testEqual(3, 3);
}

// ___________________________________________________________
TEST(ComparisonWithNan, OneFloatingPointOrNaN) {
  testLess(3.0, 4);
  testLess(3.0, 15);
  testLess(6, NaN);
  testLess(-7432, NaN);
}

// ___________________________________________________________
TEST(ComparisonWithNan, BothFloatingPointOrNan) {
  testLess(3.0, 4.0);
  testLess(3.8, 15.2);
  testEqual(3.0, 3.0);
  testLess(-2.3, 3.3);

  testLess(6.2, NaN);
  testLess(-7632.8, NaN);
  testEqual(NaN, NaN);
  testLess(negInf, NaN);
  testLess(inf, NaN);
}
