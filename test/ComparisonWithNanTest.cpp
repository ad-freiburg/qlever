//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <functional>

#include "util/ComparisonWithNan.h"

static constexpr double NaN = std::numeric_limits<double>::quiet_NaN();

// ___________________________________________________________
TEST(ComparisonWithNan, NoFloatingPoint) {
  auto comp = ad_utility::makeComparatorForNans(std::less{});
  ASSERT_TRUE(comp(3, 4));
  ASSERT_TRUE(comp(3, 15));
  ASSERT_FALSE(comp(3, 3));
  ASSERT_FALSE(comp(3, -2));
}

// ___________________________________________________________
TEST(ComparisonWithNan, FirstFloatingPoint) {
  auto comp = ad_utility::makeComparatorForNans(std::less{});
  ASSERT_TRUE(comp(3.0, 4));
  ASSERT_TRUE(comp(3.0, 15));
  ASSERT_FALSE(comp(3.0, 3));
  ASSERT_FALSE(comp(3.0, -2));

  ASSERT_FALSE(comp(NaN, 6));
  ASSERT_FALSE(comp(NaN, -7432));

  auto compEq = ad_utility::makeComparatorForNans(std::equal_to{});
  auto compNe = ad_utility::makeComparatorForNans(std::not_equal_to{});

  ASSERT_TRUE(compEq(3.0, 3));
  ASSERT_FALSE(compEq(3.0, 5));
  ASSERT_FALSE(compEq(NaN, 5));

  ASSERT_FALSE(compNe(3.0, 3));
  ASSERT_TRUE(compNe(3.0, 5));

  ASSERT_TRUE(compNe(NaN, 5));

  auto compLe = ad_utility::makeComparatorForNans(std::less_equal{});
  ASSERT_TRUE(compLe(3.0, 3));
  ASSERT_TRUE(compLe(3.0, 4));
  ASSERT_FALSE(compLe(NaN, 4));
}

// ___________________________________________________________
TEST(ComparisonWithNan, SecondFloatingPoint) {
  auto comp = ad_utility::makeComparatorForNans(std::less{});
  ASSERT_TRUE(comp(3, 4.0));
  ASSERT_TRUE(comp(3, 15.2));
  ASSERT_FALSE(comp(3, 3.0));
  ASSERT_FALSE(comp(3, -2.3));

  ASSERT_TRUE(comp(6, NaN));
  ASSERT_TRUE(comp(-7432, NaN));

  auto compEq = ad_utility::makeComparatorForNans(std::equal_to{});
  auto compNe = ad_utility::makeComparatorForNans(std::not_equal_to{});

  ASSERT_TRUE(compEq(3, 3.0));
  ASSERT_FALSE(compEq(3, 5.2));
  ASSERT_FALSE(compEq(5, NaN));

  ASSERT_FALSE(compNe(3, 3.0));
  ASSERT_TRUE(compNe(3, 5.2));

  ASSERT_TRUE(compNe(5, NaN));

  auto compLe = ad_utility::makeComparatorForNans(std::less_equal{});
  ASSERT_TRUE(compLe(3, 3.0));
  ASSERT_TRUE(compLe(3, 3.1));
  ASSERT_TRUE(compLe(3, NaN));
}

// ___________________________________________________________
TEST(ComparisonWithNan, BothFloatingPoint) {
  auto comp = ad_utility::makeComparatorForNans(std::less{});
  ASSERT_TRUE(comp(3.0, 4.0));
  ASSERT_TRUE(comp(3.8, 15.2));
  ASSERT_FALSE(comp(3.0, 3.0));
  ASSERT_FALSE(comp(3.3, -2.3));

  ASSERT_TRUE(comp(6.2, NaN));
  ASSERT_TRUE(comp(-7432.5, NaN));
  ASSERT_FALSE(comp(NaN, 6.2));
  ASSERT_FALSE(comp(NaN, -7432.5));

  auto compEq = ad_utility::makeComparatorForNans(std::equal_to{});
  auto compNe = ad_utility::makeComparatorForNans(std::not_equal_to{});

  ASSERT_TRUE(compEq(3.0, 3.0));
  ASSERT_FALSE(compEq(3.0, 5.2));
  ASSERT_FALSE(compEq(5.0, NaN));
  ASSERT_TRUE(compEq(NaN, NaN));

  ASSERT_FALSE(compNe(3.0, 3.0));
  ASSERT_TRUE(compNe(3.4, 5.2));

  ASSERT_TRUE(compNe(5.0, NaN));
  ASSERT_FALSE(compNe(NaN, NaN));

  auto compLe = ad_utility::makeComparatorForNans(std::less_equal{});
  ASSERT_TRUE(compLe(3, 3.0));
  ASSERT_TRUE(compLe(3, 3.1));
  ASSERT_TRUE(compLe(3, NaN));
  ASSERT_TRUE(compLe(NaN, NaN));
}
