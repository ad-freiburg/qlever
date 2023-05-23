//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <functional>

#include "util/ComparisonWithNan.h"

static constexpr double NaN = std::numeric_limits<double>::quiet_NaN();

TEST(ComparisonWithNan, NoFloatingPoint) {
  auto comp = ad_utility::makeComparatorForNans(std::less{});
  ASSERT_TRUE(comp(3, 4));
  ASSERT_TRUE(comp(3, 15));
  ASSERT_FALSE(comp(3, 3));
  ASSERT_FALSE(comp(3, -2));
}

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
}
