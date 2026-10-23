// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <limits>

#include "util/Math.h"

using ad_utility::ceilDiv;

// _____________________________________________________________________________
TEST(Math, ceilDiv) {
  static_assert(ceilDiv(0, 3) == 0);
  static_assert(ceilDiv(1, 3) == 1);
  static_assert(ceilDiv(3, 3) == 1);
  static_assert(ceilDiv(4, 3) == 2);
  static_assert(ceilDiv(7, 1) == 7);
  EXPECT_EQ(ceilDiv(10, 4), 3u);
  EXPECT_EQ(ceilDiv(12, 4), 3u);
  // No overflow for large numerators.
  constexpr size_t max = std::numeric_limits<size_t>::max();
  EXPECT_EQ(ceilDiv(max, 2), max / 2 + 1);
  EXPECT_EQ(ceilDiv(max, max), 1u);
  EXPECT_ANY_THROW(ceilDiv(1, 0));
}
