// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdint>

#include "util/Memory.h"

// Importing all the literals.
using ad_utility::operator""_Byte;
using ad_utility::operator""_KB;
using ad_utility::operator""_MB;
using ad_utility::operator""_GB;
using ad_utility::operator""_TB;
using ad_utility::operator""_PB;

TEST(MemoryUtilTest, UserDefinedLiterals) {
  // Normal bytes.
  ASSERT_EQ(50uL, 50_Byte);

  // Kilobytes.
  ASSERT_EQ(2048uL, 2_KB);    // Whole number.
  ASSERT_EQ(1536uL, 1.5_KB);  // Floating point without rounding.
  ASSERT_EQ(1332uL, 1.3_KB);  // Floating point with rounding.

  // Megabytes.
  ASSERT_EQ(2097152uL, 2_MB);    // Whole number.
  ASSERT_EQ(1572864uL, 1.5_MB);  // Floating point without rounding.
  ASSERT_EQ(1363149uL, 1.3_MB);  // Floating point with rounding.

  // Gigabytes.
  ASSERT_EQ(2147483648uL, 2_GB);    // Whole number.
  ASSERT_EQ(1610612736uL, 1.5_GB);  // Floating point without rounding.
  ASSERT_EQ(1395864372uL, 1.3_GB);  // Floating point with rounding.

  // Terabytes.
  ASSERT_EQ(2199023255552uL, 2_TB);  // Whole number.
  ASSERT_EQ(1649267441664uL,
            1.5_TB);  // Floating point without rounding.
  ASSERT_EQ(1429365116109uL,
            1.3_TB);  // Floating point with rounding.

  // Petabytes.
  ASSERT_EQ(2251799813685248uL, 2_PB);  // Whole number.
  ASSERT_EQ(1688849860263936uL,
            1.5_PB);  // Floating point without rounding.
  ASSERT_EQ(1463669878895412uL,
            1.3_PB);  // Floating point with rounding.
}
