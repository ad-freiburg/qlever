// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <gtest/gtest.h>

#include <cstdint>

#include "util/AbstractMemory/Memory.h"

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

// Describes a memory size in all available memory units.
struct MemorySize {
  size_t bytes;
  double kilobytes;
  double megabytes;
  double gigabytes;
  double terabytes;
  double petabytes;
};

/*
Checks all the getters of the class with the wanted memory sizes.
*/
void checkAllMemoryGetter(const ad_utility::Memory& m, const MemorySize& ms) {
  ASSERT_EQ(m.bytes(), ms.bytes);
  ASSERT_DOUBLE_EQ(m.kilobytes(), ms.kilobytes);
  ASSERT_DOUBLE_EQ(m.megabytes(), ms.megabytes);
  ASSERT_DOUBLE_EQ(m.gigabytes(), ms.gigabytes);
  ASSERT_DOUBLE_EQ(m.terabytes(), ms.terabytes);
  ASSERT_DOUBLE_EQ(m.petabytes(), ms.petabytes);
}

TEST(MemoryUtilTest, MemoryConstructor) {
  // Default constructor.
  ad_utility::Memory m1;
  checkAllMemoryGetter(m1, MemorySize{0uL, 0.0, 0.0, 0.0, 0.0, 0.0});

  // Non-default constructor.
  ad_utility::Memory m2(1024);
  checkAllMemoryGetter(m2,
                       MemorySize{1024uL, 1.0, 0.0009765625, 9.5367431640625e-7,
                                  9.31322574615478515625e-10,
                                  9.094947017729282379150390625e-13});
}

TEST(MemoryUtilTest, SizeTAssignmentOperator) {
  ad_utility::Memory m;
  checkAllMemoryGetter(m, MemorySize{0uL, 0.0, 0.0, 0.0, 0.0, 0.0});

  m = 1_Byte;
  checkAllMemoryGetter(m, MemorySize{1uL, 0.0009765625, 9.5367431640625e-7,
                                     9.31322574615478515625e-10,
                                     9.094947017729282379150390625e-13,
                                     8.8817841970012523233890533447265625e-16});

  m = 1_KB;
  checkAllMemoryGetter(m,
                       MemorySize{1024uL, 1.0, 0.0009765625, 9.5367431640625e-7,
                                  9.31322574615478515625e-10,
                                  9.094947017729282379150390625e-13});

  m = 1_MB;
  checkAllMemoryGetter(
      m, MemorySize{1048576uL, 1024, 1.0, 0.0009765625, 9.5367431640625e-7,
                    9.31322574615478515625e-10});

  m = 1_GB;
  checkAllMemoryGetter(m, MemorySize{1073741824uL, 1048576.0, 1024.0, 1.0,
                                     0.0009765625, 9.5367431640625e-7});

  m = 1_TB;
  checkAllMemoryGetter(m, MemorySize{1099511627776uL, 1073741824.0, 1048576.0,
                                     1024.0, 1.0, 0.0009765625});

  m = 1_PB;
  checkAllMemoryGetter(m, MemorySize{1125899906842624uL, 1099511627776.0,
                                     1073741824.0, 1048576.0, 1024.0, 1.0});
}

TEST(MemoryUtilTest, AsString) {
  // Creates an instance with given amount of memory noted and checks the
  // expected string representation.
  auto doTest = [](const size_t& memoryAmount,
                   std::string_view expectedStringRepresantation) {
    ASSERT_STREQ(ad_utility::Memory(memoryAmount).asString().c_str(),
                 expectedStringRepresantation.data());
  };

  doTest(50_Byte, "50 Byte");
  doTest(2_KB, "2 KB");
  doTest(1.5_KB, "1.5 KB");
  doTest(2_MB, "2 MB");
  doTest(1.5_MB, "1.5 MB");
  doTest(2_GB, "2 GB");
  doTest(1.5_GB, "1.5 GB");
  doTest(2_TB, "2 TB");
  doTest(1.5_TB, "1.5 TB");
  doTest(2_PB, "2 PB");
  doTest(1.5_PB, "1.5 PB");

  doTest(4096_Byte, "4 KB");
  doTest(4096_KB, "4 MB");
  doTest(4096_MB, "4 GB");
  doTest(4096_GB, "4 TB");
  doTest(4096_TB, "4 PB");
}
