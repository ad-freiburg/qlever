// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <ranges>
#include <vector>

#include "util/AbstractMemory/Memory.h"

// Importing all the literals.
using namespace ad_utility::memory_literals;

TEST(AbstractMemory, UserDefinedLiterals) {
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

TEST(AbstractMemory, MemoryConstructor) {
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

TEST(AbstractMemory, SizeTAssignmentOperator) {
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

// For tests, where one is converted into the other and vice-versa.
struct MemoryAmountAndStringRepresentation {
  size_t memoryAmount_;
  std::string stringRepresentation;
};

static std::vector<MemoryAmountAndStringRepresentation>
generalAsStringTestCases() {
  return {{50_Byte, "50 Byte"}, {2_KB, "2 KB"},     {1.5_KB, "1.5 KB"},
          {2_MB, "2 MB"},       {1.5_MB, "1.5 MB"}, {2_GB, "2 GB"},
          {1.5_GB, "1.5 GB"},   {2_TB, "2 TB"},     {1.5_TB, "1.5 TB"},
          {2_PB, "2 PB"},       {1.5_PB, "1.5 PB"}};
}

TEST(AbstractMemory, AsString) {
  // Creates an instance with given amount of memory noted and checks the
  // expected string representation.
  auto doTest = [](const MemoryAmountAndStringRepresentation& testCase) {
    ASSERT_STREQ(ad_utility::Memory(testCase.memoryAmount_).asString().c_str(),
                 testCase.stringRepresentation.data());
  };

  std::ranges::for_each(generalAsStringTestCases(), doTest);

  // Check, if it always uses the biggest unit.
  doTest({4096_Byte, "4 KB"});
  doTest({4096_KB, "4 MB"});
  doTest({4096_MB, "4 GB"});
  doTest({4096_GB, "4 TB"});
  doTest({4096_TB, "4 PB"});
}

TEST(AbstractMemory, Parse) {
  // Creates an instance of `Memory`, parse the given string and compare to the
  // expected amount of bytes.
  auto doTest = [](const MemoryAmountAndStringRepresentation& testCase) {
    ad_utility::Memory m;
    m.parse(testCase.stringRepresentation);
    ASSERT_EQ(m.bytes(), testCase.memoryAmount_);
  };

  // Check, if parsing the given string causes an exception.
  auto doExceptionTest = [](std::string_view str) {
    ASSERT_ANY_THROW(ad_utility::Memory{}.parse(str));
  };

  // General testing.
  std::ranges::for_each(generalAsStringTestCases(), doTest);

  // Does `B` work as a replacement for `Byte`?
  doTest({46_Byte, "46 Byte"});
  doTest({46_Byte, "46 B"});

  // Does `Byte` only work with whole, positive numbers?
  doExceptionTest("-46 B");
  doExceptionTest("4.2 B");
  doExceptionTest("-4.2 B");

  // Nothing should work with negativ numbers.
  std::ranges::for_each(
      generalAsStringTestCases(), doExceptionTest,
      [](const MemoryAmountAndStringRepresentation& testCase) {
        return absl::StrCat("-", testCase.stringRepresentation);
      });

  // Is it truly case insensitive?
  std::ranges::for_each(
      std::vector<MemoryAmountAndStringRepresentation>{
          {42_Byte, "42 BYTE"}, {42_Byte, "42 BYTe"}, {42_Byte, "42 BYtE"},
          {42_Byte, "42 BYte"}, {42_Byte, "42 ByTE"}, {42_Byte, "42 ByTe"},
          {42_Byte, "42 BytE"}, {42_Byte, "42 Byte"}, {42_Byte, "42 bYTE"},
          {42_Byte, "42 bYTe"}, {42_Byte, "42 bYtE"}, {42_Byte, "42 bYte"},
          {42_Byte, "42 byTE"}, {42_Byte, "42 byTe"}, {42_Byte, "42 bytE"},
          {42_Byte, "42 byte"}, {42_Byte, "42 B"},    {42_Byte, "42 b"},
          {42_KB, "42 KB"},     {42_KB, "42 Kb"},     {42_KB, "42 kB"},
          {42_KB, "42 kb"},     {42_MB, "42 MB"},     {42_MB, "42 Mb"},
          {42_MB, "42 mB"},     {42_MB, "42 mb"},     {42_GB, "42 GB"},
          {42_GB, "42 Gb"},     {42_GB, "42 gB"},     {42_GB, "42 gb"},
          {42_TB, "42 TB"},     {42_TB, "42 Tb"},     {42_TB, "42 tB"},
          {42_TB, "42 tb"},     {42_PB, "42 PB"},     {42_PB, "42 Pb"},
          {42_PB, "42 pB"},     {42_PB, "42 pb"}},
      doTest);
}
