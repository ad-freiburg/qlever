// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <ranges>
#include <sstream>
#include <vector>

#include "util/AbstractMemory/Memory.h"
#include "util/ConstexprMap.h"

// Importing all the literals.
using namespace ad_utility::memory_literals;

// A small helper, because you can't call member functions on user defined
// literals.
size_t getBytes(const ad_utility::Memory& m) { return m.bytes(); };

TEST(AbstractMemory, UserDefinedLiterals) {
  // Normal bytes.
  ASSERT_EQ(50uL, getBytes(50_Byte));

  // Kilobytes.
  ASSERT_EQ(2048uL, getBytes(2_KB));    // Whole number.
  ASSERT_EQ(1536uL, getBytes(1.5_KB));  // Floating point without rounding.
  ASSERT_EQ(1332uL, getBytes(1.3_KB));  // Floating point with rounding.

  // Megabytes.
  ASSERT_EQ(2097152uL, getBytes(2_MB));    // Whole number.
  ASSERT_EQ(1572864uL, getBytes(1.5_MB));  // Floating point without rounding.
  ASSERT_EQ(1363149uL, getBytes(1.3_MB));  // Floating point with rounding.

  // Gigabytes.
  ASSERT_EQ(2147483648uL, getBytes(2_GB));  // Whole number.
  ASSERT_EQ(1610612736uL,
            getBytes(1.5_GB));  // Floating point without rounding.
  ASSERT_EQ(1395864372uL, getBytes(1.3_GB));  // Floating point with rounding.

  // Terabytes.
  ASSERT_EQ(2199023255552uL, getBytes(2_TB));  // Whole number.
  ASSERT_EQ(1649267441664uL,
            getBytes(1.5_TB));  // Floating point without rounding.
  ASSERT_EQ(1429365116109uL,
            getBytes(1.3_TB));  // Floating point with rounding.

  // Petabytes.
  ASSERT_EQ(2251799813685248uL, getBytes(2_PB));  // Whole number.
  ASSERT_EQ(1688849860263936uL,
            getBytes(1.5_PB));  // Floating point without rounding.
  ASSERT_EQ(1463669878895412uL,
            getBytes(1.3_PB));  // Floating point with rounding.
}

// Describes a memory size in all available memory units.
struct AllMemoryUnitSizes {
  size_t bytes;
  double kilobytes;
  double megabytes;
  double gigabytes;
  double terabytes;
  double petabytes;
};

// A hash map pairing up a single memory size unit with the corresponding
// `AllMemoryUnitSizes` representations.
constexpr ad_utility::ConstexprMap<std::string_view, AllMemoryUnitSizes, 6>
    singleMemoryUnitSizes(
        {std::pair<std::string_view, AllMemoryUnitSizes>{
             "Byte",
             AllMemoryUnitSizes{1uL, 0.0009765625, 9.5367431640625e-7,
                                9.31322574615478515625e-10,
                                9.094947017729282379150390625e-13,
                                8.8817841970012523233890533447265625e-16}},
         std::pair<std::string_view, AllMemoryUnitSizes>{
             "KB",
             AllMemoryUnitSizes{1024uL, 1.0, 0.0009765625, 9.5367431640625e-7,
                                9.31322574615478515625e-10,
                                9.094947017729282379150390625e-13}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "MB", AllMemoryUnitSizes{1048576uL, 1024, 1.0, 0.0009765625,
                                      9.5367431640625e-7,
                                      9.31322574615478515625e-10}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "GB", AllMemoryUnitSizes{1073741824uL, 1048576.0, 1024.0, 1.0,
                                      0.0009765625, 9.5367431640625e-7}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "TB", AllMemoryUnitSizes{1099511627776uL, 1073741824.0, 1048576.0,
                                      1024.0, 1.0, 0.0009765625}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "PB", AllMemoryUnitSizes{1125899906842624uL, 1099511627776.0,
                                      1073741824.0, 1048576.0, 1024.0, 1.0}}});

/*
Checks all the getters of the class with the wanted memory sizes.
*/
void checkAllMemoryGetter(const ad_utility::Memory& m,
                          const AllMemoryUnitSizes& ms) {
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
  checkAllMemoryGetter(m1, AllMemoryUnitSizes{0uL, 0.0, 0.0, 0.0, 0.0, 0.0});

  // Factory functions for size_t overload.
  checkAllMemoryGetter(ad_utility::Memory::bytes(1uL),
                       singleMemoryUnitSizes.at("Byte"));
  checkAllMemoryGetter(ad_utility::Memory::kilobytes(1uL),
                       singleMemoryUnitSizes.at("KB"));
  checkAllMemoryGetter(ad_utility::Memory::megabytes(1uL),
                       singleMemoryUnitSizes.at("MB"));
  checkAllMemoryGetter(ad_utility::Memory::gigabytes(1uL),
                       singleMemoryUnitSizes.at("GB"));
  checkAllMemoryGetter(ad_utility::Memory::terabytes(1uL),
                       singleMemoryUnitSizes.at("TB"));
  checkAllMemoryGetter(ad_utility::Memory::petabytes(1uL),
                       singleMemoryUnitSizes.at("PB"));

  // Factory functions for double overload.
  checkAllMemoryGetter(ad_utility::Memory::kilobytes(1.0),
                       singleMemoryUnitSizes.at("KB"));
  checkAllMemoryGetter(ad_utility::Memory::megabytes(1.0),
                       singleMemoryUnitSizes.at("MB"));
  checkAllMemoryGetter(ad_utility::Memory::gigabytes(1.0),
                       singleMemoryUnitSizes.at("GB"));
  checkAllMemoryGetter(ad_utility::Memory::terabytes(1.0),
                       singleMemoryUnitSizes.at("TB"));
  checkAllMemoryGetter(ad_utility::Memory::petabytes(1.0),
                       singleMemoryUnitSizes.at("PB"));
}

TEST(AbstractMemory, AssignmentOperator) {
  ad_utility::Memory m;
  checkAllMemoryGetter(m, AllMemoryUnitSizes{0uL, 0.0, 0.0, 0.0, 0.0, 0.0});

  m = 1_Byte;
  checkAllMemoryGetter(m, singleMemoryUnitSizes.at("Byte"));

  m = 1_KB;
  checkAllMemoryGetter(m, singleMemoryUnitSizes.at("KB"));

  m = 1_MB;
  checkAllMemoryGetter(m, singleMemoryUnitSizes.at("MB"));

  m = 1_GB;
  checkAllMemoryGetter(m, singleMemoryUnitSizes.at("GB"));

  m = 1_TB;
  checkAllMemoryGetter(m, singleMemoryUnitSizes.at("TB"));

  m = 1_PB;
  checkAllMemoryGetter(m, singleMemoryUnitSizes.at("PB"));
}

// For tests, where one is converted into the other and vice-versa.
struct MemoryAmountAndStringRepresentation {
  size_t memoryAmount_;
  std::string stringRepresentation_;
};

static std::vector<MemoryAmountAndStringRepresentation>
generalAsStringTestCases() {
  return {{getBytes(50_Byte), "50 Byte"}, {getBytes(2_KB), "2 KB"},
          {getBytes(1.5_KB), "1.5 KB"},   {getBytes(2_MB), "2 MB"},
          {getBytes(1.5_MB), "1.5 MB"},   {getBytes(2_GB), "2 GB"},
          {getBytes(1.5_GB), "1.5 GB"},   {getBytes(2_TB), "2 TB"},
          {getBytes(1.5_TB), "1.5 TB"},   {getBytes(2_PB), "2 PB"},
          {getBytes(1.5_PB), "1.5 PB"}};
}

TEST(AbstractMemory, AsString) {
  // Creates an instance with given amount of memory noted and checks the
  // expected string representation.
  auto doTest = [](const MemoryAmountAndStringRepresentation& testCase) {
    ad_utility::Memory mem(ad_utility::Memory::bytes(testCase.memoryAmount_));

    // Normal `asString`.
    ASSERT_STREQ(mem.asString().c_str(), testCase.stringRepresentation_.data());

    // With the `<<` operator.
    std::ostringstream stream;
    stream << mem;
    ASSERT_STREQ(stream.str().c_str(), testCase.stringRepresentation_.data());
  };

  std::ranges::for_each(generalAsStringTestCases(), doTest);

  // Check, if it always uses the biggest unit.
  doTest({getBytes(4096_Byte), "4 KB"});
  doTest({getBytes(4096_KB), "4 MB"});
  doTest({getBytes(4096_MB), "4 GB"});
  doTest({getBytes(4096_GB), "4 TB"});
  doTest({getBytes(4096_TB), "4 PB"});
}

TEST(AbstractMemory, Parse) {
  // Parse the given string and compare to the expected amount of bytes.
  auto doTest = [](const MemoryAmountAndStringRepresentation& testCase) {
    ASSERT_EQ(ad_utility::Memory::parse(testCase.stringRepresentation_).bytes(),
              testCase.memoryAmount_);
  };

  // Check, if parsing the given string causes an exception.
  auto doExceptionTest = [](std::string_view str) {
    ASSERT_ANY_THROW(ad_utility::Memory{}.parse(str));
  };

  // General testing.
  std::ranges::for_each(generalAsStringTestCases(), doTest);

  // Does `B` work as a replacement for `Byte`?
  doTest({getBytes(46_Byte), "46 Byte"});
  doTest({getBytes(46_Byte), "46 B"});

  // Does `Byte` only work with whole, positive numbers?
  doExceptionTest("-46 B");
  doExceptionTest("4.2 B");
  doExceptionTest("-4.2 B");

  // Nothing should work with negativ numbers.
  std::ranges::for_each(
      generalAsStringTestCases(), doExceptionTest,
      [](const MemoryAmountAndStringRepresentation& testCase) {
        return absl::StrCat("-", testCase.stringRepresentation_);
      });

  // Is it truly case insensitive?
  std::ranges::for_each(
      std::vector<MemoryAmountAndStringRepresentation>{
          {getBytes(42_Byte), "42 BYTE"}, {getBytes(42_Byte), "42 BYTe"},
          {getBytes(42_Byte), "42 BYtE"}, {getBytes(42_Byte), "42 BYte"},
          {getBytes(42_Byte), "42 ByTE"}, {getBytes(42_Byte), "42 ByTe"},
          {getBytes(42_Byte), "42 BytE"}, {getBytes(42_Byte), "42 Byte"},
          {getBytes(42_Byte), "42 bYTE"}, {getBytes(42_Byte), "42 bYTe"},
          {getBytes(42_Byte), "42 bYtE"}, {getBytes(42_Byte), "42 bYte"},
          {getBytes(42_Byte), "42 byTE"}, {getBytes(42_Byte), "42 byTe"},
          {getBytes(42_Byte), "42 bytE"}, {getBytes(42_Byte), "42 byte"},
          {getBytes(42_Byte), "42 B"},    {getBytes(42_Byte), "42 b"},
          {getBytes(42_KB), "42 KB"},     {getBytes(42_KB), "42 Kb"},
          {getBytes(42_KB), "42 kB"},     {getBytes(42_KB), "42 kb"},
          {getBytes(42_MB), "42 MB"},     {getBytes(42_MB), "42 Mb"},
          {getBytes(42_MB), "42 mB"},     {getBytes(42_MB), "42 mb"},
          {getBytes(42_GB), "42 GB"},     {getBytes(42_GB), "42 Gb"},
          {getBytes(42_GB), "42 gB"},     {getBytes(42_GB), "42 gb"},
          {getBytes(42_TB), "42 TB"},     {getBytes(42_TB), "42 Tb"},
          {getBytes(42_TB), "42 tB"},     {getBytes(42_TB), "42 tb"},
          {getBytes(42_PB), "42 PB"},     {getBytes(42_PB), "42 Pb"},
          {getBytes(42_PB), "42 pB"},     {getBytes(42_PB), "42 pb"}},
      doTest);
}
