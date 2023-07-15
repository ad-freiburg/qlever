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

#include "util/ConstexprMap.h"
#include "util/MemorySize/MemorySize.h"

// Importing all the literals.
using namespace ad_utility::memory_literals;

// A small helper, because you can't call member functions on user defined
// literals.
size_t getBytes(const ad_utility::MemorySize& m) { return m.bytes(); };

TEST(MemorySize, UserDefinedLiterals) {
  // Normal bytes.
  ASSERT_EQ(50uL, getBytes(50_Byte));

  // Kilobytes.
  ASSERT_EQ(2000uL, getBytes(2_KB));       // Whole number.
  ASSERT_EQ(1500uL, getBytes(1.5_KB));     // Floating point without rounding.
  ASSERT_EQ(1001uL, getBytes(1.0003_KB));  // Floating point with rounding.

  // Megabytes.
  ASSERT_EQ(2000000uL, getBytes(2_MB));    // Whole number.
  ASSERT_EQ(1500000uL, getBytes(1.5_MB));  // Floating point without rounding.
  ASSERT_EQ(1000001uL,
            getBytes(1.0000003_MB));  // Floating point with rounding.

  // Gigabytes.
  ASSERT_EQ(2000000000uL, getBytes(2_GB));  // Whole number.
  ASSERT_EQ(1500000000uL,
            getBytes(1.5_GB));  // Floating point without rounding.
  ASSERT_EQ(1000000001uL,
            getBytes(1.0000000003_GB));  // Floating point with rounding.

  // Terabytes.
  ASSERT_EQ(2000000000000uL, getBytes(2_TB));  // Whole number.
  ASSERT_EQ(1500000000000uL,
            getBytes(1.5_TB));  // Floating point without rounding.
  ASSERT_EQ(1000000000001uL,
            getBytes(1.0000000000003_TB));  // Floating point with rounding.

  // Petabytes.
  ASSERT_EQ(2000000000000000uL, getBytes(2_PB));  // Whole number.
  ASSERT_EQ(1500000000000000uL,
            getBytes(1.5_PB));  // Floating point without rounding.
  ASSERT_EQ(1000000000000001uL,
            getBytes(1.0000000000000003_PB));  // Floating point with rounding.
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
             "Byte", AllMemoryUnitSizes{1uL, 1e-3, 1e-6, 1e-9, 1e-12, 1e-15}},
         std::pair<std::string_view, AllMemoryUnitSizes>{
             "KB", AllMemoryUnitSizes{1000uL, 1, 1e-3, 1e-6, 1e-9, 1e-12}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "MB", AllMemoryUnitSizes{1000000uL, 1e3, 1, 1e-3, 1e-6, 1e-9}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "GB", AllMemoryUnitSizes{1000000000uL, 1e6, 1e3, 1, 1e-3, 1e-6}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "TB", AllMemoryUnitSizes{1000000000000uL, 1e9, 1e6, 1e3, 1, 1e-3}},

         std::pair<std::string_view, AllMemoryUnitSizes>{
             "PB",
             AllMemoryUnitSizes{1000000000000000uL, 1e12, 1e9, 1e6, 1e3, 1}}});

/*
Checks all the getters of the class with the wanted memory sizes.
*/
void checkAllMemorySizeGetter(const ad_utility::MemorySize& m,
                              const AllMemoryUnitSizes& ms) {
  ASSERT_EQ(m.bytes(), ms.bytes);
  ASSERT_DOUBLE_EQ(m.kilobytes(), ms.kilobytes);
  ASSERT_DOUBLE_EQ(m.megabytes(), ms.megabytes);
  ASSERT_DOUBLE_EQ(m.gigabytes(), ms.gigabytes);
  ASSERT_DOUBLE_EQ(m.terabytes(), ms.terabytes);
  ASSERT_DOUBLE_EQ(m.petabytes(), ms.petabytes);
}

TEST(MemorySize, MemorySizeConstructor) {
  // Default constructor.
  ad_utility::MemorySize m1;
  checkAllMemorySizeGetter(m1,
                           AllMemoryUnitSizes{0uL, 0.0, 0.0, 0.0, 0.0, 0.0});

  // Factory functions for size_t overload.
  checkAllMemorySizeGetter(ad_utility::MemorySize::bytes(1uL),
                           singleMemoryUnitSizes.at("Byte"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::kilobytes(1uL),
                           singleMemoryUnitSizes.at("KB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::megabytes(1uL),
                           singleMemoryUnitSizes.at("MB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::gigabytes(1uL),
                           singleMemoryUnitSizes.at("GB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::terabytes(1uL),
                           singleMemoryUnitSizes.at("TB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::petabytes(1uL),
                           singleMemoryUnitSizes.at("PB"));

  // Factory functions for double overload.
  checkAllMemorySizeGetter(ad_utility::MemorySize::kilobytes(1.0),
                           singleMemoryUnitSizes.at("KB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::megabytes(1.0),
                           singleMemoryUnitSizes.at("MB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::gigabytes(1.0),
                           singleMemoryUnitSizes.at("GB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::terabytes(1.0),
                           singleMemoryUnitSizes.at("TB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::petabytes(1.0),
                           singleMemoryUnitSizes.at("PB"));
}

TEST(MemorySize, AssignmentOperator) {
  ad_utility::MemorySize m;
  checkAllMemorySizeGetter(m, AllMemoryUnitSizes{0uL, 0.0, 0.0, 0.0, 0.0, 0.0});

  m = 1_Byte;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("Byte"));

  m = 1_KB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("KB"));

  m = 1_MB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("MB"));

  m = 1_GB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("GB"));

  m = 1_TB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("TB"));

  m = 1_PB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("PB"));
}

// For tests, where `MemorySize` is converted into string and vice-versa.
struct MemorySizeInByteAndStringRepresentation {
  size_t memoryAmount_;
  std::string stringRepresentation_;
};

static std::vector<MemorySizeInByteAndStringRepresentation>
generalAsStringTestCases() {
  return {{getBytes(50_Byte), "50 Byte"}, {getBytes(2_KB), "2 KB"},
          {getBytes(1.5_KB), "1.5 KB"},   {getBytes(2_MB), "2 MB"},
          {getBytes(1.5_MB), "1.5 MB"},   {getBytes(2_GB), "2 GB"},
          {getBytes(1.5_GB), "1.5 GB"},   {getBytes(2_TB), "2 TB"},
          {getBytes(1.5_TB), "1.5 TB"},   {getBytes(2_PB), "2 PB"},
          {getBytes(1.5_PB), "1.5 PB"}};
}

TEST(MemorySize, AsString) {
  // Creates an instance with given amount of memory noted and checks the
  // expected string representation.
  auto doTest = [](const MemorySizeInByteAndStringRepresentation& testCase) {
    ad_utility::MemorySize mem(
        ad_utility::MemorySize::bytes(testCase.memoryAmount_));

    // Normal `asString`.
    ASSERT_STREQ(mem.asString().c_str(), testCase.stringRepresentation_.data());

    // With the `<<` operator.
    std::ostringstream stream;
    stream << mem;
    ASSERT_STREQ(stream.str().c_str(), testCase.stringRepresentation_.data());
  };

  std::ranges::for_each(generalAsStringTestCases(), doTest);

  // Check, if it always uses the biggest unit.
  doTest({getBytes(4000_Byte), "4 KB"});
  doTest({getBytes(4000_KB), "4 MB"});
  doTest({getBytes(4000_MB), "4 GB"});
  doTest({getBytes(4000_GB), "4 TB"});
  doTest({getBytes(4000_TB), "4 PB"});
}

TEST(MemorySize, Parse) {
  // Parse the given string and compare to the expected amount of bytes.
  auto doTest = [](const MemorySizeInByteAndStringRepresentation& testCase) {
    ASSERT_EQ(
        ad_utility::MemorySize::parse(testCase.stringRepresentation_).bytes(),
        testCase.memoryAmount_);
  };

  // Check, if parsing the given string causes an exception.
  auto doExceptionTest = [](std::string_view str) {
    ASSERT_ANY_THROW(ad_utility::MemorySize{}.parse(str));
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
      [](const MemorySizeInByteAndStringRepresentation& testCase) {
        return absl::StrCat("-", testCase.stringRepresentation_);
      });

  // Is it truly case insensitive?
  std::ranges::for_each(
      std::vector<MemorySizeInByteAndStringRepresentation>{
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
