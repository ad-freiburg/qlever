// Copyright 2023, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Andre Schlegel (July of 2023,
// schlegea@informatik.uni-freiburg.de)

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cfloat>
#include <cstdint>
#include <ranges>
#include <sstream>
#include <vector>

#include "./util/GTestHelpers.h"
#include "util/Cache.h"
#include "util/ConstexprMap.h"
#include "util/MemorySize/MemorySize.h"
#include "util/SourceLocation.h"

// Importing all the literals.
using namespace ad_utility::memory_literals;

TEST(MemorySize, UserDefinedLiterals) {
  // Normal bytes.
  ASSERT_EQ(50uL, (50_B).getBytes());

  // Kilobytes.
  ASSERT_EQ(2000uL, (2_kB).getBytes());    // Whole number.
  ASSERT_EQ(1500uL, (1.5_kB).getBytes());  // Floating point without rounding.
  ASSERT_EQ(1001uL, (1.0003_kB).getBytes());  // Floating point with rounding.

  // Megabytes.
  ASSERT_EQ(2000000uL, (2_MB).getBytes());  // Whole number.
  ASSERT_EQ(1500000uL,
            (1.5_MB).getBytes());  // Floating point without rounding.
  ASSERT_EQ(1000001uL,
            (1.0000003_MB).getBytes());  // Floating point with rounding.

  // Gigabytes.
  ASSERT_EQ(2000000000uL, (2_GB).getBytes());  // Whole number.
  ASSERT_EQ(1500000000uL,
            (1.5_GB).getBytes());  // Floating point without rounding.
  ASSERT_EQ(1000000001uL,
            (1.0000000003_GB).getBytes());  // Floating point with rounding.

  // Terabytes.
  ASSERT_EQ(2000000000000uL, (2_TB).getBytes());  // Whole number.
  ASSERT_EQ(1500000000000uL,
            (1.5_TB).getBytes());  // Floating point without rounding.
  ASSERT_EQ(1000000000001uL,
            (1.0000000000003_TB).getBytes());  // Floating point with rounding.
}

// Describes a memory size in all available memory units.
struct AllMemoryUnitSizes {
  size_t bytes;
  double kilobytes;
  double megabytes;
  double gigabytes;
  double terabytes;
};

// A hash map pairing up a single memory size unit with the corresponding
// `AllMemoryUnitSizes` representations.
constexpr ad_utility::ConstexprMap<std::string_view, AllMemoryUnitSizes, 5>
    singleMemoryUnitSizes(
        {std::pair{"B", AllMemoryUnitSizes{1uL, 1e-3, 1e-6, 1e-9, 1e-12}},
         std::pair{"kB", AllMemoryUnitSizes{1'000uL, 1, 1e-3, 1e-6, 1e-9}},
         std::pair{"MB", AllMemoryUnitSizes{1'000'000uL, 1e3, 1, 1e-3, 1e-6}},
         std::pair{"GB",
                   AllMemoryUnitSizes{1'000'000'000uL, 1e6, 1e3, 1, 1e-3}},
         std::pair{"TB",
                   AllMemoryUnitSizes{1'000'000'000'000uL, 1e9, 1e6, 1e3, 1}}});

/*
Checks all the getters of the class with the wanted memory sizes.
*/
void checkAllMemorySizeGetter(
    const ad_utility::MemorySize& m, const AllMemoryUnitSizes& ms,
    ad_utility::source_location l = ad_utility::source_location::current()) {
  auto trace{generateLocationTrace(l)};

  ASSERT_EQ(m.getBytes(), ms.bytes);
  ASSERT_DOUBLE_EQ(m.getKilobytes(), ms.kilobytes);
  ASSERT_DOUBLE_EQ(m.getMegabytes(), ms.megabytes);
  ASSERT_DOUBLE_EQ(m.getGigabytes(), ms.gigabytes);
  ASSERT_DOUBLE_EQ(m.getTerabytes(), ms.terabytes);
}

TEST(MemorySize, MemorySizeConstructor) {
  // Default constructor.
  ad_utility::MemorySize m1;
  checkAllMemorySizeGetter(m1, AllMemoryUnitSizes{0uL, 0.0, 0.0, 0.0, 0.0});

  // Factory functions for size_t overload.
  checkAllMemorySizeGetter(ad_utility::MemorySize::bytes(1uL),
                           singleMemoryUnitSizes.at("B"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::kilobytes(1uL),
                           singleMemoryUnitSizes.at("kB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::megabytes(1uL),
                           singleMemoryUnitSizes.at("MB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::gigabytes(1uL),
                           singleMemoryUnitSizes.at("GB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::terabytes(1uL),
                           singleMemoryUnitSizes.at("TB"));

  // Factory functions for double overload.
  checkAllMemorySizeGetter(ad_utility::MemorySize::kilobytes(1.0),
                           singleMemoryUnitSizes.at("kB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::megabytes(1.0),
                           singleMemoryUnitSizes.at("MB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::gigabytes(1.0),
                           singleMemoryUnitSizes.at("GB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::terabytes(1.0),
                           singleMemoryUnitSizes.at("TB"));

  // Negative numbers are not allowed.
  ASSERT_ANY_THROW(ad_utility::MemorySize::kilobytes(-1.0));
  ASSERT_ANY_THROW(ad_utility::MemorySize::megabytes(-1.0));
  ASSERT_ANY_THROW(ad_utility::MemorySize::gigabytes(-1.0));
  ASSERT_ANY_THROW(ad_utility::MemorySize::terabytes(-1.0));

  // Numbers, that lead to overflow, are not allowed.
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::kilobytes(ad_utility::size_t_max),
      ::testing::ContainsRegex(absl::StrCat(ad_utility::size_t_max, " kB")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::megabytes(ad_utility::size_t_max),
      ::testing::ContainsRegex(absl::StrCat(ad_utility::size_t_max, " MB")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::gigabytes(ad_utility::size_t_max),
      ::testing::ContainsRegex(absl::StrCat(ad_utility::size_t_max, " GB")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::terabytes(ad_utility::size_t_max),
      ::testing::ContainsRegex(absl::StrCat(ad_utility::size_t_max, " TB")));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::kilobytes(DBL_MAX),
      ::testing::ContainsRegex("is larger than the maximum amount of memory"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::megabytes(DBL_MAX),
      ::testing::ContainsRegex("is larger than the maximum amount of memory"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::gigabytes(DBL_MAX),
      ::testing::ContainsRegex("is larger than the maximum amount of memory"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::MemorySize::terabytes(DBL_MAX),
      ::testing::ContainsRegex("is larger than the maximum amount of memory"));
}

TEST(MemorySize, AssignmentOperator) {
  ad_utility::MemorySize m;
  checkAllMemorySizeGetter(m, AllMemoryUnitSizes{0uL, 0.0, 0.0, 0.0, 0.0});

  m = 1_B;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("B"));

  m = 1_kB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("kB"));

  m = 1_MB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("MB"));

  m = 1_GB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("GB"));

  m = 1_TB;
  checkAllMemorySizeGetter(m, singleMemoryUnitSizes.at("TB"));
}

// For tests, where `MemorySize` is converted into string and vice-versa.
struct MemorySizeAndStringRepresentation {
  ad_utility::MemorySize memorySize_;
  std::string stringRepresentation_;
};

static std::vector<MemorySizeAndStringRepresentation>
generalAsStringTestCases() {
  return {{50_B, "50 B"},         {1_kB, "1000 B"},   {200_kB, "200 kB"},
          {150.5_kB, "150.5 kB"}, {2_MB, "2 MB"},     {1.5_MB, "1.5 MB"},
          {2_GB, "2 GB"},         {1.5_GB, "1.5 GB"}, {2_TB, "2 TB"},
          {1.5_TB, "1.5 TB"}};
}

TEST(MemorySize, AsString) {
  // Checks the expected string representation.
  auto doTest = [](const MemorySizeAndStringRepresentation& testCase) {
    // Normal `asString`.
    ASSERT_STREQ(testCase.memorySize_.asString().c_str(),
                 testCase.stringRepresentation_.data());

    // With the `<<` operator.
    std::ostringstream stream;
    stream << testCase.memorySize_;
    ASSERT_STREQ(stream.str().c_str(), testCase.stringRepresentation_.data());
  };

  std::ranges::for_each(generalAsStringTestCases(), doTest);

  // Check, if it always uses the right unit.
  doTest({99'999_B, "99999 B"});
  doTest({100'000_B, "100 kB"});
  doTest({400'000_B, "400 kB"});
  doTest({4'000_kB, "4 MB"});
  doTest({4'000_MB, "4 GB"});
  doTest({4'000_GB, "4 TB"});
}

TEST(MemorySize, Parse) {
  // Parse the given string and compare to the expected instance of
  // `MemorySize`.
  auto doTest = [](const MemorySizeAndStringRepresentation& testCase) {
    ASSERT_EQ(ad_utility::MemorySize::parse(testCase.stringRepresentation_)
                  .getBytes(),
              testCase.memorySize_.getBytes());
  };

  // Check, if parsing the given string causes an exception.
  auto doExceptionTest = [](std::string_view str) {
    ASSERT_ANY_THROW(ad_utility::MemorySize{}.parse(str));
  };

  // General testing.
  std::ranges::for_each(generalAsStringTestCases(), doTest);

  // Does `Byte` only work with whole, positive numbers?
  doExceptionTest("-46 B");
  doExceptionTest("4.2 B");
  doExceptionTest("-4.2 B");

  // Nothing should work with negativ numbers.
  std::ranges::for_each(generalAsStringTestCases(), doExceptionTest,
                        [](const MemorySizeAndStringRepresentation& testCase) {
                          return absl::StrCat("-",
                                              testCase.stringRepresentation_);
                        });

  // byte sizes can only be set with `B`.
  std::ranges::for_each(std::vector{"42 BYTE", "42 BYTe", "42 BYtE", "42 BYte",
                                    "42 ByTE", "42 ByTe", "42 BytE", "42 Byte",
                                    "42 bYTE", "42 bYTe", "42 bYtE", "42 bYte",
                                    "42 byTE", "42 byTe", "42 bytE", "42 byte"},
                        doExceptionTest);

  // Is our grammar truly case insensitive?
  std::ranges::for_each(
      std::vector<MemorySizeAndStringRepresentation>{{42_B, "42 B"},
                                                     {42_B, "42 b"},
                                                     {42_kB, "42 KB"},
                                                     {42_kB, "42 Kb"},
                                                     {42_kB, "42 kB"},
                                                     {42_kB, "42 kb"},
                                                     {42_MB, "42 MB"},
                                                     {42_MB, "42 Mb"},
                                                     {42_MB, "42 mB"},
                                                     {42_MB, "42 mb"},
                                                     {42_GB, "42 GB"},
                                                     {42_GB, "42 Gb"},
                                                     {42_GB, "42 gB"},
                                                     {42_GB, "42 gb"},
                                                     {42_TB, "42 TB"},
                                                     {42_TB, "42 Tb"},
                                                     {42_TB, "42 tB"},
                                                     {42_TB, "42 tb"}},
      doTest);
}

// Checks, if all the constexpr functions can actually be evaluated at compile
// time.
TEST(MemorySize, ConstEval) {
  // Default constructor.
  constexpr ad_utility::MemorySize m{};
  static_assert(m.getBytes() == 0);

  // Copy constructor.
  static_assert(ad_utility::MemorySize(m).getBytes() == 0);

  // Move constructor.
  static_assert(ad_utility::MemorySize(ad_utility::MemorySize{}).getBytes() ==
                0);

  // Factory functions.
  static_assert(ad_utility::MemorySize::bytes(42).getBytes() == 42);
  static_assert(ad_utility::MemorySize::kilobytes(42uL).getKilobytes() == 42);
  static_assert(ad_utility::MemorySize::kilobytes(4.2).getKilobytes() == 4.2);
  static_assert(ad_utility::MemorySize::megabytes(42uL).getMegabytes() == 42);
  static_assert(ad_utility::MemorySize::megabytes(4.2).getMegabytes() == 4.2);
  static_assert(ad_utility::MemorySize::gigabytes(42uL).getGigabytes() == 42);
  static_assert(ad_utility::MemorySize::gigabytes(4.2).getGigabytes() == 4.2);
  static_assert(ad_utility::MemorySize::terabytes(42uL).getTerabytes() == 42);
  static_assert(ad_utility::MemorySize::terabytes(4.2).getTerabytes() == 4.2);
}
