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
#include <stdexcept>
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
using Pair = ad_utility::ConstexprMapPair<std::string_view, AllMemoryUnitSizes>;
constexpr ad_utility::ConstexprMap<std::string_view, AllMemoryUnitSizes, 5>
    singleMemoryUnitSizes(
        {Pair{"B", AllMemoryUnitSizes{1uL, 1e-3, 1e-6, 1e-9, 1e-12}},
         Pair{"kB", AllMemoryUnitSizes{1'000uL, 1, 1e-3, 1e-6, 1e-9}},
         Pair{"MB", AllMemoryUnitSizes{1'000'000uL, 1e3, 1, 1e-3, 1e-6}},
         Pair{"GB", AllMemoryUnitSizes{1'000'000'000uL, 1e6, 1e3, 1, 1e-3}},
         Pair{"TB",
              AllMemoryUnitSizes{1'000'000'000'000uL, 1e9, 1e6, 1e3, 1}}});

/*
Checks all the getters of the class with the wanted memory sizes.
*/
void checkAllMemorySizeGetter(
    const ad_utility::MemorySize& m, const AllMemoryUnitSizes& ms,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
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

  // Factory functions for integral overload.
  checkAllMemorySizeGetter(ad_utility::MemorySize::bytes(1),
                           singleMemoryUnitSizes.at("B"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::kilobytes(1),
                           singleMemoryUnitSizes.at("kB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::megabytes(1),
                           singleMemoryUnitSizes.at("MB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::gigabytes(1),
                           singleMemoryUnitSizes.at("GB"));
  checkAllMemorySizeGetter(ad_utility::MemorySize::terabytes(1),
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

  // The factory function for a max size instance, should be the same as calling
  // `ad_utility::MemorySize::bytes(size_t_max)`.
  ASSERT_EQ(ad_utility::MemorySize::bytes(ad_utility::size_t_max),
            ad_utility::MemorySize::max());

  // Negative numbers are not allowed.
  ASSERT_ANY_THROW(ad_utility::MemorySize::bytes(-1));
  ASSERT_ANY_THROW(ad_utility::MemorySize::kilobytes(-1));
  ASSERT_ANY_THROW(ad_utility::MemorySize::megabytes(-1));
  ASSERT_ANY_THROW(ad_utility::MemorySize::gigabytes(-1));
  ASSERT_ANY_THROW(ad_utility::MemorySize::terabytes(-1));
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
    // Normal `getCacheKey`.
    ASSERT_STREQ(testCase.memorySize_.asString().c_str(),
                 testCase.stringRepresentation_.data());

    // With the `<<` operator.
    std::ostringstream stream;
    stream << testCase.memorySize_;
    ASSERT_STREQ(stream.str().c_str(), testCase.stringRepresentation_.data());
  };

  ql::ranges::for_each(generalAsStringTestCases(), doTest);

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
    ASSERT_EQ(ad_utility::MemorySize::parse(testCase.stringRepresentation_),
              testCase.memorySize_);
  };

  // Check, if parsing the given string causes an exception.
  auto doExceptionTest = [](std::string_view str) {
    ASSERT_ANY_THROW(ad_utility::MemorySize{}.parse(str));
  };

  // General testing.
  ql::ranges::for_each(generalAsStringTestCases(), doTest);

  // Does `Byte` only work with whole, positive numbers?
  doExceptionTest("-46 B");
  doExceptionTest("4.2 B");
  doExceptionTest("-4.2 B");

  // Nothing should work with negative numbers.
  ql::ranges::for_each(generalAsStringTestCases(), doExceptionTest,
                       [](const MemorySizeAndStringRepresentation& testCase) {
                         return absl::StrCat("-",
                                             testCase.stringRepresentation_);
                       });

  // Byte sizes can only be set with `B`.
  ql::ranges::for_each(std::vector{"42 BYTE", "42 BYTe", "42 BYtE", "42 BYte",
                                   "42 ByTE", "42 ByTe", "42 BytE", "42 Byte",
                                   "42 bYTE", "42 bYTe", "42 bYtE", "42 bYte",
                                   "42 byTE", "42 byTe", "42 bytE", "42 byte"},
                       doExceptionTest);

  // Is our grammar truly case insensitive?
  ql::ranges::for_each(
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

  // Does our short hand (memory unit without the `B` at the end) work? And is
  // it case insensitive?
  ql::ranges::for_each(
      std::vector<MemorySizeAndStringRepresentation>{{42_kB, "42 K"},
                                                     {42_kB, "42 k"},
                                                     {42_MB, "42 M"},
                                                     {42_MB, "42 m"},
                                                     {42_GB, "42 G"},
                                                     {42_GB, "42 g"},
                                                     {42_TB, "42 T"},
                                                     {42_TB, "42 t"}},
      doTest);

  // Check if whitespace between unit and amount is truly optional
  ql::ranges::for_each(
      std::vector<MemorySizeAndStringRepresentation>{{42_B, "42B"},
                                                     {42_B, "42b"},
                                                     {42_kB, "42KB"},
                                                     {42_kB, "42Kb"},
                                                     {42_kB, "42kB"},
                                                     {42_kB, "42kb"},
                                                     {42_MB, "42MB"},
                                                     {42_MB, "42Mb"},
                                                     {42_MB, "42mB"},
                                                     {42_MB, "42mb"},
                                                     {42_GB, "42GB"},
                                                     {42_GB, "42Gb"},
                                                     {42_GB, "42gB"},
                                                     {42_GB, "42gb"},
                                                     {42_TB, "42TB"},
                                                     {42_TB, "42Tb"},
                                                     {42_TB, "42tB"},
                                                     {42_TB, "42tb"}},
      doTest);

  ql::ranges::for_each(
      std::vector<MemorySizeAndStringRepresentation>{{42_kB, "42K"},
                                                     {42_kB, "42k"},
                                                     {42_MB, "42M"},
                                                     {42_MB, "42m"},
                                                     {42_GB, "42G"},
                                                     {42_GB, "42g"},
                                                     {42_TB, "42T"},
                                                     {42_TB, "42t"}},
      doTest);

  // Test if multiple spaces are fine too
  ql::ranges::for_each(
      std::vector<MemorySizeAndStringRepresentation>{{42_kB, "42    K"},
                                                     {42_kB, "42  k"}},
      doTest);

  // We only take memory units up to `TB`. Not further.
  ql::ranges::for_each(std::vector{"42 P", "42 PB"}, doExceptionTest);
}

TEST(MemorySize, ArithmeticOperators) {
  // Addition.
  ASSERT_EQ((2_GB).getBytes(), (1_GB + 1_GB).getBytes());
  ASSERT_EQ((20_TB).getBytes(), (1_TB + 1_TB + 10_TB + 8000_GB).getBytes());
  ad_utility::MemorySize memAddition{4_MB};
  memAddition += 7_MB;
  ASSERT_EQ((11_MB).getBytes(), memAddition.getBytes());
  memAddition += 11000_kB;
  ASSERT_EQ((22_MB).getBytes(), memAddition.getBytes());

  // Subtraction.
  ASSERT_EQ((2_GB).getBytes(), (3_GB - 1_GB).getBytes());
  ASSERT_EQ((12_TB).getBytes(), (31_TB - 1_TB - 10_TB - 8000_GB).getBytes());
  ad_utility::MemorySize memSubtraction{40_MB};
  memSubtraction -= 7_MB;
  ASSERT_EQ((33_MB).getBytes(), memSubtraction.getBytes());
  memSubtraction -= 11000_kB;
  ASSERT_EQ((22_MB).getBytes(), memSubtraction.getBytes());

  // Whole number multiplication.
  ASSERT_EQ((2_GB).getBytes(), (1_GB * 2).getBytes());
  ASSERT_EQ((20_TB).getBytes(), (2 * 1_TB * 10).getBytes());
  ad_utility::MemorySize memWholeMultiplication{40_MB};
  memWholeMultiplication *= 5;
  ASSERT_EQ((200_MB).getBytes(), memWholeMultiplication.getBytes());
  memWholeMultiplication *= 3;
  ASSERT_EQ((600_MB).getBytes(), memWholeMultiplication.getBytes());
  ASSERT_ANY_THROW(1_GB * -2);

  // Floating point multiplication.
  ASSERT_EQ((5_GB).getBytes(), (2_GB * 2.5).getBytes());
  ASSERT_EQ((375_TB).getBytes(), (0.25 * 400_TB * 3.75).getBytes());
  ad_utility::MemorySize memFloatingPointMultiplication{40_MB};
  memFloatingPointMultiplication *= 1.5;
  ASSERT_EQ((60_MB).getBytes(), memFloatingPointMultiplication.getBytes());
  memFloatingPointMultiplication *= 0.2;
  ASSERT_EQ((12_MB).getBytes(), memFloatingPointMultiplication.getBytes());
  ASSERT_ANY_THROW(1_GB * -2.48);

  // Whole number division.
  ASSERT_EQ((2_GB).getBytes(), (4_GB / 2).getBytes());
  ASSERT_EQ((20_TB).getBytes(), (400_TB / 2 / 10).getBytes());
  ad_utility::MemorySize memWholeDivision{600_MB};
  memWholeDivision /= 3;
  ASSERT_EQ((200_MB).getBytes(), memWholeDivision.getBytes());
  memWholeDivision /= 5;
  ASSERT_EQ((40_MB).getBytes(), memWholeDivision.getBytes());
  ASSERT_ANY_THROW(1_GB / -2);
  ASSERT_ANY_THROW(1_GB / 0);

  // Floating point division.
  ASSERT_EQ((2_GB).getBytes(), (5_GB / 2.5).getBytes());
  ASSERT_EQ((400_TB).getBytes(), (375_TB / 0.25 / 3.75).getBytes());
  ad_utility::MemorySize memFloatingPointDivision{12_MB};
  memFloatingPointDivision /= 1.5;
  ASSERT_EQ((8_MB).getBytes(), memFloatingPointDivision.getBytes());
  memFloatingPointDivision /= 0.2;
  ASSERT_EQ((40_MB).getBytes(), memFloatingPointDivision.getBytes());
  ASSERT_ANY_THROW(1_GB / -2.48);
  ASSERT_ANY_THROW(1_GB / 0.);
}

// For checking, if the operators throw errors, when we have over-, or
// underflow.
TEST(MemorySize, ArithmeticOperatorsOverAndUnderFlow) {
  // Addition.
  ASSERT_THROW(
      100_GB + ad_utility::MemorySize::bytes(ad_utility::size_t_max - 400),
      std::overflow_error);
  ASSERT_NO_THROW(ad_utility::MemorySize::bytes(400) +
                  ad_utility::MemorySize::bytes(ad_utility::size_t_max - 400));
  ad_utility::MemorySize memAddition{4_MB};
  ASSERT_THROW(memAddition +=
               ad_utility::MemorySize::bytes(ad_utility::size_t_max - 400),
               std::overflow_error);
  memAddition = ad_utility::MemorySize::bytes(10);
  ASSERT_NO_THROW(memAddition +=
                  ad_utility::MemorySize::bytes(ad_utility::size_t_max - 10));

  // Subtraction.
  ASSERT_THROW(
      100_GB - ad_utility::MemorySize::bytes(ad_utility::size_t_max - 400),
      std::underflow_error);
  ASSERT_NO_THROW(ad_utility::MemorySize::bytes(400) -
                  ad_utility::MemorySize::bytes(400));
  ad_utility::MemorySize memSubtraction{40_MB};
  ASSERT_THROW(memSubtraction -=
               ad_utility::MemorySize::bytes(ad_utility::size_t_max - 400),
               std::underflow_error);
  memSubtraction = ad_utility::MemorySize::bytes(10);
  ASSERT_NO_THROW(memSubtraction -= ad_utility::MemorySize::bytes(10));

  // Whole number multiplication.
  ASSERT_THROW(100_GB * ad_utility::size_t_max, std::overflow_error);
  ASSERT_NO_THROW(ad_utility::MemorySize::bytes(ad_utility::size_t_max / 2UL) *
                  2UL);
  ad_utility::MemorySize memWholeMultiplication{40_MB};
  ASSERT_THROW(memWholeMultiplication *= ad_utility::size_t_max,
               std::overflow_error);
  memWholeMultiplication = ad_utility::MemorySize::max();
  ASSERT_NO_THROW(memWholeMultiplication *= 1);

  // Floating point multiplication.
  ASSERT_THROW(ad_utility::MemorySize::max() * 1.5, std::overflow_error);
  ASSERT_NO_THROW(ad_utility::MemorySize::bytes(static_cast<size_t>(
                      static_cast<float>(ad_utility::size_t_max) / 2.3)) *
                  2.3);
  ad_utility::MemorySize memFloatingPointMultiplication{
      ad_utility::MemorySize::max()};
  ASSERT_THROW(memFloatingPointMultiplication *= 1.487, std::overflow_error);
  memFloatingPointMultiplication = ad_utility::MemorySize::bytes(
      static_cast<size_t>(static_cast<float>(ad_utility::size_t_max) / 4.73));
  ASSERT_NO_THROW(memFloatingPointMultiplication *= 4.73);

  // Floating point division. We are checking for overflow via divisor, that
  // results in a quotient bigger than the dividend. For example: 1/(1/2) = 2
  ASSERT_THROW(100_GB / (1. / static_cast<float>(ad_utility::size_t_max)),
               std::overflow_error);
  ASSERT_NO_THROW(ad_utility::MemorySize::bytes(static_cast<size_t>(
                      static_cast<float>(ad_utility::size_t_max) / 2.4)) /
                  (1. / 2.4));
  ad_utility::MemorySize memFloatingPointDivision{12_MB};
  ASSERT_THROW(memFloatingPointDivision /=
               (1. / static_cast<float>(ad_utility::size_t_max)),
               std::overflow_error);
  memFloatingPointDivision = ad_utility::MemorySize::max();
  ASSERT_NO_THROW(memFloatingPointDivision /= 7.80);
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
  static_assert(ad_utility::MemorySize::max().getBytes() ==
                ad_utility::size_t_max);

  // Comparison operators.
  static_assert(42_B == 42_B);
  static_assert(42_B != 41_B);
  static_assert(42_B < 43_B);
  static_assert(42_B <= 42_B);
  static_assert(42_B <= 43_B);
  static_assert(42_B > 41_B);
  static_assert(42_B >= 42_B);
  static_assert(42_B >= 41_B);
  static_assert(!(42_B == 41_B));
  static_assert(!(42_B != 42_B));
  static_assert(!(42_B < 42_B));
  static_assert(!(42_B <= 41_B));
  static_assert(!(42_B > 43_B));
  static_assert(!(42_B >= 43_B));

  // Addition.
  static_assert((20_TB).getBytes() ==
                (1_TB + 1_TB + 10_TB + 8000_GB).getBytes());
  static_assert((20_TB += 5_TB).getBytes() ==
                (2_TB + 5_TB + 10_TB + 8000_GB).getBytes());

  // Subtraction.
  static_assert((20_TB).getBytes() ==
                (40_TB - 1_TB - 10_TB - 9000_GB).getBytes());
  static_assert((20_TB -= 5_TB).getBytes() ==
                (40_TB - 5_TB - 10_TB - 10000_GB).getBytes());

  // Whole number multiplication.
  static_assert((20_TB).getBytes() == (2 * 1_TB * 10).getBytes());
  static_assert((20_TB *= 5).getBytes() == (4 * 5_TB * 5).getBytes());

  // Floating point multiplication.
  static_assert((5_GB).getBytes() == (2_GB * 2.5).getBytes());
  static_assert((30_TB *= 1.15).getBytes() == (0.15 * 100_TB * 2.3).getBytes());

  // Whole number division.
  static_assert((1_TB).getBytes() == (20_TB / 2 / 10).getBytes());
  static_assert((25_TB /= 5).getBytes() == (100_TB / 4 / 5).getBytes());

  // Floating point division.
  static_assert((2_GB).getBytes() == (5_GB / 2.5).getBytes());
  static_assert((30_TB *= 1.15).getBytes() == (100_TB * 0.15 * 2.3).getBytes());
  static_assert((115_TB /= 1.15).getBytes() ==
                (34.5_TB / 0.15 / 2.3).getBytes());
}
