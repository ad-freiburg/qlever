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
        {std::pair<std::string_view, AllMemoryUnitSizes>{
             "B", AllMemoryUnitSizes{1uL, 1e-3, 1e-6, 1e-9, 1e-12}},
         std::pair<std::string_view, AllMemoryUnitSizes>{
             "kB", AllMemoryUnitSizes{1000uL, 1, 1e-3, 1e-6, 1e-9}},
         std::pair<std::string_view, AllMemoryUnitSizes>{
             "MB", AllMemoryUnitSizes{1000000uL, 1e3, 1, 1e-3, 1e-6}},
         std::pair<std::string_view, AllMemoryUnitSizes>{
             "GB", AllMemoryUnitSizes{1000000000uL, 1e6, 1e3, 1, 1e-3}},
         std::pair<std::string_view, AllMemoryUnitSizes>{
             "TB", AllMemoryUnitSizes{1000000000000uL, 1e9, 1e6, 1e3, 1}}});

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
  ASSERT_ANY_THROW(ad_utility::MemorySize::kilobytes(ad_utility::size_t_max));
  ASSERT_ANY_THROW(ad_utility::MemorySize::megabytes(ad_utility::size_t_max));
  ASSERT_ANY_THROW(ad_utility::MemorySize::gigabytes(ad_utility::size_t_max));
  ASSERT_ANY_THROW(ad_utility::MemorySize::terabytes(ad_utility::size_t_max));
  ASSERT_ANY_THROW(ad_utility::MemorySize::kilobytes(DBL_MAX));
  ASSERT_ANY_THROW(ad_utility::MemorySize::megabytes(DBL_MAX));
  ASSERT_ANY_THROW(ad_utility::MemorySize::gigabytes(DBL_MAX));
  ASSERT_ANY_THROW(ad_utility::MemorySize::terabytes(DBL_MAX));
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
struct MemorySizeInByteAndStringRepresentation {
  size_t memoryAmount_;
  std::string stringRepresentation_;
};

static std::vector<MemorySizeInByteAndStringRepresentation>
generalAsStringTestCases() {
  return {{(50_B).getBytes(), "50 B"},     {(1_kB).getBytes(), "1000 B"},
          {(200_kB).getBytes(), "200 kB"}, {(150.5_kB).getBytes(), "150.5 kB"},
          {(2_MB).getBytes(), "2 MB"},     {(1.5_MB).getBytes(), "1.5 MB"},
          {(2_GB).getBytes(), "2 GB"},     {(1.5_GB).getBytes(), "1.5 GB"},
          {(2_TB).getBytes(), "2 TB"},     {(1.5_TB).getBytes(), "1.5 TB"}};
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

  // Check, if it always uses the right unit.
  doTest({(99999_B).getBytes(), "99999 B"});
  doTest({(100000_B).getBytes(), "100 kB"});
  doTest({(400000_B).getBytes(), "400 kB"});
  doTest({(4000_kB).getBytes(), "4 MB"});
  doTest({(4000_MB).getBytes(), "4 GB"});
  doTest({(4000_GB).getBytes(), "4 TB"});
}

TEST(MemorySize, Parse) {
  // Parse the given string and compare to the expected amount of bytes.
  auto doTest = [](const MemorySizeInByteAndStringRepresentation& testCase) {
    ASSERT_EQ(ad_utility::MemorySize::parse(testCase.stringRepresentation_)
                  .getBytes(),
              testCase.memoryAmount_);
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
  std::ranges::for_each(
      generalAsStringTestCases(), doExceptionTest,
      [](const MemorySizeInByteAndStringRepresentation& testCase) {
        return absl::StrCat("-", testCase.stringRepresentation_);
      });

  // byte sizes can only be set with `B`.
  std::ranges::for_each(std::vector{"42 BYTE", "42 BYTe", "42 BYtE", "42 BYte",
                                    "42 ByTE", "42 ByTe", "42 BytE", "42 Byte",
                                    "42 bYTE", "42 bYTe", "42 bYtE", "42 bYte",
                                    "42 byTE", "42 byTe", "42 bytE", "42 byte"},
                        doExceptionTest);

  // Is our grammar truly case insensitive?
  std::ranges::for_each(
      std::vector<MemorySizeInByteAndStringRepresentation>{
          {(42_B).getBytes(), "42 B"},
          {(42_B).getBytes(), "42 b"},
          {(42_kB).getBytes(), "42 KB"},
          {(42_kB).getBytes(), "42 Kb"},
          {(42_kB).getBytes(), "42 kB"},
          {(42_kB).getBytes(), "42 kb"},
          {(42_MB).getBytes(), "42 MB"},
          {(42_MB).getBytes(), "42 Mb"},
          {(42_MB).getBytes(), "42 mB"},
          {(42_MB).getBytes(), "42 mb"},
          {(42_GB).getBytes(), "42 GB"},
          {(42_GB).getBytes(), "42 Gb"},
          {(42_GB).getBytes(), "42 gB"},
          {(42_GB).getBytes(), "42 gb"},
          {(42_TB).getBytes(), "42 TB"},
          {(42_TB).getBytes(), "42 Tb"},
          {(42_TB).getBytes(), "42 tB"},
          {(42_TB).getBytes(), "42 tb"}},
      doTest);
}
