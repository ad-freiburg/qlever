// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <string_view>

#include "util/FastIntToString.h"

namespace {
using namespace ad_utility;

// _____________________________________________________________________________
TEST(FastIntToStringTest, NumDigitsUInt64) {
  EXPECT_EQ(numDigits(0ULL), 1U);
  EXPECT_EQ(numDigits(1ULL), 1U);
  EXPECT_EQ(numDigits(9ULL), 1U);
  EXPECT_EQ(numDigits(10ULL), 2U);
  EXPECT_EQ(numDigits(99ULL), 2U);
  EXPECT_EQ(numDigits(100ULL), 3U);
  EXPECT_EQ(numDigits(999ULL), 3U);
  EXPECT_EQ(numDigits(1000ULL), 4U);
  EXPECT_EQ(numDigits(9999ULL), 4U);
  EXPECT_EQ(numDigits(10000ULL), 5U);
  EXPECT_EQ(numDigits(99999ULL), 5U);
  EXPECT_EQ(numDigits(100000ULL), 6U);
  EXPECT_EQ(numDigits(999999ULL), 6U);
  EXPECT_EQ(numDigits(1000000ULL), 7U);
  EXPECT_EQ(numDigits(9999999ULL), 7U);
  EXPECT_EQ(numDigits(10000000ULL), 8U);
  EXPECT_EQ(numDigits(99999999ULL), 8U);
  EXPECT_EQ(numDigits(100000000ULL), 9U);
  EXPECT_EQ(numDigits(999999999ULL), 9U);
  EXPECT_EQ(numDigits(1000000000ULL), 10U);
  EXPECT_EQ(numDigits(9999999999ULL), 10U);
  EXPECT_EQ(numDigits(10000000000ULL), 11U);
  EXPECT_EQ(numDigits(99999999999ULL), 11U);
  EXPECT_EQ(numDigits(100000000000ULL), 12U);
  EXPECT_EQ(numDigits(999999999999ULL), 12U);
  EXPECT_EQ(numDigits(1000000000000ULL), 13U);
  EXPECT_EQ(numDigits(9999999999999ULL), 13U);
  EXPECT_EQ(numDigits(10000000000000ULL), 14U);
  EXPECT_EQ(numDigits(99999999999999ULL), 14U);
  EXPECT_EQ(numDigits(100000000000000ULL), 15U);
  EXPECT_EQ(numDigits(999999999999999ULL), 15U);
  EXPECT_EQ(numDigits(1000000000000000ULL), 16U);
  EXPECT_EQ(numDigits(9999999999999999ULL), 16U);
  EXPECT_EQ(numDigits(10000000000000000ULL), 17U);
  EXPECT_EQ(numDigits(99999999999999999ULL), 17U);
  EXPECT_EQ(numDigits(100000000000000000ULL), 18U);
  EXPECT_EQ(numDigits(999999999999999999ULL), 18U);
  EXPECT_EQ(numDigits(1000000000000000000ULL), 19U);
  EXPECT_EQ(numDigits(9999999999999999999ULL), 19U);
  EXPECT_EQ(numDigits(10000000000000000000ULL), 20U);
  EXPECT_EQ(numDigits(std::numeric_limits<uint64_t>::max()), 20U);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, NumDigitsUInt32) {
  EXPECT_EQ(numDigits(0U), 1U);
  EXPECT_EQ(numDigits(1U), 1U);
  EXPECT_EQ(numDigits(9U), 1U);
  EXPECT_EQ(numDigits(10U), 2U);
  EXPECT_EQ(numDigits(99U), 2U);
  EXPECT_EQ(numDigits(100U), 3U);
  EXPECT_EQ(numDigits(999U), 3U);
  EXPECT_EQ(numDigits(1000U), 4U);
  EXPECT_EQ(numDigits(9999U), 4U);
  EXPECT_EQ(numDigits(10000U), 5U);
  EXPECT_EQ(numDigits(99999U), 5U);
  EXPECT_EQ(numDigits(100000U), 6U);
  EXPECT_EQ(numDigits(999999U), 6U);
  EXPECT_EQ(numDigits(1000000U), 7U);
  EXPECT_EQ(numDigits(9999999U), 7U);
  EXPECT_EQ(numDigits(10000000U), 8U);
  EXPECT_EQ(numDigits(99999999U), 8U);
  EXPECT_EQ(numDigits(100000000U), 9U);
  EXPECT_EQ(numDigits(999999999U), 9U);
  EXPECT_EQ(numDigits(1000000000U), 10U);
  EXPECT_EQ(numDigits(std::numeric_limits<uint32_t>::max()), 10U);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, CountlZero64) {
  // Powers of two pin down every bit position.
  for (int k = 0; k < 64; ++k) {
    EXPECT_EQ(detail::countlZero64(1ULL << k), static_cast<uint32_t>(63 - k))
        << "Failed for 1ULL << " << k;
  }
  EXPECT_EQ(detail::countlZero64(std::numeric_limits<uint64_t>::max()), 0U);
  EXPECT_EQ(detail::countlZero64(0xFFFFFFFF00000000ULL), 0U);
  EXPECT_EQ(detail::countlZero64(0x00000000FFFFFFFFULL), 32U);
  EXPECT_EQ(detail::countlZero64(0x00FF000000000000ULL), 8U);
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatUIntBranchless) {
  char buf[64];

  auto testValue = [&](uint64_t val) {
    char* end = formatUIntBranchless(val, buf);
    std::string expected = std::to_string(val);
    std::string actual(buf, end - buf);
    EXPECT_EQ(actual, expected) << "Failed on value: " << val;
  };

  // Boundary cases
  testValue(0ULL);
  testValue(1ULL);
  testValue(9ULL);
  testValue(10ULL);
  testValue(42ULL);
  testValue(99ULL);
  testValue(100ULL);
  testValue(999ULL);
  testValue(1000ULL);
  testValue(9999ULL);
  testValue(10000ULL);
  testValue(99999ULL);
  testValue(100000ULL);
  testValue(999999ULL);
  testValue(1000000ULL);
  testValue(9999999ULL);
  testValue(10000000ULL);
  testValue(99999999ULL);
  testValue(100000000ULL);
  testValue(999999999ULL);
  testValue(1000000000ULL);
  testValue(123456789ULL);
  testValue(9999999999ULL);
  testValue(10000000000ULL);
  testValue(123456789012345678ULL);
  testValue(10000000000000000000ULL);
  testValue(std::numeric_limits<uint64_t>::max());

  // Randomized tests across full 64-bit space
  std::mt19937_64 gen(42);
  for (size_t i = 0; i < 50000; ++i) {
    testValue(gen());
  }
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatUInt32Branchless) {
  char buf[64];

  auto testValue = [&](uint32_t val) {
    char* end = formatUInt32Branchless(val, buf);
    std::string expected = std::to_string(val);
    std::string actual(buf, end - buf);
    EXPECT_EQ(actual, expected) << "Failed on 32-bit value: " << val;
  };

  testValue(0U);
  testValue(1U);
  testValue(42U);
  testValue(99U);
  testValue(100U);
  testValue(12345U);
  testValue(123456789U);
  testValue(std::numeric_limits<uint32_t>::max());

  std::mt19937 gen(1337);
  for (size_t i = 0; i < 10000; ++i) {
    testValue(gen());
  }
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatIntBranchless) {
  char buf[64];

  auto testValue = [&](int64_t val) {
    char* end = formatIntBranchless(val, buf);
    std::string expected = std::to_string(val);
    std::string actual(buf, end - buf);
    EXPECT_EQ(actual, expected) << "Failed on int64 value: " << val;
  };

  testValue(0);
  testValue(1);
  testValue(-1);
  testValue(42);
  testValue(-42);
  testValue(100);
  testValue(-100);
  testValue(123456789);
  testValue(-123456789);
  testValue(std::numeric_limits<int64_t>::max());
  testValue(std::numeric_limits<int64_t>::min());

  std::mt19937_64 gen(999);
  for (size_t i = 0; i < 50000; ++i) {
    testValue(static_cast<int64_t>(gen()));
  }
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatInt32Branchless) {
  char buf[64];

  auto testValue = [&](int32_t val) {
    char* end = formatInt32Branchless(val, buf);
    std::string expected = std::to_string(val);
    std::string actual(buf, end - buf);
    EXPECT_EQ(actual, expected) << "Failed on int32 value: " << val;
  };

  testValue(0);
  testValue(1);
  testValue(-1);
  testValue(42);
  testValue(-42);
  testValue(100);
  testValue(-100);
  testValue(std::numeric_limits<int32_t>::max());
  testValue(std::numeric_limits<int32_t>::min());

  std::mt19937 gen(2026);
  for (size_t i = 0; i < 10000; ++i) {
    testValue(static_cast<int32_t>(gen()));
  }
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatQidAndPid) {
  char buf[128];

  // QID tests
  {
    char* end = formatQid(42ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf),
              "http://www.wikidata.org/entity/Q42");
  }
  {
    char* end = formatQid(1234567ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf),
              "http://www.wikidata.org/entity/Q1234567");
  }
  {
    char* end = formatQid(115858349ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf),
              "http://www.wikidata.org/entity/Q115858349");
  }

  // PID tests
  {
    char* end = formatPid(31ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf),
              "http://www.wikidata.org/prop/direct/P31");
  }

  // Custom prefix tests
  {
    char* end = formatPrefixedId("http://example.org/item/", 98765ULL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf),
              "http://example.org/item/98765");
  }
  {
    char* end = formatPrefixedInt("http://example.org/temp/-", 555LL, buf);
    EXPECT_EQ(std::string_view(buf, end - buf), "http://example.org/temp/-555");
  }
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, FormatDetailFixedDigits) {
  char buf[16];

  // 8 digits formatting
  detail::format8Digits(12345678U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "12345678");

  detail::format8Digits(42U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "00000042");

  detail::format8Digits(0U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "00000000");

  detail::format8Digits(99999999U, buf);
  EXPECT_EQ(std::string_view(buf, 8), "99999999");

  // 4 digits formatting
  detail::format4Digits(1234U, buf);
  EXPECT_EQ(std::string_view(buf, 4), "1234");

  detail::format4Digits(7U, buf);
  EXPECT_EQ(std::string_view(buf, 4), "0007");

  // 2 digits formatting
  detail::format2Digits(42U, buf);
  EXPECT_EQ(std::string_view(buf, 2), "42");

  detail::format2Digits(5U, buf);
  EXPECT_EQ(std::string_view(buf, 2), "05");
}

// _____________________________________________________________________________
TEST(FastIntToStringTest, StringWrappers) {
  EXPECT_EQ(formatUIntToString(42ULL), "42");
  EXPECT_EQ(formatUIntToString(1234567890ULL), "1234567890");
  EXPECT_EQ(formatIntToString(-987654321LL), "-987654321");
  EXPECT_EQ(formatIntToString(0LL), "0");
  EXPECT_EQ(formatQidToString(42ULL), "http://www.wikidata.org/entity/Q42");
}

}  // namespace
