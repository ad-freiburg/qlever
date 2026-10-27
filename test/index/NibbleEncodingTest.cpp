// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <vector>

#include "../util/GTestHelpers.h"
#include "index/vocabulary/NibbleEncoding.h"
#include "util/Random.h"

using namespace encodedIri;
using namespace ::testing;

namespace {
// Expect that the `digits` are encoded into `numBits` bits as `expected`, and
// that both ways of decoding yield the `digits` again.
void expectRoundTrip(std::string_view digits, size_t numBits, uint64_t expected,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  uint64_t encoded = encodeDigitsAsNibbles(digits, numBits);
  EXPECT_EQ(encoded, expected);
  std::string decoded = "prefix";
  decodeNibblesToDigits(decoded, encoded, numBits);
  EXPECT_EQ(decoded, absl::StrCat("prefix", digits));
  EXPECT_EQ(decodeNibblesToNumber(encoded, numBits),
            digits.empty() ? 0 : std::stoull(std::string{digits}));
}
}  // namespace

// _____________________________________________________________________________
TEST(NibbleEncoding, examplesFromTheDocumentation) {
  // The examples at the top of `NibbleEncoding.h`.
  expectRoundTrip("1", 32, 0x20000000);
  expectRoundTrip("10", 32, 0x21000000);
  expectRoundTrip("100", 32, 0x21100000);
  expectRoundTrip("2", 32, 0x30000000);
  expectRoundTrip("20", 32, 0x31000000);
}

// _____________________________________________________________________________
TEST(NibbleEncoding, encodeAndDecode) {
  // The digit `d` is stored as `d + 1`, left-aligned in the `numBits` bits.
  expectRoundTrip("0", 8, 0x10);
  expectRoundTrip("9", 8, 0xA0);
  expectRoundTrip("09", 8, 0x1A);
  expectRoundTrip("90", 8, 0xA1);
  expectRoundTrip("007", 64, 0x1180000000000000);
  expectRoundTrip("7", 64, 0x8000000000000000);
  // Exactly `numBits / NibbleSize` digits fill all the bits.
  expectRoundTrip("1234", 16, 0x2345);
  expectRoundTrip("9999999999999999", 64, 0xAAAAAAAAAAAAAAAA);
  // The result is `constexpr`.
  static_assert(encodeDigitsAsNibbles("12", 8) == 0x23);
}

// _____________________________________________________________________________
TEST(NibbleEncoding, preservesTheLexicographicOrder) {
  // Sort a set of digit sequences (including ones with leading zeros)
  // lexicographically and check that their encodings are strictly increasing.
  std::vector<std::string> digitSequences{
      "0",  "00", "000", "001", "01", "1", "10", "100", "1000", "101",
      "11", "2",  "20",  "3",   "33", "9", "90", "99",  "999",  "9999"};
  ASSERT_TRUE(ql::ranges::is_sorted(digitSequences));
  for (size_t i = 1; i < digitSequences.size(); ++i) {
    EXPECT_LT(encodeDigitsAsNibbles(digitSequences[i - 1], 16),
              encodeDigitsAsNibbles(digitSequences[i], 16))
        << digitSequences[i - 1] << " vs " << digitSequences[i];
  }
}

// _____________________________________________________________________________
TEST(NibbleEncoding, randomRoundTrip) {
  // Ten digits (with leading zeros) always fit into 40 bits.
  ad_utility::SlowRandomIntGenerator<uint64_t> gen(0, 9'999'999'999ull);
  for (size_t i = 0; i < 1000; ++i) {
    uint64_t number = gen();
    std::string digits = std::to_string(number);
    std::string padded = std::string(10 - digits.size(), '0') + digits;
    for (const auto& sequence : {digits, padded}) {
      uint64_t encoded = encodeDigitsAsNibbles(sequence, 40);
      std::string decoded;
      decodeNibblesToDigits(decoded, encoded, 40);
      EXPECT_EQ(decoded, sequence);
      EXPECT_EQ(decodeNibblesToNumber(encoded, 40), number);
    }
  }
}

// _____________________________________________________________________________
TEST(NibbleEncoding, tooManyDigits) {
  // Eight bits hold exactly two nibbles, so two digits fit and three do not.
  EXPECT_NO_THROW(encodeDigitsAsNibbles("12", 8));
  EXPECT_THROW(encodeDigitsAsNibbles("123", 8), std::out_of_range);
  EXPECT_NO_THROW(encodeDigitsAsNibbles("1234567890123456", 64));
  EXPECT_THROW(encodeDigitsAsNibbles("12345678901234567", 64),
               std::out_of_range);
}

// _____________________________________________________________________________
TEST(NibbleEncoding, decodeEmptyDigitSequence) {
  // The encoding of an empty sequence of digits consists of padding nibbles
  // only, so it is `0`. Decoding it must yield no digits at all.
  EXPECT_EQ(encodeDigitsAsNibbles("", 64), 0U);
  std::string result = "prefix";
  decodeNibblesToDigits(result, 0, 64);
  EXPECT_EQ(result, "prefix");
  EXPECT_EQ(decodeNibblesToNumber(0, 64), 0U);
  expectRoundTrip("", 8, 0);
}

// _____________________________________________________________________________
TEST(NibbleEncoding, invalidNumBits) {
  // `numBits` must be at least `NibbleSize` and at most 64. The two calls
  // violate the left and the right operand of that condition, respectively.
  // Both report the same (stringified) condition.
  AD_EXPECT_THROW_WITH_MESSAGE(
      decodeNibblesToNumber(0, 2),
      HasSubstr("numBits >= NibbleSize && numBits <= 64"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      decodeNibblesToNumber(0, 68),
      HasSubstr("numBits >= NibbleSize && numBits <= 64"));
}

// _____________________________________________________________________________
TEST(NibbleEncoding, encodedValueTooLarge) {
  // The `encoded` value must fit into the `numBits` bits.
  AD_EXPECT_THROW_WITH_MESSAGE(
      decodeNibblesToNumber(1ull << 8, 8),
      HasSubstr("encoded <= ad_utility::bitMaskForLowerBits(numBits)"));
  EXPECT_NO_THROW(decodeNibblesToNumber(0xAA, 8));
}

// _____________________________________________________________________________
TEST(NibbleEncoding, invalidNibbleValue) {
  // Every non-padding nibble must lie in `[1, 10]`, because the digit `d` is
  // stored as `d + 1`. The two calls violate the left and the right operand of
  // that condition, respectively; both report the same (stringified)
  // condition.
  //
  // The first value has a `0` nibble that is not padding (the trailing `1`
  // makes it an interior nibble), the second one has a nibble of `15`.
  AD_EXPECT_THROW_WITH_MESSAGE(
      decodeNibblesToNumber((uint64_t{2} << 60) | 1, 64),
      HasSubstr("nibble >= 1 && nibble <= 10"));
  AD_EXPECT_THROW_WITH_MESSAGE(decodeNibblesToNumber(uint64_t{0xF} << 60, 64),
                               HasSubstr("nibble >= 1 && nibble <= 10"));
}
