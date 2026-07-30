// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "index/EncodedIriNibbleEncoding.h"
#include "util/Random.h"

namespace {
// The encoder with the number of bits that the default `EncodedIriManager`
// uses.
using Encoder = encodedIris::NibbleEncoder<52>;

// ____________________________________________________________________________
TEST(NibbleEncoder, numDigits) {
  EXPECT_EQ(Encoder::NibbleSize, 4u);
  EXPECT_EQ(Encoder::NumDigits, 13u);
  EXPECT_EQ((encodedIris::NibbleEncoder<32>::NumDigits), 8u);
}

// ____________________________________________________________________________
TEST(NibbleEncoder, encode) {
  // Each digit `i` is encoded as the nibble `i + 1`, left-aligned in the 32
  // bits, and filled on the right with zeroes. See the examples in
  // `EncodedIriNibbleEncoding.h`.
  using SmallEncoder = encodedIris::NibbleEncoder<32>;
  EXPECT_EQ(SmallEncoder::encode("1"), 0x20000000u);
  EXPECT_EQ(SmallEncoder::encode("10"), 0x21000000u);
  EXPECT_EQ(SmallEncoder::encode("100"), 0x21100000u);
  EXPECT_EQ(SmallEncoder::encode("2"), 0x30000000u);
  EXPECT_EQ(SmallEncoder::encode("20"), 0x31000000u);
  // The encoded values are in the same order as the digit strings.
  EXPECT_LT(SmallEncoder::encode("1"), SmallEncoder::encode("10"));
  EXPECT_LT(SmallEncoder::encode("10"), SmallEncoder::encode("100"));
  EXPECT_LT(SmallEncoder::encode("100"), SmallEncoder::encode("2"));
  EXPECT_LT(SmallEncoder::encode("2"), SmallEncoder::encode("20"));
}

// ____________________________________________________________________________
TEST(NibbleEncoder, roundTrip) {
  auto testNumber = [](uint64_t number, ad_utility::source_location l =
                                            AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(number,
              Encoder::decode(Encoder::encode(std::to_string(number)).value()));
  };
  uint64_t MAX = std::stoull(std::string(Encoder::NumDigits, '9'));
  testNumber(0);
  testNumber(MAX);
  auto intGenerator = ad_utility::SlowRandomIntGenerator<uint64_t>(0, MAX);
  for (auto _ = 0; _ < 20; ++_) {
    testNumber(intGenerator());
  }
}

// ____________________________________________________________________________
TEST(NibbleEncoder, decodeToStringPreservesLeadingZeros) {
  // In contrast to `decode`, the string-based decoding recovers the digits
  // exactly, including leading zeros.
  std::string result;
  Encoder::decodeToString(result, Encoder::encode("007").value());
  EXPECT_EQ(result, "007");
  EXPECT_EQ(Encoder::decode(Encoder::encode("007").value()), 7u);
}

// ____________________________________________________________________________
TEST(NibbleEncoder, toStringWithGivenPrefix) {
  EXPECT_EQ(Encoder::toStringWithGivenPrefix(Encoder::encode("7643").value(),
                                             "<blibb_"),
            "<blibb_7643>");
}

// ____________________________________________________________________________
// Regression test: a payload of `0` never comes from this encoding (which
// always sets the highest nibble), but a prefix with constraints can produce
// it, and the payload of a corrupted index can be anything. The decoding must
// terminate and not loop into a `size_t` underflow.
TEST(NibbleEncoder, decodeZero) {
  EXPECT_EQ(Encoder::decode(uint64_t{0}), 0u);
  std::string decoded;
  Encoder::decodeToString(decoded, uint64_t{0});
  EXPECT_TRUE(decoded.empty());
}

}  // namespace
