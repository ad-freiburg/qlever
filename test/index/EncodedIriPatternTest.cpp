// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "index/vocabulary/EncodedIriPattern.h"
#include "util/BitUtils.h"
#include "util/Random.h"

using namespace encodedIri;
using namespace ::testing;

namespace {
// The number of payload bits that the `EncodedIriManager` has available with
// its default configuration.
constexpr size_t numBitsAvailable = 52;

// The patterns that are used in the tests below. They are modelled after the
// IRIs of a real-world dataset: `range_` and `valRange_` consist of a 32-bit
// number, the three highest bits of which are always `001`, followed by two
// smaller numbers and a fixed letter. The `ref_` patterns consist of a 64-bit
// number, the three highest bits of which are always `001` and the bits
// `[17, 32)` of which are always zero, followed by a number smaller than 16.
// The numbers `545554944` and `2343140642651111426` that appear in the tests
// fulfill these constraints; the numbers of the negative test cases are
// deliberate perturbations of them.
//
// NOTE: The prefixes have a leading `<`, because the functions in
// `EncodedIriPattern.h` work on complete IRIs. In the public configuration the
// `<` is added by the `EncodedIriManager`.
constexpr std::string_view basePrefix = "<http://example.org/map#";

// Build the IRI `<http://example.org/map#rest>`.
std::string mapIri(std::string_view rest) {
  return absl::StrCat(basePrefix, rest, ">");
}

Pattern rangePattern() {
  return Pattern{
      absl::StrCat(basePrefix, "range_"),
      {Part{32, {{29, 32, 1}}, "_"}, Part{8, {}, "_"}, Part{8, {}, "P"}}};
}

Pattern valRangePattern() {
  return Pattern{
      absl::StrCat(basePrefix, "valRange_"),
      {Part{32, {{29, 32, 1}}, "_"}, Part{11, {}, "_"}, Part{11, {}, "M"}}};
}

Pattern refPattern(std::string_view name) {
  return Pattern{absl::StrCat(basePrefix, name),
                 {Part{64, {{17, 32, 0}, {61, 64, 1}}, "_"}, Part{4, {}, ""}}};
}

// The part of the `iri` that follows the `prefix_` of the `pattern`, which is
// the input to `encodePayload`.
std::string_view restOfIri(const Pattern& pattern, std::string_view iri) {
  AD_CONTRACT_CHECK(ql::starts_with(iri, pattern.prefix_));
  return iri.substr(pattern.prefix_.size());
}

// Expect that the `iri` matches the `pattern`, that the resulting payload fits
// into the `pattern.numBitsStored()` bits, and that decoding the payload
// yields the `iri` again.
void expectRoundTrip(const Pattern& pattern, const std::string& iri,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto payload = encodePayload(pattern, restOfIri(pattern, iri));
  ASSERT_TRUE(payload.has_value());
  EXPECT_LE(payload.value(),
            ad_utility::bitMaskForLowerBits(pattern.numBitsStored()));
  EXPECT_EQ(decodeToIri(pattern, payload.value()), iri);
}

// Expect that the `iri` (which has to start with the `prefix_` of the
// `pattern`) doesn't match the `pattern`.
void expectNotEncodable(
    const Pattern& pattern, const std::string& iri,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_FALSE(encodePayload(pattern, restOfIri(pattern, iri)).has_value());
}
}  // namespace

// _____________________________________________________________________________
TEST(EncodedIriPattern, numBitsStored) {
  EXPECT_EQ(Part{}.numBitsStored(), 64);
  EXPECT_EQ((Part{16, {}, ""}).numBitsStored(), 16);
  EXPECT_EQ((Part{32, {{29, 32, 1}}, "_"}).numBitsStored(), 29);
  EXPECT_EQ((Part{64, {{17, 32, 0}, {61, 64, 1}}, "_"}).numBitsStored(), 46);

  EXPECT_EQ(Pattern{}.numBitsStored(), 0);
  EXPECT_EQ(rangePattern().numBitsStored(), 29 + 8 + 8);
  EXPECT_EQ(refPattern("ref_").numBitsStored(), 46 + 4);
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, plainPrefixPattern) {
  Pattern plain = plainPrefixPattern("<http://example.org/id/", 32);
  EXPECT_EQ(plain.prefix_, "<http://example.org/id/");
  EXPECT_THAT(plain.parts_,
              ElementsAre(Part{32, {}, "", NumberEncoding::Nibbles}));
  EXPECT_EQ(plain.numBitsStored(), 32);
  EXPECT_NO_THROW(validatePattern(plain, numBitsAvailable));

  // Leading zeros are preserved by the nibble encoding, and at most eight
  // digits fit into 32 bits.
  expectRoundTrip(plain, "<http://example.org/id/12345>");
  expectRoundTrip(plain, "<http://example.org/id/000123>");
  expectRoundTrip(plain, "<http://example.org/id/0>");
  expectRoundTrip(plain, "<http://example.org/id/99999999>");
  expectNotEncodable(plain, "<http://example.org/id/123456789>");
  expectNotEncodable(plain, "<http://example.org/id/>");
  expectNotEncodable(plain, "<http://example.org/id/123x>");

  // A plain prefix pattern is only recognized with the `numBits` it was
  // created with, and every deviation from the shape of `plain` makes a
  // pattern non-plain.
  EXPECT_TRUE(isPlainPrefixPattern(plain, 32));
  EXPECT_FALSE(isPlainPrefixPattern(plain, 16));
  EXPECT_FALSE(isPlainPrefixPattern(plain, 64));
  EXPECT_FALSE(isPlainPrefixPattern(rangePattern(), 32));
  EXPECT_FALSE(isPlainPrefixPattern(Pattern{"<p", {}}, 32));
  EXPECT_FALSE(isPlainPrefixPattern(Pattern{"<p", {Part{32, {}, ""}}}, 32));
  EXPECT_FALSE(isPlainPrefixPattern(
      Pattern{"<p", {Part{32, {}, "_", NumberEncoding::Nibbles}}}, 32));
  EXPECT_FALSE(isPlainPrefixPattern(
      Pattern{"<p", {Part{32, {{0, 4, 0}}, "", NumberEncoding::Nibbles}}}, 32));
  EXPECT_FALSE(
      isPlainPrefixPattern(Pattern{"<p",
                                   {Part{16, {}, "", NumberEncoding::Nibbles},
                                    Part{16, {}, "", NumberEncoding::Nibbles}}},
                           32));
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, leadingDigits) {
  EXPECT_EQ(leadingDigits(""), "");
  EXPECT_EQ(leadingDigits("abc"), "");
  EXPECT_EQ(leadingDigits("123"), "123");
  EXPECT_EQ(leadingDigits("0123_456"), "0123");
  EXPECT_EQ(leadingDigits("9>"), "9");
  EXPECT_EQ(leadingDigits("_12"), "");
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, parseDecimal) {
  EXPECT_THAT(parseDecimal("0"), Optional(0));
  EXPECT_THAT(parseDecimal("1"), Optional(1));
  EXPECT_THAT(parseDecimal("12345"), Optional(12345));
  EXPECT_THAT(parseDecimal("18446744073709551615"),
              Optional(std::numeric_limits<uint64_t>::max()));

  // The empty string and leading zeros are rejected, because they cannot be
  // reconstructed from the binary value.
  EXPECT_EQ(parseDecimal(""), std::nullopt);
  EXPECT_EQ(parseDecimal("00"), std::nullopt);
  EXPECT_EQ(parseDecimal("01"), std::nullopt);
  EXPECT_EQ(parseDecimal("0123"), std::nullopt);

  // Numbers that don't fit into a `uint64_t`.
  EXPECT_EQ(parseDecimal("18446744073709551616"), std::nullopt);
  EXPECT_EQ(parseDecimal("99999999999999999999"), std::nullopt);
  EXPECT_EQ(parseDecimal("123456789012345678901234567890"), std::nullopt);

  // Non-digits are not accepted, not even as a sign.
  EXPECT_EQ(parseDecimal("12a"), std::nullopt);
  EXPECT_EQ(parseDecimal("-1"), std::nullopt);
  EXPECT_EQ(parseDecimal("+1"), std::nullopt);
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, compressAndDecompressNumber) {
  // Expect that the `value` is compressed to the `compressed` value, and that
  // decompressing the latter yields the `value` again.
  auto expectCompressed =
      [](const Part& part, uint64_t value, uint64_t compressed,
         ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
        auto trace = generateLocationTrace(l);
        EXPECT_THAT(compressNumber(part, value), Optional(compressed));
        EXPECT_EQ(decompressNumber(part, compressed), value);
        std::string asString = "prefix";
        decompressNumber(asString, part, compressed);
        EXPECT_EQ(asString, absl::StrCat("prefix", value));
      };
  // Expect that the `value` violates the constraints of the `part`.
  auto expectNotCompressible = [](const Part& part, uint64_t value,
                                  ad_utility::source_location l =
                                      AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(compressNumber(part, value), std::nullopt);
  };

  // Without constraints the number is stored as it is.
  Part plain{16, {}, ""};
  expectCompressed(plain, 0, 0);
  expectCompressed(plain, 12345, 12345);
  expectCompressed(plain, (1 << 16) - 1, (1 << 16) - 1);
  expectNotCompressible(plain, 1 << 16);

  // The full 64 bits without constraints.
  Part full{64, {}, ""};
  expectCompressed(full, std::numeric_limits<uint64_t>::max(),
                   std::numeric_limits<uint64_t>::max());

  // A fixed range at the very beginning and one at the very end.
  Part part{16, {{0, 4, 3}, {12, 16, 5}}, ""};
  EXPECT_EQ(part.numBitsStored(), 8);
  uint64_t value = (5ull << 12) | (0xabull << 4) | 3ull;
  expectCompressed(part, value, 0xab);
  // The fixed ranges have the wrong value.
  expectNotCompressible(part, value + 1);
  expectNotCompressible(part, value ^ (1ull << 15));
  // The value is too large for the 16 bits.
  expectNotCompressible(part, value | (1ull << 16));

  // Adjacent fixed ranges.
  Part adjacent{12, {{4, 6, 1}, {6, 8, 2}}, ""};
  uint64_t adjacentValue = (0xaull << 8) | (2ull << 6) | (1ull << 4) | 0xbull;
  expectCompressed(adjacent, adjacentValue, 0xab);

  // The parts of the `ref_` pattern, for which the constraints span the bits
  // `[17, 32)` and `[61, 64)`.
  Part ref = refPattern("ref_").parts_.at(0);
  expectCompressed(ref, 1ull << 61, 0);
  expectCompressed(ref, (1ull << 61) | 5, 5);
  expectCompressed(ref, (1ull << 61) | (1ull << 32), 1ull << 17);
  expectNotCompressible(ref, 5);
  expectNotCompressible(ref, (1ull << 61) | (1ull << 20));
  expectNotCompressible(ref, (1ull << 62));
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, PatternWithSeveralNumbers) {
  Pattern range = rangePattern();
  expectRoundTrip(range, mapIri("range_545554944_0_0P"));
  expectRoundTrip(range, mapIri("range_536870912_50_25P"));
  expectRoundTrip(range, mapIri("range_1073741823_255_255P"));

  // The three highest bits of the first number are not `001`.
  expectNotEncodable(range, mapIri("range_1073741824_0_0P"));
  expectNotEncodable(range, mapIri("range_100_0_0P"));
  // The second and third number don't fit into eight bits.
  expectNotEncodable(range, mapIri("range_545554944_256_0P"));
  expectNotEncodable(range, mapIri("range_545554944_0_256P"));
  // The trailing `P` and the third number are missing.
  expectNotEncodable(range, mapIri("range_545554944_0_0"));
  expectNotEncodable(range, mapIri("range_545554944_0P"));
  // Something follows the trailing `P`.
  expectNotEncodable(range, mapIri("range_545554944_0_0P1"));
  expectNotEncodable(range, mapIri("range_545554944_0_0PP"));
  // The closing `>` is missing.
  EXPECT_FALSE(encodePayload(range, "545554944_0_0P").has_value());
  // A number is missing.
  expectNotEncodable(range, mapIri("range__0_0P"));
  expectNotEncodable(range, mapIri("range_545554944__0P"));
  // Leading zeros cannot be encoded in binary mode, because they would be lost.
  expectNotEncodable(range, mapIri("range_545554944_00_0P"));
  expectNotEncodable(range, mapIri("range_0545554944_0_0P"));

  Pattern valRange = valRangePattern();
  expectRoundTrip(valRange, mapIri("valRange_545555094_809_830M"));
  expectRoundTrip(valRange, mapIri("valRange_1073741823_2047_2047M"));
  expectRoundTrip(valRange, mapIri("valRange_536870912_0_0M"));
  // The `M` of the `valRange` pattern doesn't match the `P` here.
  expectNotEncodable(valRange, mapIri("valRange_545555094_1_1P"));
  expectNotEncodable(valRange, mapIri("valRange_1073741824_100_100M"));
  expectNotEncodable(valRange, mapIri("valRange_100_100_100M"));
  // The second and third number of `valRange` don't fit into eleven bits.
  expectNotEncodable(valRange, mapIri("valRange_545555094_2048_100M"));
  expectNotEncodable(valRange, mapIri("valRange_545555094_100_2048M"));
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, PatternWithFixedBitsInTheMiddle) {
  Pattern laneRef = refPattern("laneRef_");
  expectRoundTrip(laneRef, mapIri("laneRef_2343140642651111426_0"));
  expectRoundTrip(laneRef, mapIri("laneRef_2343140642651111426_15"));
  expectRoundTrip(laneRef, mapIri("laneRef_2305843009213693952_0"));

  // The second number is not smaller than 16.
  expectNotEncodable(laneRef, mapIri("laneRef_2343140642651111426_16"));
  // Bit 61 is not set, and the bits `[17, 32)` are not zero.
  expectNotEncodable(laneRef, mapIri("laneRef_1000000000000000000_0"));
  // Bit 62 is set.
  expectNotEncodable(laneRef, mapIri("laneRef_4611686018427387904_0"));
  // The first number doesn't fit into 64 bits.
  expectNotEncodable(laneRef, mapIri("laneRef_18446744073709551616_0"));
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, TheFirstNumberIsStoredInTheHighestBits) {
  // Two 8-bit numbers: the first one occupies the upper eight bits of the
  // 16-bit payload, the second one the lower eight bits.
  Pattern pattern{"<http://example.org/", {Part{8, {}, "-"}, Part{8, {}, ""}}};
  EXPECT_EQ(pattern.numBitsStored(), 16);
  EXPECT_THAT(encodePayload(pattern, "1-2>"), Optional((1u << 8) | 2u));
  EXPECT_THAT(encodePayload(pattern, "255-0>"), Optional(255u << 8));
  EXPECT_EQ(decodeToIri(pattern, (3u << 8) | 4u), "<http://example.org/3-4>");
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, DigitEncodingInsideAPattern) {
  // A pattern that uses the order-preserving digit encoding for a number that
  // is followed by a suffix.
  Pattern pattern{
      "<http://example.org/",
      {Part{24, {}, "-", NumberEncoding::Nibbles}, Part{8, {}, ""}}};
  expectRoundTrip(pattern, "<http://example.org/123456-255>");
  expectRoundTrip(pattern, "<http://example.org/007-0>");
  expectRoundTrip(pattern, "<http://example.org/0-0>");
  // Only six digits fit into 24 bits.
  expectNotEncodable(pattern, "<http://example.org/1234567-0>");
  expectNotEncodable(pattern, "<http://example.org/123456-256>");
  // Leading zeros are only preserved by the nibble encoding of the first
  // number, the second one uses the binary encoding.
  expectNotEncodable(pattern, "<http://example.org/123456-007>");
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, validatePattern) {
  EXPECT_NO_THROW(validatePattern(rangePattern(), numBitsAvailable));
  EXPECT_NO_THROW(validatePattern(valRangePattern(), numBitsAvailable));
  EXPECT_NO_THROW(validatePattern(refPattern("laneRef_"), numBitsAvailable));
  // A pattern may use exactly the available number of bits.
  EXPECT_NO_THROW(validatePattern(refPattern("laneRef_"), 50));

  auto expectThrowForPattern = [](Pattern pattern, const std::string& message,
                                  ad_utility::source_location l =
                                      AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE(
        validatePattern(pattern, numBitsAvailable),
        AllOf(HasSubstr(message), HasSubstr(pattern.prefix_)));
  };
  // The same for a pattern with a prefix that is valid.
  auto expectThrow = [&expectThrowForPattern](std::vector<Part> parts,
                                              const std::string& message,
                                              ad_utility::source_location l =
                                                  AD_CURRENT_SOURCE_LOC()) {
    expectThrowForPattern(Pattern{"<http://example.org/", std::move(parts)},
                          message, l);
  };
  expectThrow({}, "at least one number");
  expectThrow({Part{0, {}, ""}}, "only 1 to 64 bits are supported");
  expectThrow({Part{65, {}, ""}}, "only 1 to 64 bits are supported");
  expectThrow({Part{8, {{4, 12, 0}}, ""}}, "not contained in the [0, 8) bits");
  expectThrow({Part{8, {{4, 4, 0}}, ""}}, "is empty or not contained");
  expectThrow({Part{8, {{5, 4, 0}}, ""}}, "is empty or not contained");
  expectThrow({Part{16, {{4, 8, 0}, {6, 10, 0}}, ""}},
              "sorted and must not overlap");
  expectThrow({Part{16, {{8, 10, 0}, {4, 6, 0}}, ""}},
              "sorted and must not overlap");
  expectThrow({Part{16, {{4, 8, 16}}, ""}},
              "doesn't fit into the fixed bit range");
  expectThrow({Part{10, {}, "", NumberEncoding::Nibbles}},
              "multiple of four bits and no fixed bit ranges");
  expectThrow({Part{16, {{4, 8, 0}}, "", NumberEncoding::Nibbles}},
              "multiple of four bits and no fixed bit ranges");
  expectThrow({Part{8, {}, "a>b"}}, "must not contain an angle bracket");
  expectThrow({Part{8, {}, "a<b"}}, "must not contain an angle bracket");
  expectThrow({Part{8, {}, "1"}}, "must not start with a digit");
  expectThrow({Part{8, {}, ""}, Part{8, {}, ""}},
              "only the last number of a pattern may be followed");
  expectThrow({Part{53, {}, ""}},
              "it requires 53 bits, but only 52 bits are available");
  expectThrow({Part{32, {}, "_"}, Part{21, {}, ""}},
              "it requires 53 bits, but only 52 bits are available");
  expectThrowForPattern(Pattern{"<http://example.org/a>b", {Part{8, {}, ""}}},
                        "prefix must not contain a `>`");

  // Shifting the payload by 64 or more bits would be undefined behavior, so
  // such a number of available bits is a violated precondition.
  AD_EXPECT_THROW_WITH_MESSAGE(validatePattern(rangePattern(), 64),
                               HasSubstr("numBitsAvailable < 64"));
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, json) {
  // The JSON format of the patterns is part of the index format, so the exact
  // keys are checked.
  nlohmann::json j = rangePattern();
  EXPECT_EQ(j["prefix-with-leading-angle-bracket"],
            "<http://example.org/map#range_");
  ASSERT_EQ(j["parts"].size(), 3u);
  const auto& first = j["parts"][0];
  EXPECT_EQ(first["num-bits"], 32);
  EXPECT_EQ(first["suffix"], "_");
  EXPECT_EQ(first["encoding"], "binary");
  ASSERT_EQ(first["fixed-bit-ranges"].size(), 1u);
  EXPECT_EQ(first["fixed-bit-ranges"][0]["begin"], 29);
  EXPECT_EQ(first["fixed-bit-ranges"][0]["end"], 32);
  EXPECT_EQ(first["fixed-bit-ranges"][0]["value"], 1);
  EXPECT_EQ(j["parts"][2]["suffix"], "P");
  EXPECT_TRUE(j["parts"][2]["fixed-bit-ranges"].empty());

  nlohmann::json jNibbles = plainPrefixPattern("<http://example.org/", 32);
  EXPECT_EQ(jNibbles["parts"][0]["encoding"], "nibbles");
  EXPECT_EQ(jNibbles["parts"][0]["suffix"], "");

  // Round trips for all kinds of patterns, including the individual structs.
  for (const Pattern& pattern :
       {rangePattern(), valRangePattern(), refPattern("laneRef_"),
        plainPrefixPattern("<http://example.org/", 32),
        Pattern{
            "<http://example.org/",
            {Part{24, {}, "-", NumberEncoding::Nibbles}, Part{8, {}, ""}}}}) {
    EXPECT_EQ(nlohmann::json(pattern).get<Pattern>(), pattern);
  }
  FixedBitRange range{17, 32, 0};
  EXPECT_EQ(nlohmann::json(range).get<FixedBitRange>(), range);
  Part part{64, {{17, 32, 0}, {61, 64, 1}}, "_"};
  EXPECT_EQ(nlohmann::json(part).get<Part>(), part);

  // An unknown encoding is detected when reading a pattern. Note that the
  // other constraints of a pattern are not checked by `from_json`, but by
  // `validatePattern`, which the `EncodedIriManager` calls.
  nlohmann::json jInvalid = rangePattern();
  jInvalid["parts"][0]["encoding"] = "octal";
  AD_EXPECT_THROW_WITH_MESSAGE(jInvalid.get<Pattern>(),
                               HasSubstr("Unknown encoding \"octal\""));
  // Missing keys are reported by the JSON library.
  nlohmann::json jMissing = rangePattern();
  jMissing["parts"][0].erase("suffix");
  EXPECT_ANY_THROW(jMissing.get<Pattern>());
}

// _____________________________________________________________________________
TEST(EncodedIriPattern, randomRoundTripOfAPattern) {
  Pattern laneRef = refPattern("laneRef_");
  auto gen = ad_utility::SlowRandomIntGenerator<uint64_t>(0, (1ull << 46) - 1);
  for (size_t i = 0; i < 1000; ++i) {
    auto compressed = gen();
    // Distribute the 46 free bits of the first number to the bits `[0, 17)`
    // and `[32, 61)` and set the fixed bits.
    uint64_t number = (1ull << 61) | ((compressed >> 17) << 32) |
                      (compressed & ad_utility::bitMaskForLowerBits(17));
    for (uint64_t second : {0ull, 7ull, 15ull}) {
      expectRoundTrip(laneRef,
                      mapIri(absl::StrCat("laneRef_", number, "_", second)));
    }
  }
}
