// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "../util/GTestHelpers.h"
#include "blobConverter/LegacyDatatype.h"
#include "blobConverter/LegacyEncodedIriManager.h"
#include "global/Constants.h"
#include "util/json.h"

using namespace qlever::blobConverter;
using namespace testing;

namespace {
constexpr size_t numBitsEncoding = LegacyEncodedIriManager::NumBitsEncoding;

// The prefixes of the hardcoded BMW schemes, as used in the legacy blobs.
constexpr std::string_view lbm =
    "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#";
constexpr std::string_view map = "<http://www.bmw.de/FS/Map#";

// The `"encoded-iri-prefixes"` JSON of the legacy sample blobs (see
// `test/data/oldBlobFormat`).
nlohmann::json sampleConfigJson() {
  return nlohmann::json::parse(R"({"prefix-configs":[
    {"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#range_","specialEncoding":1},
    {"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#valRange_","specialEncoding":2},
    {"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#laneRef_","specialEncoding":3},
    {"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#roadRef_","specialEncoding":4},
    {"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#speedprofile_","specialEncoding":5},
    {"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#stopLoc_","specialEncoding":7},
    {"bitRangeConstraints":[{"bitEnd":32,"bitStart":23,"value":0},{"bitEnd":64,"bitStart":61,"value":1}],"prefix":"<http://www.bmw.de/FS/Map#lane_"},
    {"bitRangeConstraints":[{"bitEnd":23,"bitStart":22,"value":0},{"bitEnd":32,"bitStart":24,"value":0},{"bitEnd":64,"bitStart":61,"value":1}],"prefix":"<http://www.bmw.de/FS/Map#roadPart_"},
    {"bitRangeConstraints":[{"bitEnd":32,"bitStart":23,"value":0},{"bitEnd":64,"bitStart":61,"value":1}],"prefix":"<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/behaviorMap#dp_"}
  ]})");
}

LegacyEncodedIriManager sampleManager() {
  return LegacyEncodedIriManager::fromJson(sampleConfigJson());
}

// Combine a `tag` and a `payload` into the 60 data bits of a legacy `Id`.
uint64_t encoded(uint64_t tag, uint64_t payload) {
  return (tag << numBitsEncoding) | payload;
}

// Check that the `manager` encodes the `iri` to exactly `expectedEncoded` and
// decodes it back to the same string.
void expectRoundtrip(const LegacyEncodedIriManager& manager,
                     const std::string& iri, uint64_t expectedEncoded) {
  auto result = manager.encode(iri);
  ASSERT_TRUE(result.has_value()) << iri;
  EXPECT_EQ(result.value(), expectedEncoded) << iri;
  EXPECT_EQ(manager.toString(result.value()), iri);
}

// Check that the current manager built from the legacy `manager` encodes the
// `iri` and decodes it back to the same string.
void expectCurrentRoundtrip(const LegacyEncodedIriManager& manager,
                            const std::string& iri) {
  auto current = manager.makeCurrentManager();
  auto id = current.encode(iri);
  ASSERT_TRUE(id.has_value()) << iri;
  EXPECT_EQ(current.toString(id.value()), iri);
}

// The first numbers of the `LaneRef`-like schemes and of the bit range
// constraint schemes that the tests below use: bit 61 set, some bits in
// `[32, 61)`, and some bits in `[0, 17)` resp. `[0, 23)`.
constexpr uint64_t refNum1 = (1ULL << 61) | (12345ULL << 32) | 99ULL;
constexpr uint64_t laneNum = (1ULL << 61) | (0xABCULL << 32) | 0x7FFFFFULL;
constexpr uint64_t roadPartNum =
    (1ULL << 61) | (0x123ULL << 32) | (1ULL << 23) | 0x3FFFFFULL;

// Return the IRIs that the tests below use for each of the sample schemes.
std::vector<std::string> sampleIris() {
  return {absl::StrCat(lbm, "range_536870917_3_7P>"),
          absl::StrCat(lbm, "valRange_536870913_2047_5M>"),
          absl::StrCat(lbm, "laneRef_", refNum1, "_9>"),
          absl::StrCat(lbm, "roadRef_", 1ULL << 61, "_0>"),
          absl::StrCat(lbm, "speedprofile_", refNum1, "_15>"),
          absl::StrCat(lbm, "stopLoc_", refNum1, "_9>"),
          absl::StrCat(lbm, "stopLoc_4000000000_200000>"),
          absl::StrCat(lbm, "stopLoc_5_3>"),
          absl::StrCat(map, "lane_", laneNum, ">"),
          absl::StrCat(map, "roadPart_", roadPartNum, ">"),
          absl::StrCat(lbm, "dp_", (1ULL << 61) | 1, ">")};
}
}  // namespace

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, fromJson) {
  auto manager = sampleManager();
  const auto& configs = manager.configs();
  ASSERT_EQ(configs.size(), 9u);
  EXPECT_EQ(configs[0].specialEncoding_, LegacySpecialEncoding::RangePattern);
  EXPECT_EQ(configs[1].specialEncoding_,
            LegacySpecialEncoding::ValRangePattern);
  EXPECT_EQ(configs[2].specialEncoding_, LegacySpecialEncoding::LaneRef);
  EXPECT_EQ(configs[3].specialEncoding_, LegacySpecialEncoding::RoadRef);
  EXPECT_EQ(configs[4].specialEncoding_, LegacySpecialEncoding::SpeedProfile);
  EXPECT_EQ(configs[5].specialEncoding_, LegacySpecialEncoding::StopLoc32);
  EXPECT_TRUE(configs[6].isMultiConstraintMode());
  EXPECT_EQ(configs[6].prefix_, absl::StrCat(map, "lane_"));
  EXPECT_EQ(configs[7].bitRangeConstraints_.size(), 3u);
  EXPECT_TRUE(configs[8].isMultiConstraintMode());
  for (const auto& config : configs) {
    EXPECT_FALSE(config.isPlain());
    EXPECT_THAT(config.prefix_, StartsWith("<"));
  }

  // The plain format (a list of prefixes).
  auto plain = LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
      R"({"prefixes-with-leading-angle-brackets":["<http://a/","<http://b/"]})"));
  ASSERT_EQ(plain.configs().size(), 2u);
  EXPECT_TRUE(plain.configs()[0].isPlain());
  EXPECT_EQ(plain.configs()[1].prefix_, "<http://b/");

  // Error cases.
  AD_EXPECT_THROW_WITH_MESSAGE(
      LegacyEncodedIriManager::fromJson(nlohmann::json::parse("{}")),
      HasSubstr("neither the key"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
          R"({"prefix-configs":[{"prefix":"<http://a/","specialEncoding":42}]})")),
      HasSubstr("Unknown special encoding"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
          R"({"prefix-configs":[{"prefix":"http://a/"}]})")),
      HasSubstr("does not start with `<`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
          R"({"prefix-configs":[{"prefix":"<http://a/","bitRangeConstraints":[{"bitStart":3,"bitEnd":2,"value":0}]}]})")),
      HasSubstr("Invalid bit range constraint"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
          R"({"prefix-configs":[{"prefix":"<http://a/","bitRangeConstraints":[{"bitStart":0,"bitEnd":4,"value":0},{"bitStart":2,"bitEnd":8,"value":0}]}]})")),
      HasSubstr("Overlapping"));
  // A zero bit range that ends at bit 64 (which the legacy encoder did not
  // handle either), and a constraint whose value does not fit into its range.
  AD_EXPECT_THROW_WITH_MESSAGE(
      LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
          R"({"prefix-configs":[{"prefix":"<http://a/","zeroBitStart":60,"zeroBitEnd":64}]})")),
      HasSubstr("smaller than 64"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
          R"({"prefix-configs":[{"prefix":"<http://a/","bitRangeConstraints":[{"bitStart":2,"bitEnd":4,"value":4}]}]})")),
      HasSubstr("Invalid bit range constraint"));
  // A constraint that covers all 64 bits is valid (this used to be undefined
  // behavior in the value check).
  auto allBits = LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
      R"({"prefix-configs":[{"prefix":"<http://a/","bitRangeConstraints":[{"bitStart":0,"bitEnd":64,"value":7}]}]})"));
  EXPECT_EQ(allBits.configs().at(0).bitRangeConstraints_.at(0).value_, 7u);
  // The legacy alias `StopLoc` (value 6) uses the same encoding as
  // `StopLoc32`.
  auto stopLoc = LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
      R"({"prefix-configs":[{"prefix":"<http://s/","specialEncoding":6}]})"));
  EXPECT_EQ(stopLoc.configs().at(0).specialEncoding_,
            LegacySpecialEncoding::StopLoc);
  expectRoundtrip(stopLoc, "<http://s/5_3>",
                  encoded(0, (1ULL << 50) | (5ULL << 18) | 3ULL));
  EXPECT_EQ(stopLoc.toPatterns().size(), 2u);
  expectCurrentRoundtrip(stopLoc, "<http://s/5_3>");
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, rangePattern) {
  auto manager = sampleManager();
  // `num1 = 2^29 + 5`, `num2 = 3`, `num3 = 7`, packed as
  // `[num1 without bit 29 : 29][num2 : 10][num3 : 11]`.
  uint64_t payload = (5ULL << 21) | (3ULL << 11) | 7ULL;
  expectRoundtrip(manager, absl::StrCat(lbm, "range_536870917_3_7P>"),
                  encoded(0, payload));
  // The maximal values of `num2` and `num3`.
  expectRoundtrip(manager, absl::StrCat(lbm, "range_536870912_255_255P>"),
                  encoded(0, (255ULL << 11) | 255ULL));
  // Constraint violations: bit 29 not set, bit 30 set, `num2` too large,
  // wrong suffix, missing number.
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "range_1_2_3P>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "range_1610612736_2_3P>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "range_536870917_256_7P>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "range_536870917_3_7M>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "range_536870917_3P>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "range_536870917_3_7P")));
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, valRangePattern) {
  auto manager = sampleManager();
  uint64_t payload = (1ULL << 22) | (2047ULL << 11) | 5ULL;
  expectRoundtrip(manager, absl::StrCat(lbm, "valRange_536870913_2047_5M>"),
                  encoded(1, payload));
  EXPECT_FALSE(
      manager.encode(absl::StrCat(lbm, "valRange_536870913_2048_5M>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "valRange_536870913_2_5P>")));
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, refPatterns) {
  auto manager = sampleManager();
  // `num1 = 2^61 | (12345 << 32) | 99`, `num2 = 9`, packed as
  // `[bits 32..61 of num1 : 29][bits 0..17 of num1 : 17][num2 : 4]`.
  uint64_t num1 = refNum1;
  uint64_t payload = (12345ULL << 21) | (99ULL << 4) | 9ULL;
  expectRoundtrip(manager, absl::StrCat(lbm, "laneRef_", num1, "_9>"),
                  encoded(2, payload));
  expectRoundtrip(manager, absl::StrCat(lbm, "roadRef_", num1, "_9>"),
                  encoded(3, payload));
  expectRoundtrip(manager, absl::StrCat(lbm, "speedprofile_", num1, "_9>"),
                  encoded(4, payload));
  // The minimal and maximal first numbers.
  expectRoundtrip(manager, absl::StrCat(lbm, "roadRef_", 1ULL << 61, "_0>"),
                  encoded(3, 0));
  // The payload of these schemes has 29 + 17 + 4 = 50 bits.
  uint64_t maxNum1 =
      (1ULL << 61) | (((1ULL << 29) - 1) << 32) | ((1ULL << 17) - 1);
  expectRoundtrip(manager, absl::StrCat(lbm, "speedprofile_", maxNum1, "_15>"),
                  encoded(4, (1ULL << 50) - 1));
  // Constraint violations: `num2` too large, bit 61 not set, bit 62 set, one
  // of the bits `[17, 32)` set.
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "laneRef_", num1, "_16>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "laneRef_5_3>")));
  EXPECT_FALSE(manager.encode(
      absl::StrCat(lbm, "laneRef_", num1 | (1ULL << 62), "_3>")));
  EXPECT_FALSE(manager.encode(
      absl::StrCat(lbm, "laneRef_", num1 | (1ULL << 20), "_3>")));
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, stopLocPattern) {
  auto manager = sampleManager();
  // The 64-bit variant (flag bit 50 is zero).
  uint64_t num1 = refNum1;
  uint64_t payload64 = (12345ULL << 21) | (99ULL << 4) | 9ULL;
  expectRoundtrip(manager, absl::StrCat(lbm, "stopLoc_", num1, "_9>"),
                  encoded(5, payload64));
  // The 32-bit variant (flag bit 50 is one), because `num2 >= 16`.
  uint64_t payload32 = (1ULL << 50) | (4000000000ULL << 18) | 200000ULL;
  expectRoundtrip(manager, absl::StrCat(lbm, "stopLoc_4000000000_200000>"),
                  encoded(5, payload32));
  // The 32-bit variant, because `num1` violates the 64-bit constraints.
  expectRoundtrip(manager, absl::StrCat(lbm, "stopLoc_5_3>"),
                  encoded(5, (1ULL << 50) | (5ULL << 18) | 3ULL));
  // Neither variant applies.
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "stopLoc_4294967296_3>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "stopLoc_5_262144>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(lbm, "stopLoc_5>")));
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, bitRangeConstraints) {
  auto manager = sampleManager();
  // `lane_`: bits `[23, 32)` are zero, bits `[61, 64)` are `001`. The
  // remaining bits `[0, 23)` and `[32, 61)` are concatenated.
  uint64_t lane = laneNum;
  expectRoundtrip(manager, absl::StrCat(map, "lane_", lane, ">"),
                  encoded(6, 0x7FFFFFULL | (0xABCULL << 23)));
  // `roadPart_`: bits 22 and `[24, 32)` are zero, bits `[61, 64)` are `001`.
  // The remaining bits `[0, 22)`, bit 23, and `[32, 61)` are concatenated.
  uint64_t roadPart = roadPartNum;
  expectRoundtrip(manager, absl::StrCat(map, "roadPart_", roadPart, ">"),
                  encoded(7, 0x3FFFFFULL | (1ULL << 22) | (0x123ULL << 23)));
  // `dp_` has the same constraints as `lane_`.
  expectRoundtrip(manager, absl::StrCat(lbm, "dp_", (1ULL << 61) | 1, ">"),
                  encoded(8, 1));
  // Constraint violations.
  EXPECT_FALSE(manager.encode(absl::StrCat(map, "lane_5>")));
  EXPECT_FALSE(
      manager.encode(absl::StrCat(map, "lane_", lane | (1ULL << 25), ">")));
  EXPECT_FALSE(manager.encode(
      absl::StrCat(map, "roadPart_", roadPart | (1ULL << 22), ">")));
  EXPECT_FALSE(manager.encode(absl::StrCat(map, "lane_", lane, "_3>")));
  // Not a number at all, or an overflowing number.
  EXPECT_FALSE(manager.encode(absl::StrCat(map, "lane_abc>")));
  EXPECT_FALSE(manager.encode(absl::StrCat(map, "lane_99999999999999999999>")));
  // An unknown prefix.
  EXPECT_FALSE(manager.encode("<http://example.org/123>"));
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, plainAndZeroBitRange) {
  auto manager = LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
      R"({"prefix-configs":[{"prefix":"<http://example.org/"},
          {"prefix":"<http://z/","zeroBitStart":8,"zeroBitEnd":16}]})"));
  // Plain digits: four bits per digit, left-aligned, digit `d` stored as
  // `d + 1`.
  expectRoundtrip(manager, "<http://example.org/123>",
                  encoded(0, encodedIri::encodeDigits("123", numBitsEncoding)));
  expectRoundtrip(manager, "<http://example.org/0>",
                  encoded(0, 1ULL << (numBitsEncoding - 4)));
  // Leading zeros are preserved by the digit encoding.
  expectRoundtrip(manager, "<http://example.org/007>",
                  encoded(0, encodedIri::encodeDigits("007", numBitsEncoding)));
  // At most 13 digits fit.
  EXPECT_TRUE(manager.encode("<http://example.org/1234567890123>"));
  EXPECT_FALSE(manager.encode("<http://example.org/12345678901234>"));
  EXPECT_FALSE(manager.encode("<http://example.org/12a>"));
  // Zero bit range: bits `[8, 16)` are removed.
  expectRoundtrip(manager, absl::StrCat("<http://z/", 0xAB00CDULL, ">"),
                  encoded(1, 0xABCDULL));
  EXPECT_FALSE(manager.encode(absl::StrCat("<http://z/", 0xAB01CDULL, ">")));

  // The current patterns for these two configs.
  auto patterns = manager.toPatterns();
  ASSERT_EQ(patterns.size(), 2u);
  EXPECT_EQ(patterns[0], encodedIri::plainPrefixPattern("http://example.org/",
                                                        numBitsEncoding));
  EXPECT_EQ(patterns[1].prefix_, "http://z/");
  ASSERT_EQ(patterns[1].parts_.size(), 1u);
  // The legacy encoder limited the compressed value to 52 bits, which for
  // the 8 removed bits means that the highest 4 bits of the number are zero.
  EXPECT_EQ(patterns[1].parts_[0].fixedBitRanges_,
            (std::vector<encodedIri::FixedBitRange>{{8, 16, 0}, {60, 64, 0}}));
  EXPECT_EQ(patterns[1].numBitsStored(), numBitsEncoding);
  // The largest encodable number has all the unconstrained bits set.
  uint64_t largest = ((1ULL << 60) - 1) & ~0xFF00ULL;
  expectRoundtrip(manager, absl::StrCat("<http://z/", largest, ">"),
                  encoded(1, (1ULL << 52) - 1));
  EXPECT_FALSE(
      manager.encode(absl::StrCat("<http://z/", largest | (1ULL << 60), ">")));
  expectCurrentRoundtrip(manager, absl::StrCat("<http://z/", largest, ">"));
  EXPECT_FALSE(manager.makeCurrentManager().encode(
      absl::StrCat("<http://z/", largest | (1ULL << 60), ">")));
  expectCurrentRoundtrip(manager, "<http://example.org/123>");
  expectCurrentRoundtrip(manager, "<http://example.org/007>");
  expectCurrentRoundtrip(manager, absl::StrCat("<http://z/", 0xAB00CDULL, ">"));

  // A multi-constraint config with only three fixed bits: the legacy limit of
  // 52 payload bits fixes the nine bits below the constraint to zero.
  auto threeBits = LegacyEncodedIriManager::fromJson(nlohmann::json::parse(
      R"({"prefix-configs":[{"prefix":"<http://t/","bitRangeConstraints":[{"bitStart":61,"bitEnd":64,"value":1}]}]})"));
  auto threeBitPatterns = threeBits.toPatterns();
  ASSERT_EQ(threeBitPatterns.size(), 1u);
  EXPECT_EQ(threeBitPatterns[0].parts_.at(0).fixedBitRanges_,
            (std::vector<encodedIri::FixedBitRange>{{52, 61, 0}, {61, 64, 1}}));
  uint64_t largestThreeBits = (1ULL << 61) | ((1ULL << 52) - 1);
  expectRoundtrip(threeBits, absl::StrCat("<http://t/", largestThreeBits, ">"),
                  encoded(0, (1ULL << 52) - 1));
  EXPECT_FALSE(threeBits.encode(
      absl::StrCat("<http://t/", largestThreeBits | (1ULL << 52), ">")));
  expectCurrentRoundtrip(threeBits,
                         absl::StrCat("<http://t/", largestThreeBits, ">"));
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, toPatternsAndCurrentManager) {
  auto manager = sampleManager();
  auto patterns = manager.toPatterns();
  // The `stopLoc_` config becomes two patterns.
  ASSERT_EQ(patterns.size(), 10u);
  EXPECT_EQ(patterns[5].prefix_, patterns[6].prefix_);
  EXPECT_THAT(patterns[5].prefix_, EndsWith("#stopLoc_"));
  EXPECT_EQ(patterns[5].parts_.size(), 2u);
  EXPECT_EQ(patterns[5].parts_[0].numBits_, 64u);
  EXPECT_EQ(patterns[6].parts_[0].numBits_, 32u);
  EXPECT_EQ(patterns[6].parts_[1].numBits_, 18u);
  // The `range_` pattern is the example from `EncodedIriPattern.h`.
  using encodedIri::Part;
  EXPECT_EQ(patterns[0].parts_,
            (std::vector<Part>{Part{32, {{29, 32, 1}}, "_"}, Part{8, {}, "_"},
                               Part{8, {}, "P"}}));
  EXPECT_EQ(patterns[2].parts_,
            (std::vector<Part>{Part{64, {{17, 32, 0}, {61, 64, 1}}, "_"},
                               Part{4, {}, ""}}));
  EXPECT_EQ(patterns[7].parts_,
            (std::vector<Part>{Part{64, {{23, 32, 0}, {61, 64, 1}}, ""}}));
  EXPECT_EQ(patterns[8].parts_,
            (std::vector<Part>{
                Part{64, {{22, 23, 0}, {24, 32, 0}, {61, 64, 1}}, ""}}));
  for (const auto& pattern : patterns) {
    EXPECT_THAT(pattern.prefix_, StartsWith("http://"));
    EXPECT_LE(pattern.numBitsStored(), numBitsEncoding);
  }

  // Every IRI that the legacy manager encodes is also encoded by the current
  // manager, and decodes to the same string.
  // The current manager additionally has the always-on prefix for new graphs
  // as its first pattern.
  auto current = manager.makeCurrentManager();
  EXPECT_EQ(current.patterns_.size(), 11u);
  EXPECT_EQ(current.patterns_[0].prefix_,
            absl::StrCat("<", QLEVER_NEW_GRAPH_PREFIX));
  for (size_t i = 0; i < patterns.size(); ++i) {
    EXPECT_EQ(current.patterns_[i + 1].prefix_,
              absl::StrCat("<", patterns[i].prefix_));
  }
  for (const auto& iri : sampleIris()) {
    auto legacyEncoded = manager.encode(iri);
    ASSERT_TRUE(legacyEncoded.has_value()) << iri;
    EXPECT_EQ(manager.toString(legacyEncoded.value()), iri);
    expectCurrentRoundtrip(manager, iri);
  }
  // The JSON of the current manager uses the `patterns` key.
  nlohmann::json json = current;
  EXPECT_TRUE(json.contains("patterns"));
  EXPECT_EQ(json["patterns"].size(), 11u);
}

// _____________________________________________________________________________
TEST(LegacyEncodedIriManager, toStringFromLegacyBits) {
  auto manager = sampleManager();
  auto iri = absl::StrCat(lbm, "range_536870917_3_7P>");
  auto dataBits = manager.encode(iri).value();
  EXPECT_EQ(manager.toStringFromLegacyBits(
                makeLegacyBits(LegacyDatatype::EncodedVal, dataBits)),
            iri);
  // A wrong legacy datatype, and a tag without a config.
  EXPECT_ANY_THROW(manager.toStringFromLegacyBits(
      makeLegacyBits(LegacyDatatype::BlankNodeIndex, dataBits)));
  AD_EXPECT_THROW_WITH_MESSAGE(manager.toString(encoded(9, 0)),
                               HasSubstr("only 9 prefixes are configured"));
}
