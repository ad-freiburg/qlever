// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "index/EncodedIriManager.h"
#include "util/Random.h"
#include "util/TransparentFunctors.h"

namespace {
// Get `num` random indices in the range `[min, max]`. Additionally, add the min
// and the max to the result explicitly, to automaticlaly test corner cases.0
std::vector<size_t> getRandomIndices(size_t min, size_t max, size_t num) {
  ad_utility::SlowRandomIntGenerator<size_t> rand(min, max);
  std::vector<size_t> result;
  result.reserve(num + 2);
  result.push_back(min);
  result.push_back(max);
  for (size_t i = 0; i < num; ++i) {
    result.push_back(rand());
  }
  return result;
}

// _____________________________________________________________________________
TEST(EncodedIriManger, SimpleExample) {
  std::vector<std::string> prefixes = {"http://www.wikidata.org/entity/Q"};
  EncodedIriManager encodedIriManager{prefixes};
  std::string Q42{"<http://www.wikidata.org/entity/Q423>"};
  auto id = encodedIriManager.encode(Q42);
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(encodedIriManager.toString(id.value()), Q42);
}

// _____________________________________________________________________________
TEST(EncodedIriManger, EncodingAndDecoding) {
  auto indices =
      getRandomIndices(0, (1ull << EncodedIriManager::NumDigits) - 1, 10'000);
  std::vector<std::pair<std::string, uint64_t>> stringsAndEncodings;
  std::vector<std::string> prefixes = {"http://www.wikidata.org/entity/Q"};
  EncodedIriManager encodedIriManager{prefixes};
  for (auto index : indices) {
    std::string wdq =
        absl::StrCat("<http://www.wikidata.org/entity/Q", index, ">");
    auto id = encodedIriManager.encode(wdq);
    ASSERT_TRUE(id.has_value()) << index;
    EXPECT_EQ(encodedIriManager.toString(id.value()), wdq)
        << std::hex << id.value().getBits();
    stringsAndEncodings.push_back(
        std::pair{std::move(wdq), id.value().getBits()});
  }

  // Test the sorting;
  auto cpy = stringsAndEncodings;
  ql::ranges::sort(stringsAndEncodings, ql::ranges::less{},
                   [](const auto& pair) {
                     std::string_view sv{pair.first};
                     return sv.substr(1, sv.size() - 2);
                   });
  ql::ranges::sort(cpy, ql::ranges::less{}, ad_utility::second);
  EXPECT_THAT(stringsAndEncodings, ::testing::ElementsAreArray(cpy));
}

// _____________________________________________________________________________
TEST(EncodedIriManger, DifferentPrefixes) {
  std::vector<std::string> prefixes = {"a", "b"};
  EncodedIriManager encodedIriManager{prefixes};
  auto s1 = "<a123>";
  auto s2 = "<b123>";

  auto i1 = encodedIriManager.encode(s1);
  auto i2 = encodedIriManager.encode(s2);
  ASSERT_TRUE(i1.has_value());
  ASSERT_TRUE(i2.has_value());
  EXPECT_NE(i1.value().getBits(), i2.value().getBits());
  EXPECT_EQ(encodedIriManager.toString(i1.value()), s1);
  EXPECT_EQ(encodedIriManager.toString(i2.value()), s2);
}

// _____________________________________________________________________________
TEST(EncodedIriManger, Unencodable) {
  std::vector<std::string> prefixes = {"http://www.wikidata.org/entity/Q"};
  EncodedIriManager encodedIriManager{prefixes};
  std::vector<std::string> unencodable = {
      "<http://www.wikidata.org/entity/Q42a3>",
      "<http://www.wikidata.org/entity/Q4233333333333333333333333333333333333>",
      "<notAValidPrefix>",
      "<http://www.wikidata.org/entity/Q42a3",  // missing trailing '>'
  };
  for (const auto& s : unencodable) {
    EXPECT_FALSE(encodedIriManager.encode(s).has_value());
  }
}

// _____________________________________________________________________________
TEST(EncodedIriManger, illegalPrefixes) {
  using V = std::vector<std::string>;
  using namespace ::testing;
  AD_EXPECT_THROW_WITH_MESSAGE(EncodedIriManager(V{"<blubb>"}),
                               HasSubstr("enclosed in angle brackets"));
  AD_EXPECT_THROW_WITH_MESSAGE(EncodedIriManager(V{"blubb", "blubbi"}),
                               HasSubstr("may be a prefix"));
  EXPECT_NO_THROW(EncodedIriManager(V{"blubb", "blubb"}));

  V v;
  for (size_t s = 0; s < 1000; ++s) {
    v.push_back(absl::StrCat("prefix", s, "bla"));
  }
  AD_EXPECT_THROW_WITH_MESSAGE(EncodedIriManager{v},
                               HasSubstr("which is too many"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, emptyPrefixes) {
  // Calls the default constructor (now includes hard-coded BMW prefixes).
  EncodedIriManager em;
  // Note: It is tempting to use `AD_EXPECT_NULLOPT` etc. here, but that
  // requires to pull in the equality comparison for IDs, which requires linking
  // against basically the whole codebase.

  // This IRI doesn't match any hard-coded pattern, so it should fail
  EXPECT_FALSE(em.encode("<http://www.wikidata.org/entity/Q42>").has_value());

  // But hard-coded BMW patterns should work (with proper bit 29 set)
  EXPECT_TRUE(em.encode("<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
                        "behaviorMap#range_536870912_50_25P>")
                  .has_value());

  // Calls the constructor with an explicitly empty list of prefixes
  // (still includes hard-coded prefixes).
  EncodedIriManager em2(std::vector<std::string>{});
  EXPECT_FALSE(em2.encode("<http://www.wikidata.org/entity/Q42>").has_value());
  EXPECT_TRUE(
      em2.encode("<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
                 "behaviorMap#range_536870912_50_25P>")
          .has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BitPatternMode) {
  // Create an EncodedIriManager with bit pattern mode
  // For testing, let's use a simple prefix and specify that bits [10, 16) must
  // be zero
  EncodedIriManager em;
  detail::PrefixConfig cfg("<http://example.org/", 10, 16);
  em.prefixes_.push_back(cfg);

  // Test values that should encode successfully (bits [10, 16) are zero)
  // Value with bits [10, 16) all zero: e.g., 1023 (binary: 1111111111, bits
  // 0-9 set)
  auto id1 = em.encode("<http://example.org/1023>");
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), "<http://example.org/1023>");

  // Test value 0 (all bits zero)
  auto id2 = em.encode("<http://example.org/0>");
  ASSERT_TRUE(id2.has_value());
  EXPECT_EQ(em.toString(id2.value()), "<http://example.org/0>");

  // Test value with upper bits set but bits [10, 16) zero
  // 65536 = 2^16, binary: 10000000000000000 (bit 16 set, bits [10, 16) zero)
  auto id3 = em.encode("<http://example.org/65536>");
  ASSERT_TRUE(id3.has_value());
  EXPECT_EQ(em.toString(id3.value()), "<http://example.org/65536>");

  // Test values that should NOT encode (bits [10, 16) are not all zero)
  // 1024 = 2^10, binary: 10000000000 (bit 10 is set)
  auto id4 = em.encode("<http://example.org/1024>");
  EXPECT_FALSE(id4.has_value());

  // 2048 = 2^11, binary: 100000000000 (bit 11 is set)
  auto id5 = em.encode("<http://example.org/2048>");
  EXPECT_FALSE(id5.has_value());

  // 32768 = 2^15, binary: 1000000000000000 (bit 15 is set, within [10, 16))
  auto id6 = em.encode("<http://example.org/32768>");
  EXPECT_FALSE(id6.has_value());

  // Test combined value: 66559 = 65536 + 1023 (bits [10, 16) zero, others set)
  auto id7 = em.encode("<http://example.org/66559>");
  ASSERT_TRUE(id7.has_value());
  EXPECT_EQ(em.toString(id7.value()), "<http://example.org/66559>");
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BitPatternAndPlainMixed) {
  // Create an EncodedIriManager with both plain and bit pattern prefixes
  EncodedIriManager em;
  detail::PrefixConfig plainCfg("<http://plain.org/");
  detail::PrefixConfig bitPatternCfg("<http://bitpattern.org/", 8, 12);
  em.prefixes_.push_back(plainCfg);
  em.prefixes_.push_back(bitPatternCfg);

  // Test plain mode
  auto id1 = em.encode("<http://plain.org/12345>");
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), "<http://plain.org/12345>");

  // Test bit pattern mode with valid value (bits [8, 12) zero)
  // 255 = 2^8 - 1, binary: 11111111 (bits 0-7 set, bits [8, 12) zero)
  auto id2 = em.encode("<http://bitpattern.org/255>");
  ASSERT_TRUE(id2.has_value());
  EXPECT_EQ(em.toString(id2.value()), "<http://bitpattern.org/255>");

  // Test bit pattern mode with invalid value (bit 8 is set, within [8, 12))
  // 256 = 2^8
  auto id3 = em.encode("<http://bitpattern.org/256>");
  EXPECT_FALSE(id3.has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, JsonSerializationBackwardCompatibility) {
  using namespace ::testing;

  // Create an EncodedIriManager with old-style plain prefixes
  // (manually to bypass the automatic hard-coded prefix initialization)
  EncodedIriManager em1;
  em1.prefixes_.clear();  // Remove hard-coded prefixes
  em1.prefixes_.emplace_back("<http://example.org/");
  em1.prefixes_.emplace_back("<http://test.org/");

  // Serialize to JSON
  nlohmann::json j1;
  to_json(j1, em1);

  // Deserialize and check (will include hard-coded prefixes)
  EncodedIriManager em2;
  em2.prefixes_.clear();  // Clear to test deserialization only
  from_json(j1, em2);
  EXPECT_EQ(em2.prefixes_.size(), 2);
  EXPECT_EQ(em2.prefixes_[0].prefix, "<http://example.org/");
  EXPECT_FALSE(em2.prefixes_[0].isBitPatternMode());
  EXPECT_EQ(em2.prefixes_[1].prefix, "<http://test.org/");
  EXPECT_FALSE(em2.prefixes_[1].isBitPatternMode());

  // Create an EncodedIriManager with bit pattern prefixes
  EncodedIriManager em3;
  em3.prefixes_.clear();  // Remove hard-coded prefixes
  em3.prefixes_.emplace_back("<http://bitpattern.org/", 5, 11);
  em3.prefixes_.emplace_back("<http://plain.org/");

  // Serialize to JSON
  nlohmann::json j3;
  to_json(j3, em3);

  // Deserialize and check
  EncodedIriManager em4;
  em4.prefixes_.clear();  // Clear to test deserialization only
  from_json(j3, em4);
  EXPECT_EQ(em4.prefixes_.size(), 2);
  EXPECT_EQ(em4.prefixes_[0].prefix, "<http://bitpattern.org/");
  EXPECT_TRUE(em4.prefixes_[0].isBitPatternMode());
  auto [bitStart, bitEnd] = em4.prefixes_[0].getBitRange();
  EXPECT_EQ(bitStart, 5);
  EXPECT_EQ(bitEnd, 11);
  EXPECT_EQ(em4.prefixes_[1].prefix, "<http://plain.org/");
  EXPECT_FALSE(em4.prefixes_[1].isBitPatternMode());

  // Test backward compatibility: old format JSON
  nlohmann::json oldFormatJson;
  oldFormatJson["prefixes-with-leading-angle-brackets"] =
      std::vector<std::string>{"<http://old.org/", "<http://legacy.org/"};

  EncodedIriManager em5;
  em5.prefixes_.clear();  // Clear to test deserialization only
  from_json(oldFormatJson, em5);
  EXPECT_EQ(em5.prefixes_.size(), 2);
  EXPECT_EQ(em5.prefixes_[0].prefix, "<http://old.org/");
  EXPECT_FALSE(em5.prefixes_[0].isBitPatternMode());
  EXPECT_EQ(em5.prefixes_[1].prefix, "<http://legacy.org/");
  EXPECT_FALSE(em5.prefixes_[1].isBitPatternMode());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BMWRangePattern) {
  // Create an EncodedIriManager - hard-coded patterns are initialized
  // automatically
  EncodedIriManager em;

  // Test valid range_ pattern: range_545554944_0_0P
  // 545554944 in binary has upper 3 bits as "001" (bit 29=1, bits 30-31=0)
  std::string rangeIri =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_545554944_0_0P>";
  auto id1 = em.encode(rangeIri);
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), rangeIri);

  // Test with minimum valid value: bit 29 set = 536870912
  std::string rangeIri2 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_536870912_0_0P>";
  auto id2 = em.encode(rangeIri2);
  ASSERT_TRUE(id2.has_value());
  EXPECT_EQ(em.toString(id2.value()), rangeIri2);

  // Test with maximum valid value for first number (bit 29 set, bits 30-31
  // clear) Max: (1<<30) - 1 = 1073741823
  std::string rangeIri3 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_1073741823_255_255P>";
  auto id3 = em.encode(rangeIri3);
  ASSERT_TRUE(id3.has_value());
  EXPECT_EQ(em.toString(id3.value()), rangeIri3);

  // Test values that exceed constraints
  // First number has bit 30 set (violates upper 3 bits = "001")
  std::string rangeIri4 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_1073741824_0_0P>";  // 2^30
  auto id4 = em.encode(rangeIri4);
  EXPECT_FALSE(id4.has_value());

  // First number doesn't have bit 29 set (violates upper 3 bits = "001")
  std::string rangeIri5 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_100_0_0P>";
  auto id5 = em.encode(rangeIri5);
  EXPECT_FALSE(id5.has_value());

  // Second number exceeds 8 bits
  std::string rangeIri6 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_545554944_256_0P>";
  auto id6 = em.encode(rangeIri6);
  EXPECT_FALSE(id6.has_value());

  // Third number exceeds 8 bits
  std::string rangeIri7 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_545554944_0_256P>";
  auto id7 = em.encode(rangeIri7);
  EXPECT_FALSE(id7.has_value());

  // Missing 'P' at the end
  std::string rangeIri8 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_545554944_0_0>";
  auto id8 = em.encode(rangeIri8);
  EXPECT_FALSE(id8.has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BMWValRangePattern) {
  EncodedIriManager em;

  // Test valid valRange_ pattern: valRange_545555094_809_830M
  // 545555094 in binary has upper 3 bits as "001" (bit 29=1, bits 30-31=0)
  std::string valRangeIri =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_545555094_809_830M>";
  auto id1 = em.encode(valRangeIri);
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), valRangeIri);

  // Test with minimum valid value: bit 29 set = 536870912
  std::string valRangeIri2 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_536870912_0_0M>";
  auto id2 = em.encode(valRangeIri2);
  ASSERT_TRUE(id2.has_value());
  EXPECT_EQ(em.toString(id2.value()), valRangeIri2);

  // Test with maximum valid values
  // First number max: (1<<30) - 1 = 1073741823
  // Second and third numbers max: (1<<11) - 1 = 2047
  std::string valRangeIri3 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_1073741823_2047_2047M>";
  auto id3 = em.encode(valRangeIri3);
  ASSERT_TRUE(id3.has_value());
  EXPECT_EQ(em.toString(id3.value()), valRangeIri3);

  // Test values that exceed constraints
  // First number has bit 30 set (violates upper 3 bits = "001")
  std::string valRangeIri4 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_1073741824_100_100M>";  // 2^30
  auto id4 = em.encode(valRangeIri4);
  EXPECT_FALSE(id4.has_value());

  // First number doesn't have bit 29 set
  std::string valRangeIri5 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_100_100_100M>";
  auto id5 = em.encode(valRangeIri5);
  EXPECT_FALSE(id5.has_value());

  // Second number exceeds 11 bits
  std::string valRangeIri6 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_545555094_2048_100M>";
  auto id6 = em.encode(valRangeIri6);
  EXPECT_FALSE(id6.has_value());

  // Third number exceeds 11 bits
  std::string valRangeIri7 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_545555094_100_2048M>";
  auto id7 = em.encode(valRangeIri7);
  EXPECT_FALSE(id7.has_value());

  // Missing 'M' at the end (has 'P' instead)
  std::string valRangeIri8 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#valRange_545555094_100_100P>";
  auto id8 = em.encode(valRangeIri8);
  EXPECT_FALSE(id8.has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BMWLaneRefPattern) {
  EncodedIriManager em;

  // Test valid laneRef pattern with the example value from the spec
  // 2343140642651111426 in binary has:
  // - Highest 3 bits: 001 (bit 61 = 1, bits 62-63 = 0)
  // - Bits [17, 32): all 0
  std::string laneRefIri =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#laneRef_2343140642651111426_0>";
  auto id1 = em.encode(laneRefIri);
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), laneRefIri);

  // Test with different valid second number (< 16)
  std::string laneRefIri2 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#laneRef_2343140642651111426_15>";
  auto id2 = em.encode(laneRefIri2);
  ASSERT_TRUE(id2.has_value());
  EXPECT_EQ(em.toString(id2.value()), laneRefIri2);

  // Test with minimum valid value: bit 61 set, bits [17,32) zero
  // Minimum: 2^61 = 2305843009213693952
  std::string laneRefIri3 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#laneRef_2305843009213693952_0>";
  auto id3 = em.encode(laneRefIri3);
  ASSERT_TRUE(id3.has_value());
  EXPECT_EQ(em.toString(id3.value()), laneRefIri3);

  // Test invalid: second number >= 16
  std::string laneRefIri4 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#laneRef_2343140642651111426_16>";
  auto id4 = em.encode(laneRefIri4);
  EXPECT_FALSE(id4.has_value());

  // Test invalid: bit 61 not set (number < 2^61)
  std::string laneRefIri5 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#laneRef_1000000000000000000_0>";
  auto id5 = em.encode(laneRefIri5);
  EXPECT_FALSE(id5.has_value());

  // Test invalid: bit 62 or 63 set (number >= 2^62)
  std::string laneRefIri6 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#laneRef_4611686018427387904_0>";  // 2^62
  auto id6 = em.encode(laneRefIri6);
  EXPECT_FALSE(id6.has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BMWRoadRefPattern) {
  EncodedIriManager em;

  // Test valid roadRef pattern (same constraints as laneRef)
  std::string roadRefIri =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#roadRef_2343140642651111426_5>";
  auto id1 = em.encode(roadRefIri);
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), roadRefIri);
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BMWSpeedProfilePattern) {
  EncodedIriManager em;

  // Test valid speedprofile pattern (same constraints as laneRef)
  std::string speedProfileIri =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#speedprofile_2343140642651111426_10>";
  auto id1 = em.encode(speedProfileIri);
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), speedProfileIri);
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BMWStopLocPattern) {
  EncodedIriManager em;

  // Test 32-bit variant: stopLoc_<32-bit>_<18-bit>
  std::string stopLocIri32 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#stopLoc_545554944_0>";
  auto id1 = em.encode(stopLocIri32);
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), stopLocIri32);

  // Test 64-bit variant: stopLoc_<special-64-bit>_<4-bit>
  // 2343140642651111426 has upper 3 bits "001" and bits [17,32) zero
  std::string stopLocIri64 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#stopLoc_2343140642651111426_7>";
  auto id2 = em.encode(stopLocIri64);
  ASSERT_TRUE(id2.has_value());
  EXPECT_EQ(em.toString(id2.value()), stopLocIri64);

  // Test minimum valid 64-bit value
  std::string stopLocIri64Min =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#stopLoc_2305843009213693952_0>";  // 2^61
  auto id3 = em.encode(stopLocIri64Min);
  ASSERT_TRUE(id3.has_value());
  EXPECT_EQ(em.toString(id3.value()), stopLocIri64Min);

  // Test 32-bit variant with maximum values
  // First number max: (1<<32) - 1 = 4294967295
  // Second number max: (1<<18) - 1 = 262143
  std::string stopLocIri32Max =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#stopLoc_4294967295_262143>";
  auto id4 = em.encode(stopLocIri32Max);
  ASSERT_TRUE(id4.has_value());
  EXPECT_EQ(em.toString(id4.value()), stopLocIri32Max);

  // Test values that exceed constraints
  // First number exceeds 64 bits (shouldn't be possible with parseDecimal, but
  // test large invalid value)
  std::string stopLocIriBad1 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#stopLoc_4294967296_0>";  // > 32 bits, but < 64 bits, no
                                            // special bits
  auto id5 = em.encode(stopLocIriBad1);
  // This should fail because it doesn't match 64-bit constraints (no bit 61
  // set) and exceeds 32 bits for 32-bit variant
  EXPECT_FALSE(id5.has_value());

  // Second number exceeds both variants (> 18 bits)
  std::string stopLocIriBad2 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#stopLoc_545554944_262144>";
  auto id6 = em.encode(stopLocIriBad2);
  EXPECT_FALSE(id6.has_value());

  // 64-bit variant with second number >= 16 should fall back to 32-bit, but
  // here the first number is valid for 64-bit pattern but second exceeds 18
  // bits
  std::string stopLocIriBad3 =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#stopLoc_2343140642651111426_262144>";
  auto id7 = em.encode(stopLocIriBad3);
  EXPECT_FALSE(id7.has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, BMWPatternsWithConfigurablePrefixes) {
  // Test that hard-coded patterns work alongside configurable prefixes
  std::vector<std::string> prefixes = {"http://www.wikidata.org/entity/Q"};
  EncodedIriManager em{prefixes};

  // Test configurable prefix
  std::string wikidataIri = "<http://www.wikidata.org/entity/Q42>";
  auto id1 = em.encode(wikidataIri);
  ASSERT_TRUE(id1.has_value());
  EXPECT_EQ(em.toString(id1.value()), wikidataIri);

  // Test hard-coded BMW pattern (with proper bit 29 set)
  std::string rangeIri =
      "<http://www.bmw-carit.de/Foresight/Map/Ontologies/Low/"
      "behaviorMap#range_536883257_100_200P>";  // 536870912 + 12345
  auto id2 = em.encode(rangeIri);
  ASSERT_TRUE(id2.has_value());
  EXPECT_EQ(em.toString(id2.value()), rangeIri);

  // Ensure they have different prefix indices
  EXPECT_NE(id1.value().getBits(), id2.value().getBits());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, splitIntoPrefixIdxAndPayload) {
  EncodedIriManager em{{"blabb", "blubb"}};
  auto id = em.encode("<blubb42>");
  ASSERT_TRUE(id.has_value());
  auto [prefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(id.value());
  EXPECT_EQ(prefixIdx, 1);
  std::string result;
  EncodedIriManager::decodeDecimalFrom64Bit(result, payload);
  EXPECT_EQ(result, "42");
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager::splitIntoPrefixIdxAndPayload(Id::makeUndefined()),
      ::testing::HasSubstr("must be `EncodedVal`"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, toStringWithGivenPrefix) {
  auto str = EncodedIriManager::toStringWithGivenPrefix(
      EncodedIriManager::encodeDecimalToNBit("7643"), "<blibb_");
  EXPECT_EQ(str, "<blibb_7643>");
}

// _____________________________________________________________________________
TEST(EncodedIriManager, makeIdFromPrefixIdxAndPayload) {
  EncodedIriManager em{{"blabb", "blubb"}};
  auto id = EncodedIriManager::makeIdFromPrefixIdxAndPayload(
      1, EncodedIriManager::encodeDecimalToNBit("7643"));
  EXPECT_EQ(em.toString(id), "<blubb7643>");
}

}  // namespace
