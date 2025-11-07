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
  // Calls the default constructor.
  EncodedIriManager em;
  // Note: It is tempting to use `AD_EXPECT_NULLOPT` etc. here, but that
  // requires to pull in the equality comparison for IDs, which requires linking
  // against basically the whole codebase.
  EXPECT_FALSE(em.encode("<http://www.wikidata.org/entity/Q42>").has_value());

  // Calls the constructor with an explicitly empty list of prefixes.
  EncodedIriManager em2(std::vector<std::string>{});
  EXPECT_FALSE(em.encode("<http://www.wikidata.org/entity/Q42>").has_value());
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
  EncodedIriManager em1;
  em1.prefixes_.emplace_back("<http://example.org/");
  em1.prefixes_.emplace_back("<http://test.org/");

  // Serialize to JSON
  nlohmann::json j1;
  to_json(j1, em1);

  // Deserialize and check
  EncodedIriManager em2;
  from_json(j1, em2);
  EXPECT_EQ(em2.prefixes_.size(), 2);
  EXPECT_EQ(em2.prefixes_[0].prefix, "<http://example.org/");
  EXPECT_FALSE(em2.prefixes_[0].isBitPatternMode());
  EXPECT_EQ(em2.prefixes_[1].prefix, "<http://test.org/");
  EXPECT_FALSE(em2.prefixes_[1].isBitPatternMode());

  // Create an EncodedIriManager with bit pattern prefixes
  EncodedIriManager em3;
  em3.prefixes_.emplace_back("<http://bitpattern.org/", 5, 11);
  em3.prefixes_.emplace_back("<http://plain.org/");

  // Serialize to JSON
  nlohmann::json j3;
  to_json(j3, em3);

  // Deserialize and check
  EncodedIriManager em4;
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
  from_json(oldFormatJson, em5);
  EXPECT_EQ(em5.prefixes_.size(), 2);
  EXPECT_EQ(em5.prefixes_[0].prefix, "<http://old.org/");
  EXPECT_FALSE(em5.prefixes_[0].isBitPatternMode());
  EXPECT_EQ(em5.prefixes_[1].prefix, "<http://legacy.org/");
  EXPECT_FALSE(em5.prefixes_[1].isBitPatternMode());
}

}  // namespace
