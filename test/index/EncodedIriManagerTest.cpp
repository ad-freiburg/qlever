// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "index/vocabulary/EncodedIriManager.h"
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
TEST(EncodedIriManager, makeIdFromPrefixIdxAndPayload) {
  EncodedIriManager em{{"blabb", "blubb"}};
  auto id = EncodedIriManager::makeIdFromPrefixIdxAndPayload(
      1, EncodedIriManager::encodeDecimalToNBit("7643"));
  EXPECT_EQ(em.toString(id), "<blubb7643>");
}

// The encoding is usable in a constant expression. This is not only a nice
// property, but also required: a `constexpr` function that can never yield a
// constant expression is ill-formed, and several compilers reject it (see the
// note in `NibbleEncoding.h`).
// The digit `1` is stored as the nibble `2` in the leftmost nibble.
static_assert(EncodedIriManager::encodeDecimalToNBit("1") ==
              uint64_t{2} << (EncodedIriManager::NumBitsEncoding -
                              EncodedIriManager::NibbleSize));

// _____________________________________________________________________________
TEST(EncodedIriManager, decodeDecimalFrom64Bit) {
  auto testNumber = [](uint64_t number, ad_utility::source_location l =
                                            AD_CURRENT_SOURCE_LOC()) {
    using m = EncodedIriManager;
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(number, m::decodeDecimalFrom64Bit(
                          m::encodeDecimalToNBit(std::to_string(number))));
  };
  uint64_t MAX = std::stoull(std::string(EncodedIriManager::NumDigits, '9'));
  testNumber(0);
  testNumber(MAX);
  auto intGenerator = ad_utility::SlowRandomIntGenerator<uint64_t>(0, MAX);
  for (auto _ = 0; _ < 20; ++_) {
    testNumber(intGenerator());
  }
}

// _____________________________________________________________________________
TEST(EncodedIriManager, getIndexOfPrefix) {
  {
    auto manager = EncodedIriManager();
    // No custom prefixes so only need to test the hardcoded ones.
    for (const auto& [i, fixedPrefix] :
         ranges::views::enumerate(AlwaysOnPrefixes::value)) {
      EXPECT_THAT(manager.getIndexOfPrefix(fixedPrefix),
                  testing::Optional(testing::Eq(i)));
    }
    EXPECT_THAT(manager.getIndexOfPrefix("http://example.org"),
                testing::Eq(std::nullopt));
  }
  {
    std::vector<std::string> customPrefixes = {"http://qlever.dev"};
    auto manager = EncodedIriManager(customPrefixes);
    // Create a list of all prefixes, including the hardcoded ones, for testing
    // the function.
    auto allPrefixes = customPrefixes;
    for (auto prefix : AlwaysOnPrefixes::value) {
      allPrefixes.emplace_back(prefix);
    }
    ql::ranges::sort(allPrefixes);
    for (const auto& [i, prefix] : ranges::views::enumerate(allPrefixes)) {
      EXPECT_THAT(manager.getIndexOfPrefix(prefix),
                  testing::Optional(testing::Eq(i)));
    }
    EXPECT_THAT(manager.getIndexOfPrefix("http://example.org"),
                testing::Eq(std::nullopt));
  }
}

// _____________________________________________________________________________
struct TestHardcodedPrefixes {
  static constexpr std::array<std::string_view, 1> value = {
      "http://example.org/always/"};
};

// _____________________________________________________________________________
TEST(EncodedIriManager, HardcodedPrefixes) {
  using Manager =
      EncodedIriManagerImpl<Id::numDataBits, 8, TestHardcodedPrefixes>;

  // Default constructor includes hardcoded prefix.
  Manager em;
  auto id = em.encode("<http://example.org/always/42>");
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(em.toString(id.value()), "<http://example.org/always/42>");

  // Constructor with additional prefixes also includes hardcoded.
  Manager em2{{"http://other.org/"}};
  auto id2 = em2.encode("<http://example.org/always/99>");
  ASSERT_TRUE(id2.has_value());
  auto id3 = em2.encode("<http://other.org/1>");
  ASSERT_TRUE(id3.has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, cannotAddHarcodedPrefixes) {
  using Manager =
      EncodedIriManagerImpl<Id::numDataBits, 8, TestHardcodedPrefixes>;

  // Adding a hardcoded prefix a second time in the constructor is an error.
  AD_EXPECT_THROW_WITH_MESSAGE(
      Manager({std::string{TestHardcodedPrefixes::value.at(0)}}),
      testing::HasSubstr(
          "!ad_utility::contains(prefixesWithoutAngleBrackets, prefix)"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, HardcodedPrefixesJson) {
  using Manager =
      EncodedIriManagerImpl<Id::numDataBits, 8, TestHardcodedPrefixes>;

  Manager em{{"http://other.org/"}};
  nlohmann::json j = em;
  Manager em2 = j.get<Manager>();
  auto id = em2.encode("<http://example.org/always/42>");
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(em2.toString(id.value()), "<http://example.org/always/42>");
  auto id2 = em2.encode("<http://other.org/1>");
  ASSERT_TRUE(id2.has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, noPrefixesAtAll) {
  // The default `EncodedIriManager` always contains the `AlwaysOnPrefixes`, so
  // an explicit instantiation with `NoHardcodedPrefixes` (the default for the
  // third template parameter) is required to obtain a manager with a truly
  // empty list of prefixes.
  using Manager = EncodedIriManagerImpl<Id::numDataBits, 8>;

  // Calls the default constructor.
  Manager em;
  EXPECT_TRUE(em.patterns_.empty());
  EXPECT_FALSE(em.encode("<http://example.org/42>").has_value());

  // Calls the constructor with an explicitly empty list of prefixes.
  Manager em2{std::vector<std::string>{}};
  EXPECT_TRUE(em2.patterns_.empty());
  EXPECT_FALSE(em2.encode("<http://example.org/42>").has_value());
}

using encodedIri::Part;
using encodedIri::Pattern;

// The patterns that are used in the tests below, see
// `EncodedIriPatternTest.cpp` for a detailed description of their constraints
// and for the tests of the patterns themselves. The tests in this file only
// test the interplay of the patterns with the `EncodedIriManager`, in
// particular the assignment of the tags and the JSON format of the manager.
constexpr std::string_view basePrefix = "http://example.org/map#";

// Build the IRI `<http://example.org/map#rest>`.
std::string mapIri(std::string_view rest) {
  return absl::StrCat("<", basePrefix, rest, ">");
}

Pattern rangePattern() {
  return Pattern{
      absl::StrCat(basePrefix, "range_"),
      {Part{32, {{29, 32, 1}}, "_"}, Part{8, {}, "_"}, Part{8, {}, "P"}}};
}

Pattern refPattern(std::string_view name) {
  return Pattern{absl::StrCat(basePrefix, name),
                 {Part{64, {{17, 32, 0}, {61, 64, 1}}, "_"}, Part{4, {}, ""}}};
}

// Expect that the `iri` can be encoded by the `manager` and that decoding the
// result yields the `iri` again.
void expectRoundTrip(const EncodedIriManager& manager, const std::string& iri,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto id = manager.encode(iri);
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(manager.toString(id.value()), iri);
}

// Expect that the `iri` cannot be encoded by the `manager`.
void expectNotEncodable(
    const EncodedIriManager& manager, const std::string& iri,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_FALSE(manager.encode(iri).has_value());
}

// _____________________________________________________________________________
TEST(EncodedIriManager, SeveralPatternsWithTheSamePrefix) {
  // Two alternative patterns for the same prefix: the first number either is a
  // 64-bit number with fixed bits and is followed by a number smaller than 16,
  // or it is an arbitrary 32-bit number that is followed by an 18-bit number.
  // They are simply stored with different tags.
  Pattern first = refPattern("stopLoc_");
  Pattern second{absl::StrCat(basePrefix, "stopLoc_"),
                 {Part{32, {}, "_"}, Part{18, {}, ""}}};
  EncodedIriManager em{{}, {first, second}};

  auto tagOf = [&em](const std::string& iri) {
    auto id = em.encode(iri);
    AD_CONTRACT_CHECK(id.has_value());
    return EncodedIriManager::splitIntoPrefixIdxAndPayload(id.value()).first;
  };

  // Only matches the first pattern. Note that the tag 0 is taken by the
  // hardcoded prefix for the graphs that QLever creates itself.
  expectRoundTrip(em, mapIri("stopLoc_2343140642651111426_7"));
  EXPECT_EQ(tagOf(mapIri("stopLoc_2343140642651111426_7")), 1);
  // Only matches the second pattern.
  expectRoundTrip(em, mapIri("stopLoc_4294967295_262143"));
  expectRoundTrip(em, mapIri("stopLoc_545554944_0"));
  EXPECT_EQ(tagOf(mapIri("stopLoc_545554944_0")), 2);

  expectRoundTrip(em, mapIri("stopLoc_2305843009213693952_0"));

  // Matches neither of the patterns: the first number is too large for the
  // second pattern and doesn't have the required bits for the first one.
  expectNotEncodable(em, mapIri("stopLoc_4294967296_0"));
  // The second number is too large for both patterns.
  expectNotEncodable(em, mapIri("stopLoc_545554944_262144"));
  expectNotEncodable(em, mapIri("stopLoc_2343140642651111426_262144"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, PlainPrefixesAndPatternsTogether) {
  EncodedIriManager em{{"http://example.org/id/"}, {rangePattern()}};
  expectRoundTrip(em, "<http://example.org/id/12345>");
  // Leading zeros are preserved by the digit encoding of a plain prefix.
  expectRoundTrip(em, "<http://example.org/id/000123>");
  expectRoundTrip(em, mapIri("range_545554944_0_0P"));

  // The plain prefixes (and the hardcoded ones) come first, so the pattern gets
  // the last tag.
  auto id = em.encode(mapIri("range_545554944_0_0P"));
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(EncodedIriManager::splitIntoPrefixIdxAndPayload(id.value()).first,
            em.patterns_.size() - 1);
}

// _____________________________________________________________________________
TEST(EncodedIriManager, illegalPatterns) {
  using namespace ::testing;
  using V = std::vector<std::string>;
  using P = std::vector<Pattern>;
  auto expectThrow = [](Pattern pattern, const std::string& message,
                        ad_utility::source_location l =
                            AD_CURRENT_SOURCE_LOC()) {
    auto trace = generateLocationTrace(l);
    AD_EXPECT_THROW_WITH_MESSAGE(EncodedIriManager(V{}, P{std::move(pattern)}),
                                 HasSubstr(message));
  };
  // The prefix of a pattern is specified without the leading `<`, which the
  // manager adds itself.
  expectThrow(Pattern{"<http://example.org/", {Part{8, {}, ""}}},
              "enclosed in angle brackets");
  // The patterns are checked against the number of payload bits of the
  // manager (see `validatePattern` in `EncodedIriPatternTest.cpp`).
  expectThrow(Pattern{"http://example.org/", {Part{53, {}, ""}}},
              "it requires 53 bits, but only 52 bits are available");
}

// _____________________________________________________________________________
TEST(EncodedIriManager, patternsJson) {
  EncodedIriManager em{{"http://example.org/id/"},
                       {rangePattern(), refPattern("laneRef_")}};
  nlohmann::json j = em;
  // As soon as a general pattern is used, the extended format is written.
  EXPECT_TRUE(j.contains("patterns"));
  EXPECT_FALSE(j.contains("prefixes-with-leading-angle-brackets"));
  auto em2 = j.get<EncodedIriManager>();
  EXPECT_EQ(em, em2);
  expectRoundTrip(em2, "<http://example.org/id/12345>");
  expectRoundTrip(em2, mapIri("range_545554944_0_0P"));
  expectRoundTrip(em2, mapIri("laneRef_2343140642651111426_0"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, plainPrefixesUseTheLegacyJsonFormat) {
  // Without general patterns, the JSON format of the index metadata is
  // unchanged, which keeps the compatibility with existing indices.
  EncodedIriManager em{{"http://example.org/id/"}};
  nlohmann::json j = em;
  EXPECT_TRUE(j.contains("prefixes-with-leading-angle-brackets"));
  EXPECT_FALSE(j.contains("patterns"));
  EXPECT_EQ(j["prefixes-with-leading-angle-brackets"],
            nlohmann::json({"<http://example.org/id/",
                            absl::StrCat("<", AlwaysOnPrefixes::value.at(0))}));
  auto em2 = j.get<EncodedIriManager>();
  EXPECT_EQ(em, em2);
}

// _____________________________________________________________________________
TEST(EncodedIriManager, aPatternWithFewerDigitsIsNotPlain) {
  // A pattern that uses the digit encoding, but with fewer bits than a plain
  // prefix, must not be written in the legacy format, which would silently
  // widen it to the full number of payload bits.
  EncodedIriManager em{
      {},
      {Pattern{"http://example.org/",
               {Part{16, {}, "", encodedIri::NumberEncoding::Nibbles}}}}};
  nlohmann::json j = em;
  EXPECT_TRUE(j.contains("patterns"));
  EXPECT_EQ(j.get<EncodedIriManager>(), em);
  expectRoundTrip(em, "<http://example.org/1234>");
  expectNotEncodable(em, "<http://example.org/12345>");
}

// _____________________________________________________________________________
TEST(EncodedIriManager, tooManyPrefixesInJson) {
  // The number of patterns is also checked when they are read from the index,
  // because there are only `2 ^ 8` tags.
  std::vector<std::string> prefixes;
  for (size_t i = 0; i < 300; ++i) {
    prefixes.push_back(absl::StrCat("<prefix", i, "bla"));
  }
  nlohmann::json j;
  j["prefixes-with-leading-angle-brackets"] = prefixes;
  AD_EXPECT_THROW_WITH_MESSAGE(j.get<EncodedIriManager>(),
                               ::testing::HasSubstr("which is too many"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, invalidPatternsInJsonAreDetected) {
  using namespace ::testing;
  nlohmann::json j = EncodedIriManager{{}, {rangePattern()}};
  j["patterns"][0]["parts"][0]["num-bits"] = 65;
  AD_EXPECT_THROW_WITH_MESSAGE(j.get<EncodedIriManager>(),
                               HasSubstr("only 1 to 64 bits are supported"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, tooManyDigits) {
  // The manager encodes at most `NumDigits` digits. Note that `encode` never
  // triggers this, because it rejects too long digit sequences beforehand (see
  // `EncodedIriManager::encode`).
  using M = EncodedIriManager;
  EXPECT_NO_THROW(M::encodeDecimalToNBit(std::string(M::NumDigits, '9')));
  EXPECT_THROW(M::encodeDecimalToNBit(std::string(M::NumDigits + 1, '9')),
               std::out_of_range);
}
}  // namespace
