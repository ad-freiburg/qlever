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
#include "index/vocabulary/encodedIris/EncodedIriManager.h"
#include "util/Random.h"
#include "util/TransparentFunctors.h"

namespace {
// Get `num` random indices in the range `[min, max]`. Additionally, add the min
// and the max to the result explicitly, to automatically test corner cases.
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

// The prefix of the Wikidata entity IRIs, which several tests below use.
constexpr std::string_view wikidataQ = "http://www.wikidata.org/entity/Q";

// Create an `EncodedIriManager` that encodes the `wikidataQ` prefix.
EncodedIriManager makeWikidataManager() {
  return EncodedIriManager{std::vector<std::string>{std::string{wikidataQ}}};
}

// Check that the `manager` can encode the `iri` and that decoding the result
// yields the `iri` again. Return the `Id` of the encoded `iri`.
// Note: It is tempting to compare `Id`s directly here, but that requires to
// pull in the equality comparison for `Id`s, which requires linking against
// basically the whole codebase.
Id expectRoundTrip(const EncodedIriManager& manager, std::string_view iri,
                   ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto id = manager.encode(iri);
  EXPECT_TRUE(id.has_value());
  if (!id.has_value()) {
    return Id::makeUndefined();
  }
  EXPECT_EQ(manager.toString(id.value()), iri);
  return id.value();
}

// Check that the `manager` can encode the `iri`, without also checking the
// decoding.
void expectEncodable(const EncodedIriManager& manager, std::string_view iri,
                     ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_TRUE(manager.encode(iri).has_value());
}

// The opposite of `expectEncodable`.
void expectNotEncodable(
    const EncodedIriManager& manager, std::string_view iri,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_FALSE(manager.encode(iri).has_value());
}

// Check that `manager.getIndexOfPrefix` returns the position in the sorted
// `expectedPrefixes` for each of them, and `std::nullopt` for a prefix that
// the `manager` does not know.
void expectPrefixIndices(
    const EncodedIriManager& manager, std::vector<std::string> expectedPrefixes,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  ql::ranges::sort(expectedPrefixes);
  for (const auto& [i, prefix] : ranges::views::enumerate(expectedPrefixes)) {
    EXPECT_THAT(manager.getIndexOfPrefix(prefix),
                testing::Optional(testing::Eq(i)));
  }
  EXPECT_THAT(manager.getIndexOfPrefix("http://example.org"),
              testing::Eq(std::nullopt));
}

// _____________________________________________________________________________
TEST(EncodedIriManger, SimpleExample) {
  expectRoundTrip(makeWikidataManager(), absl::StrCat("<", wikidataQ, "423>"));
}

// _____________________________________________________________________________
TEST(EncodedIriManger, EncodingAndDecoding) {
  auto indices =
      getRandomIndices(0, (1ull << encodedIri::NumDigits) - 1, 10'000);
  std::vector<std::pair<std::string, uint64_t>> stringsAndEncodings;
  auto encodedIriManager = makeWikidataManager();
  for (auto index : indices) {
    std::string wdq = absl::StrCat("<", wikidataQ, index, ">");
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

  auto i1 = expectRoundTrip(encodedIriManager, s1);
  auto i2 = expectRoundTrip(encodedIriManager, s2);
  EXPECT_NE(i1.getBits(), i2.getBits());
}

// _____________________________________________________________________________
TEST(EncodedIriManger, Unencodable) {
  auto encodedIriManager = makeWikidataManager();
  std::vector<std::string> unencodable = {
      absl::StrCat("<", wikidataQ, "42a3>"),
      // One digit more than fits into the encoding.
      absl::StrCat("<", wikidataQ, std::string(encodedIri::NumDigits + 1, '3'),
                   ">"),
      "<notAValidPrefix>",
      absl::StrCat("<", wikidataQ, "42a3"),  // missing trailing '>'
  };
  for (const auto& s : unencodable) {
    expectNotEncodable(encodedIriManager, s);
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
  auto q42 = absl::StrCat("<", wikidataQ, "42>");

  // Calls the default constructor.
  EncodedIriManager em;
  expectNotEncodable(em, q42);

  // Calls the constructor with an explicitly empty list of prefixes.
  EncodedIriManager em2(std::vector<std::string>{});
  expectNotEncodable(em2, q42);
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

// The encoding is usable in a constant expression. This is not only a nice
// property, but also required: a `constexpr` function that can never yield a
// constant expression is ill-formed, and several compilers reject it (see the
// note in `NibbleEncoding.h`).
// The digit `1` is stored as the nibble `2` in the leftmost nibble.
static_assert(EncodedIriManager::encodeDecimalToNBit("1") ==
              uint64_t{2} << (encodedIri::NumBitsEncoding -
                              encodedIri::NibbleSize));

// _____________________________________________________________________________
TEST(EncodedIriManager, decodeDecimalFrom64Bit) {
  auto testNumber = [](uint64_t number, ad_utility::source_location l =
                                            AD_CURRENT_SOURCE_LOC()) {
    using m = EncodedIriManager;
    auto trace = generateLocationTrace(l);
    EXPECT_EQ(number, m::decodeDecimalFrom64Bit(
                          m::encodeDecimalToNBit(std::to_string(number))));
  };
  uint64_t MAX = std::stoull(std::string(encodedIri::NumDigits, '9'));
  testNumber(0);
  testNumber(MAX);
  auto intGenerator = ad_utility::SlowRandomIntGenerator<uint64_t>(0, MAX);
  for (auto _ = 0; _ < 20; ++_) {
    testNumber(intGenerator());
  }
}

// _____________________________________________________________________________
TEST(EncodedIriManager, getIndexOfPrefix) {
  // Create a list of all prefixes, including the always-on ones, for testing
  // the function.
  auto withAlwaysOnPrefixes = [](std::vector<std::string> customPrefixes) {
    for (auto prefix : encodedIri::AlwaysOnPrefixes) {
      customPrefixes.emplace_back(prefix);
    }
    return customPrefixes;
  };

  // No custom prefixes, so only the always-on ones have to be tested.
  expectPrefixIndices(EncodedIriManager(), withAlwaysOnPrefixes({}));

  std::vector<std::string> customPrefixes = {"http://qlever.dev"};
  expectPrefixIndices(EncodedIriManager(customPrefixes),
                      withAlwaysOnPrefixes(customPrefixes));
}

// A set of always-on prefixes that differs from the default
// `encodedIri::AlwaysOnPrefixes`, for the tests below.
constexpr std::array<std::string_view, 1> testAlwaysOnPrefixes = {
    "http://example.org/always/"};

// _____________________________________________________________________________
TEST(EncodedIriManager, AlwaysOnPrefixes) {
  // Without any explicit prefixes, the always-on prefix is still encoded.
  EncodedIriManager em{{}, testAlwaysOnPrefixes};
  expectRoundTrip(em, "<http://example.org/always/42>");

  // Additional prefixes are added on top of the always-on ones.
  EncodedIriManager em2{{"http://other.org/"}, testAlwaysOnPrefixes};
  expectEncodable(em2, "<http://example.org/always/99>");
  expectEncodable(em2, "<http://other.org/1>");
}

// _____________________________________________________________________________
TEST(EncodedIriManager, cannotAddAlwaysOnPrefixes) {
  // Adding an always-on prefix a second time in the constructor is an error.
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager({std::string{testAlwaysOnPrefixes.at(0)}},
                        testAlwaysOnPrefixes),
      testing::HasSubstr(
          "!ad_utility::contains(prefixesWithoutAngleBrackets, prefix)"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, AlwaysOnPrefixesJson) {
  // The always-on prefixes are part of the JSON representation, so that an
  // index that was built with different ones can still be read.
  EncodedIriManager em{{"http://other.org/"}, testAlwaysOnPrefixes};
  nlohmann::json j = em;
  auto em2 = j.get<EncodedIriManager>();
  expectRoundTrip(em2, "<http://example.org/always/42>");
  expectEncodable(em2, "<http://other.org/1>");
}

}  // namespace
