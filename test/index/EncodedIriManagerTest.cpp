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
TEST(EncodedIriManager, splitIntoPrefixIdxAndPayload) {
  EncodedIriManager em{{"blabb", "blubb"}};
  auto id = em.encode("<blubb42>");
  ASSERT_TRUE(id.has_value());
  auto [prefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(id.value());
  EXPECT_EQ(prefixIdx, 1);
  std::string result;
  EncodedIriManager::NibbleEncoder::decodeToString(result, payload);
  EXPECT_EQ(result, "42");
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager::splitIntoPrefixIdxAndPayload(Id::makeUndefined()),
      ::testing::HasSubstr("must be `EncodedVal`"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, makeIdFromPrefixIdxAndPayload) {
  EncodedIriManager em{{"blabb", "blubb"}};
  auto id = EncodedIriManager::makeIdFromPrefixIdxAndPayload(
      1, EncodedIriManager::NibbleEncoder::encode("7643").value());
  EXPECT_EQ(em.toString(id), "<blubb7643>");
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
      testing::HasSubstr("is always added automatically and must not be "
                         "specified explicitly"));
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
// Tests for prefixes with bit constraints (see
// `encodedIris::BitRangeConstraint`). The numbers used here are typical of the
// use case that motivated the feature: external 64-bit IDs whose top three bits
// are a constant tag `001` and which have a window of zero bits, so that 12 of
// the 64 bits carry no information and do not have to be stored.
using BitRangeConstraint = encodedIris::BitRangeConstraint;
using PrefixWithConstraints = encodedIris::PrefixWithConstraints;

// A prefix whose numbers have bits [61, 64) equal to `001` and bits [23, 32)
// equal to zero; 3 + 9 = 12 constrained bits, so 52 remain, which is exactly
// `NumBitsEncoding`.
PrefixWithConstraints laneConstraints() {
  return PrefixWithConstraints{"http://example.org/lane_",
                               {{61, 64, 0b001}, {23, 32, 0u}}};
}

TEST(EncodedIriManager, ConstraintsRoundTrip) {
  auto em = EncodedIriManager::fromPrefixesWithConstraints({laneConstraints()});
  // A real ID that fulfills the constraints: bits [61, 64) are `001` and bits
  // [23, 32) are zero.
  uint64_t value = 2343140642651111426ULL;
  ASSERT_EQ((value >> 61) & 0b111, 0b001u);
  ASSERT_EQ((value >> 23) & 0x1FFu, 0u);
  std::string iri = absl::StrCat("<http://example.org/lane_", value, ">");

  auto id = em.encode(iri);
  ASSERT_TRUE(id.has_value());
  EXPECT_EQ(id.value().getDatatype(), Datatype::EncodedVal);
  // The IRI, and in particular the full 64-bit number, is recovered exactly,
  // although only 52 bits are available for the payload.
  EXPECT_EQ(em.toString(id.value()), iri);
  EXPECT_EQ(em.getNumberOfConstrainedId(id.value()), value);
}

TEST(EncodedIriManager, ConstraintsViolatedIsNotEncoded) {
  auto em = EncodedIriManager::fromPrefixesWithConstraints({laneConstraints()});
  // Bits [61, 64) are `000` instead of `001`.
  EXPECT_FALSE(em.encode("<http://example.org/lane_42>").has_value());
  // Bits [61, 64) are correct, but a bit in [23, 32) is set.
  uint64_t base = 1ULL << 61;
  EXPECT_FALSE(em.encode(absl::StrCat("<http://example.org/lane_",
                                      base | (1ULL << 25), ">"))
                   .has_value());
  // The same number is encodable once the offending bit is gone.
  EXPECT_TRUE(em.encode(absl::StrCat("<http://example.org/lane_",
                                     base | (1ULL << 22), ">"))
                  .has_value());
  // A number that does not fit into 64 bits at all.
  EXPECT_FALSE(
      em.encode("<http://example.org/lane_99999999999999999999>").has_value());
}

TEST(EncodedIriManager, ConstraintsWithGapAndSeveralPrefixes) {
  // Three prefixes with different constraint sets, including one whose three
  // constraints leave bit 23 free in between.
  auto em = EncodedIriManager::fromPrefixesWithConstraints(
      {laneConstraints(),
       PrefixWithConstraints{"http://example.org/roadPart_",
                             {{61, 64, 0b001}, {22, 23, 0}, {24, 32, 0u}}},
       PrefixWithConstraints{"http://example.org/dp_",
                             {{61, 64, 0b001}, {23, 32, 0}}}});
  // For `roadPart_` bit 23 is unconstrained, so both values round-trip.
  uint64_t base = 1ULL << 61;
  for (uint64_t bit23 : {0ULL, 1ULL << 23}) {
    auto iri = absl::StrCat("<http://example.org/roadPart_",
                            base | bit23 | 12345ULL, ">");
    auto id = em.encode(iri);
    ASSERT_TRUE(id.has_value()) << iri;
    EXPECT_EQ(em.toString(id.value()), iri);
  }
  // Each prefix uses its own constraints.
  auto dpIri = absl::StrCat("<http://example.org/dp_", base | 777ULL, ">");
  auto dpId = em.encode(dpIri);
  ASSERT_TRUE(dpId.has_value());
  EXPECT_EQ(em.toString(dpId.value()), dpIri);
}

TEST(EncodedIriManager, ConstraintsAndPlainPrefixesMixed) {
  auto em = EncodedIriManager::fromPrefixesWithConstraints(
      {laneConstraints(), PrefixWithConstraints{"http://plain.org/"}});
  // The plain prefix still uses the digit encoding, with all its properties
  // (in particular the lexicographic order and preserved leading zeros).
  auto plain = em.encode("<http://plain.org/007>");
  ASSERT_TRUE(plain.has_value());
  EXPECT_EQ(em.toString(plain.value()), "<http://plain.org/007>");
  // A number that is too long for the digit encoding is not encoded.
  EXPECT_FALSE(em.encode("<http://plain.org/12345678901234>").has_value());
  // The constrained prefix works alongside it.
  auto constrained = em.encode(
      absl::StrCat("<http://example.org/lane_", 2343140642651111426ULL, ">"));
  ASSERT_TRUE(constrained.has_value());
  EXPECT_NE(plain.value(), constrained.value());
}

TEST(EncodedIriManager, ConstraintsJsonRoundTrip) {
  auto em = EncodedIriManager::fromPrefixesWithConstraints(
      {laneConstraints(), PrefixWithConstraints{"http://plain.org/"}});
  nlohmann::json j = em;
  auto em2 = j.get<EncodedIriManager>();
  EXPECT_EQ(em, em2);
  auto iri =
      absl::StrCat("<http://example.org/lane_", 2343140642651111426ULL, ">");
  EXPECT_EQ(em2.toString(em2.encode(iri).value()), iri);
}

TEST(EncodedIriManager, JsonWithoutConstraintsIsUnchangedAndReadable) {
  // An index that was built before the constraints existed has no constraint
  // key at all, and must still be readable.
  EncodedIriManager em{{"http://example.org/"}};
  nlohmann::json j = em;
  EXPECT_FALSE(j.contains("prefix-bit-constraints"));
  auto em2 = j.get<EncodedIriManager>();
  EXPECT_EQ(em, em2);
  EXPECT_EQ(em2.toString(em2.encode("<http://example.org/42>").value()),
            "<http://example.org/42>");
}

// NOTE: The constraints themselves are tested in
// `EncodedIriBitConstraintTest.cpp`; the tests here only cover that the manager
// applies and reports them.
TEST(EncodedIriManager, InvalidConstraintsAreRejected) {
  // Overlapping ranges.
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager::fromPrefixesWithConstraints({PrefixWithConstraints{
          "http://example.org/", {{10, 20, 0}, {15, 25, 0}}}}),
      testing::HasSubstr("must not overlap"));
  // Too few constrained bits: 64 - 4 = 60 bits remain, but only 52 are
  // available.
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager::fromPrefixesWithConstraints(
          {PrefixWithConstraints{"http://example.org/", {{0, 4, 0}}}}),
      testing::HasSubstr("constrain at least 12 bits"));
  // The same prefix twice with different constraints.
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager::fromPrefixesWithConstraints(
          {laneConstraints(),
           PrefixWithConstraints{"http://example.org/lane_",
                                 {{61, 64, 0b001}, {24, 33, 0u}}}}),
      testing::HasSubstr("with different constraints"));
}

// _____________________________________________________________________________
// Regression test: for a constrained prefix the number is stored, not the
// digits, so a number with leading zeros must NOT be encoded. Otherwise
// `<pfx_007>` and `<pfx_7>` would get the same `Id` although they are different
// IRIs.
TEST(EncodedIriManager, ConstraintsRejectLeadingZeros) {
  auto em = EncodedIriManager::fromPrefixesWithConstraints({laneConstraints()});
  uint64_t value = 2343140642651111426ULL;
  auto withoutZeros =
      em.encode(absl::StrCat("<http://example.org/lane_", value, ">"));
  ASSERT_TRUE(withoutZeros.has_value());
  // A single leading zero, and many of them, are both rejected.
  EXPECT_FALSE(em.encode(absl::StrCat("<http://example.org/lane_0", value, ">"))
                   .has_value());
  EXPECT_FALSE(em.encode(absl::StrCat("<http://example.org/lane_",
                                      std::string(30, '0'), value, ">"))
                   .has_value());
  // `0` itself is a single digit and has no leading zero, so it is not affected
  // by the check (it violates the constraints here and is rejected for that
  // reason).
  EXPECT_FALSE(em.encode("<http://example.org/lane_0>").has_value());
}

// _____________________________________________________________________________
// Regression test: a constrained prefix can produce a payload of `0` (namely
// for the number that consists of exactly the constrained values with all free
// bits zero). The digit decoder must not be confused by that, and the round
// trip must still work.
TEST(EncodedIriManager, ConstraintsPayloadZero) {
  auto em = EncodedIriManager::fromPrefixesWithConstraints({laneConstraints()});
  // Bits [61, 64) are `001` and everything else is zero, so all free bits are
  // zero and the payload becomes 0.
  uint64_t value = 1ULL << 61;
  auto iri = absl::StrCat("<http://example.org/lane_", value, ">");
  auto id = em.encode(iri);
  ASSERT_TRUE(id.has_value());
  auto [prefixIdx, payload] =
      EncodedIriManager::splitIntoPrefixIdxAndPayload(id.value());
  EXPECT_EQ(payload, 0u);
  EXPECT_EQ(em.toString(id.value()), iri);
  EXPECT_EQ(em.getNumberOfConstrainedId(id.value()), value);
}

// _____________________________________________________________________________
// Constraints that come from (possibly corrupted) index metadata are validated
// on deserialization, so that a violation is reported there and not deep inside
// the parser.
TEST(EncodedIriManager, ConstraintsFromJsonAreValidated) {
  nlohmann::json j;
  j["prefixes-with-leading-angle-brackets"] =
      std::vector<std::string>{"<http://example.org/"};
  // Overlapping ranges.
  j["prefix-bit-constraints"] = std::vector<std::vector<BitRangeConstraint>>{
      {BitRangeConstraint{10, 20, 0}, BitRangeConstraint{15, 25, 0}}};
  AD_EXPECT_THROW_WITH_MESSAGE(j.get<EncodedIriManager>(),
                               testing::HasSubstr("must not overlap"));
  // Constraints that are stored in an arbitrary order are accepted and
  // normalized, because the order carries no meaning.
  nlohmann::json j2;
  j2["prefixes-with-leading-angle-brackets"] =
      std::vector<std::string>{"<http://example.org/lane_"};
  j2["prefix-bit-constraints"] = std::vector<std::vector<BitRangeConstraint>>{
      {BitRangeConstraint{61, 64, 0b001}, BitRangeConstraint{23, 32, 0}}};
  auto em = j2.get<EncodedIriManager>();
  auto iri =
      absl::StrCat("<http://example.org/lane_", 2343140642651111426ULL, ">");
  EXPECT_EQ(em.toString(em.encode(iri).value()), iri);
}

}  // namespace
