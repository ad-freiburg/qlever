// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "../util/EncodedIriSchemeTestHelpers.h"
#include "../util/GTestHelpers.h"
#include "index/EncodedIriManager.h"
#include "index/EncodedIriScheme.h"
#include "util/Random.h"
#include "util/TransparentFunctors.h"

namespace {
using ad_utility::testing::TwoNumbersScheme;
using ad_utility::testing::twoNumbersScheme;

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

// A scheme that can be configured arbitrarily (in particular in invalid ways),
// to test the validation of the schemes by the `EncodedIriManager`. It cannot
// encode anything.
class ConfigurableScheme : public qlever::EncodedIriScheme {
 private:
  std::string name_;
  std::vector<std::string> prefixes_;
  size_t numTags_;
  size_t numPayloadBits_;

 public:
  ConfigurableScheme(std::string name, std::vector<std::string> prefixes,
                     size_t numTags = 1, size_t numPayloadBits = 8)
      : name_{std::move(name)},
        prefixes_{std::move(prefixes)},
        numTags_{numTags},
        numPayloadBits_{numPayloadBits} {}

  std::string name() const override { return name_; }
  std::vector<std::string> prefixes() const override { return prefixes_; }
  size_t numTags() const override { return numTags_; }
  size_t numPayloadBits() const override { return numPayloadBits_; }
  std::optional<TagAndPayload> encode(
      [[maybe_unused]] std::string_view iriWithAngleBrackets) const override {
    return std::nullopt;
  }
  std::string decode([[maybe_unused]] size_t localTag,
                     [[maybe_unused]] uint64_t payload) const override {
    AD_FAIL();
  }
  Numbers decodeNumbers([[maybe_unused]] size_t localTag,
                        [[maybe_unused]] uint64_t payload) const override {
    AD_FAIL();
  }
  nlohmann::json toJson() const override { return nlohmann::json::object(); }
};

// Helper to create a manager with the `TwoNumbersScheme` and the given plain
// prefixes.
EncodedIriManager managerWithTwoNumbersScheme(
    std::vector<std::string> prefixes = {}) {
  return EncodedIriManager{std::move(prefixes), {twoNumbersScheme()}};
}

// _____________________________________________________________________________
TEST(EncodedIriManager, customSchemeEncodeAndDecode) {
  auto manager = managerWithTwoNumbersScheme({"http://example.org/"});
  for (const auto& iri : {"<somePrefix://num_123_anotherNum_24>",
                          "<somePrefix://num_0_otherNum_0>",
                          "<somePrefix://num_999999_anotherNum_1>"}) {
    auto id = manager.encode(iri);
    ASSERT_TRUE(id.has_value()) << iri;
    EXPECT_EQ(manager.toString(id.value()), iri);
  }

  // The numbers can be extracted directly from the `Id`.
  auto id = manager.encode("<somePrefix://num_123_anotherNum_24>");
  ASSERT_TRUE(id.has_value());
  EXPECT_THAT(manager.decodeNumbers(id.value()),
              ::testing::ElementsAre(123, 24));

  // For a plain prefix, `decodeNumbers` returns the single encoded number.
  auto plainId = manager.encode("<http://example.org/42>");
  ASSERT_TRUE(plainId.has_value());
  EXPECT_THAT(manager.decodeNumbers(plainId.value()),
              ::testing::ElementsAre(42));

  // The two variants of the scheme use different tags.
  auto first = manager.encode("<somePrefix://num_1_anotherNum_2>");
  auto second = manager.encode("<somePrefix://num_1_otherNum_2>");
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  EXPECT_NE(
      EncodedIriManager::splitIntoPrefixIdxAndPayload(first.value()).first,
      EncodedIriManager::splitIntoPrefixIdxAndPayload(second.value()).first);
}

// _____________________________________________________________________________
TEST(EncodedIriManager, customSchemeUnencodable) {
  auto manager = managerWithTwoNumbersScheme();
  std::vector<std::string> unencodable{
      // Not the structure that the scheme expects.
      "<somePrefix://num_123_yetAnotherNum_24>",
      "<somePrefix://num_123_anotherNum_>",
      "<somePrefix://num_123_anotherNum_24",
      "<somePrefix://num_12a_anotherNum_24>",
      // Too many digits for the 24 bits (6 digits) per number.
      "<somePrefix://num_1234567_anotherNum_24>",
      "<somePrefix://num_1_anotherNum_1234567>",
      // Doesn't start with the prefix of the scheme.
      "<someOtherPrefix://num_1_anotherNum_2>"};
  for (const auto& iri : unencodable) {
    EXPECT_FALSE(manager.encode(iri).has_value()) << iri;
  }
}

// _____________________________________________________________________________
TEST(EncodedIriManager, customSchemeReservesTags) {
  // The tags are assigned in the lexicographic order of the prefixes, and the
  // two tags of the scheme are contiguous. Note that the hardcoded prefix
  // (`QLEVER_NEW_GRAPH_PREFIX`) also uses a tag.
  auto manager = managerWithTwoNumbersScheme({"aaa", "zzz"});
  auto tagOf = [&manager](const std::string& iri) {
    auto id = manager.encode(iri);
    AD_CONTRACT_CHECK(id.has_value());
    return EncodedIriManager::splitIntoPrefixIdxAndPayload(id.value()).first;
  };
  // The tags are `<aaa` (0), the hardcoded `QLEVER_NEW_GRAPH_PREFIX` (1), the
  // two tags of the scheme (2 and 3), and `<zzz` (4).
  EXPECT_THAT(manager.getIndexOfPrefix("aaa"), ::testing::Optional(0u));
  EXPECT_THAT(manager.getIndexOfPrefix(QLEVER_NEW_GRAPH_PREFIX),
              ::testing::Optional(1u));
  EXPECT_EQ(tagOf("<somePrefix://num_1_anotherNum_2>"), 2u);
  EXPECT_EQ(tagOf("<somePrefix://num_1_otherNum_2>"), 3u);
  EXPECT_EQ(tagOf("<zzz42>"), 4u);
  // The prefixes of a scheme are not found by `getIndexOfPrefix`, which only
  // deals with the plain prefixes.
  EXPECT_EQ(manager.getIndexOfPrefix("somePrefix://num_"), std::nullopt);

  ASSERT_EQ(manager.schemes().size(), 1u);
  EXPECT_EQ(manager.schemes().at(0).firstTag_, 2u);
  EXPECT_EQ(manager.schemes().at(0).numTags_, 2u);
}

// _____________________________________________________________________________
TEST(EncodedIriManager, customSchemeOrdering) {
  // The scheme encodes both numbers in an order-preserving way, so within one
  // of its variants, the order of the `Id`s is the lexicographic order of the
  // two digit strings.
  auto manager = managerWithTwoNumbersScheme();
  std::vector<std::pair<std::string, uint64_t>> pairsAndEncodings;
  for (std::string_view first : {"1", "12", "2", "999999"}) {
    for (std::string_view second : {"0", "10", "9"}) {
      auto iri = absl::StrCat("<somePrefix://num_", first, "_anotherNum_",
                              second, ">");
      auto id = manager.encode(iri);
      ASSERT_TRUE(id.has_value()) << iri;
      pairsAndEncodings.emplace_back(absl::StrCat(first, " ", second),
                                     id.value().getBits());
    }
  }
  auto sortedByBits = pairsAndEncodings;
  ql::ranges::sort(pairsAndEncodings, ql::ranges::less{}, ad_utility::first);
  ql::ranges::sort(sortedByBits, ql::ranges::less{}, ad_utility::second);
  EXPECT_THAT(pairsAndEncodings, ::testing::ElementsAreArray(sortedByBits));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, customSchemeJson) {
  auto manager = managerWithTwoNumbersScheme({"http://example.org/"});
  nlohmann::json j = manager;
  auto manager2 = j.get<EncodedIriManager>();
  // The tags and the schemes are exactly restored.
  EXPECT_EQ(manager, manager2);
  for (const auto& iri :
       {"<somePrefix://num_123_anotherNum_24>",
        "<somePrefix://num_123_otherNum_24>", "<http://example.org/42>"}) {
    auto id = manager.encode(iri);
    auto id2 = manager2.encode(iri);
    ASSERT_TRUE(id.has_value()) << iri;
    ASSERT_TRUE(id2.has_value()) << iri;
    EXPECT_EQ(id2.value().getBits(), id.value().getBits());
    EXPECT_EQ(manager2.toString(id.value()), iri);
  }

  // A manager without any schemes writes exactly the same JSON as before the
  // schemes were introduced.
  nlohmann::json withoutSchemes = EncodedIriManager{{"http://example.org/"}};
  EXPECT_FALSE(withoutSchemes.contains(EncodedIriManager::schemesJsonKey_));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, customSchemeJsonUnknownScheme) {
  nlohmann::json j = managerWithTwoNumbersScheme();
  auto& scheme = j[EncodedIriManager::schemesJsonKey_][0]
                  [EncodedIriManager::schemeJsonKey_];
  scheme[qlever::EncodedIriScheme::nameKey_] = "a-scheme-that-is-not-linked-in";
  AD_EXPECT_THROW_WITH_MESSAGE(
      j.get<EncodedIriManager>(),
      ::testing::AllOf(::testing::HasSubstr("a-scheme-that-is-not-linked-in"),
                       ::testing::HasSubstr("no such scheme is registered"),
                       ::testing::HasSubstr(TwoNumbersScheme::schemeName())));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, customSchemeInvalidSchemes) {
  using V = std::vector<std::string>;
  using S = std::vector<qlever::EncodedIriSchemePtr>;
  using ::testing::HasSubstr;
  auto scheme = [](std::string name, V prefixes, size_t numTags = 1,
                   size_t numPayloadBits = 8) {
    return std::make_shared<ConfigurableScheme>(
        std::move(name), std::move(prefixes), numTags, numPayloadBits);
  };

  AD_EXPECT_THROW_WITH_MESSAGE(EncodedIriManager(V{}, S{nullptr}),
                               HasSubstr("must not be `nullptr`"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{}, S{scheme("", V{"a"})}),
      HasSubstr("name of an encoding scheme for IRIs must not be"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{}, S{scheme("s", V{"a"}, 0)}),
      HasSubstr("has to reserve at least one tag"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{}, S{scheme("s", V{"a"}, 1, 53)}),
      HasSubstr("requires 53 bits for its payload"));
  AD_EXPECT_THROW_WITH_MESSAGE(EncodedIriManager(V{}, S{scheme("s", V{})}),
                               HasSubstr("has to specify at least one prefix"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{}, S{scheme("s", V{"<a"})}),
      HasSubstr("must not be enclosed in angle brackets"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{}, S{scheme("s", V{"a"}), scheme("s", V{"b"})}),
      HasSubstr("same name"));
  // A prefix of a scheme must not be a prefix of a plain prefix or of the
  // prefix of another scheme.
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{"abc"}, S{scheme("s", V{"ab"})}),
      HasSubstr("may be a prefix of another"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{}, S{scheme("s", V{"ab"}), scheme("t", V{"abc"})}),
      HasSubstr("may be a prefix of another"));
  // The tags of the schemes also count towards the maximum number of tags.
  AD_EXPECT_THROW_WITH_MESSAGE(
      EncodedIriManager(V{}, S{scheme("s", V{"a"}, 256)}),
      HasSubstr("which is too many"));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, checkSchemesMatch) {
  auto manager = managerWithTwoNumbersScheme();
  EXPECT_NO_THROW(manager.checkSchemesMatch({twoNumbersScheme()}));
  // A scheme with a different configuration is detected.
  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.checkSchemesMatch({twoNumbersScheme("otherPrefix://num_")}),
      ::testing::HasSubstr("differ from the ones that the index was built"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      manager.checkSchemesMatch({}),
      ::testing::HasSubstr("differ from the ones that the index was built"));
  EXPECT_NO_THROW(EncodedIriManager{}.checkSchemesMatch({}));
}

// _____________________________________________________________________________
TEST(EncodedIriManager, schemeRegistry) {
  // Registering the same class twice is a no-op.
  EXPECT_NO_THROW(qlever::registerEncodedIriScheme<TwoNumbersScheme>());
  // Registering a different class under the same name is an error. The
  // `ConfigurableScheme` is not registered anywhere else, so this doesn't
  // affect the other tests.
  struct OtherScheme : ConfigurableScheme {
    OtherScheme() : ConfigurableScheme{TwoNumbersScheme::schemeName(), {"a"}} {}
    static std::string schemeName() { return TwoNumbersScheme::schemeName(); }
    static qlever::EncodedIriSchemePtr fromJson(const nlohmann::json&) {
      return std::make_shared<OtherScheme>();
    }
  };
  AD_EXPECT_THROW_WITH_MESSAGE(
      qlever::registerEncodedIriScheme<OtherScheme>(),
      ::testing::HasSubstr("Two different classes were registered"));
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

}  // namespace
