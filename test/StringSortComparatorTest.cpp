// Copyright 2019, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "index/StringSortComparator.h"
using namespace std::literals;
using ad_utility::source_location;

TEST(LocaleManagerTest, Levels) {
  using L = LocaleManager::Level;
  LocaleManager loc;

  ASSERT_EQ(loc.compare("alpha", "ALPHA", L::SECONDARY), 0);
  ASSERT_LT(loc.compare("alpha", "ALPHA", L::TERTIARY), 0);
  ASSERT_EQ(loc.compare("älpha", "ALPHA", L::PRIMARY), 0);
  ASSERT_GT(loc.compare("älpha", "ALPHA", L::SECONDARY), 0);
}

TEST(LocaleManagerTest, getLowercaseUtf8) {
  LocaleManager loc;
  ASSERT_EQ("schindler's list", loc.getLowercaseUtf8("Schindler's List"));
  ASSERT_EQ("#+-_foo__bar++", loc.getLowercaseUtf8("#+-_foo__Bar++"));
  ASSERT_EQ("fôéßaéé", loc.getLowercaseUtf8("FÔÉßaéÉ"));
}

TEST(LocaleManagerTest, Punctuation) {
  using L = LocaleManager::Level;
  {
    LocaleManager loc("en", "US", false);
    ASSERT_LT(loc.compare("a.c", "ab", L::IDENTICAL), 0);
    ASSERT_LT(loc.compare(".a", "a", L::IDENTICAL), 0);
    ASSERT_LT(loc.compare(".a", "a", L::PRIMARY), 0);
  }
  {
    LocaleManager loc("en", "US", true);
    ASSERT_GT(loc.compare("a.c", "ab", L::IDENTICAL), 0);
    ASSERT_LT(loc.compare(".a", "a", L::IDENTICAL), 0);
    ASSERT_EQ(loc.compare(".a", "a", L::PRIMARY), 0);
    ASSERT_EQ(loc.compare(".a", "#?a", L::PRIMARY), 0);
    ASSERT_EQ(loc.compare(".a", "#?a", L::TERTIARY), 0);
    ASSERT_LT(loc.compare(".a", "#?a", L::QUARTERNARY), 0);
  }
}

TEST(LocaleManagerTest, Normalization) {
  // é as single codepoints
  std::string as = "\xc3\xa9"s;
  // é as e + accent aigu
  std::string bs = "e\xcc\x81"s;
  ASSERT_EQ(2u, as.size());
  ASSERT_EQ(3u, bs.size());
  LocaleManager loc;
  auto resA = loc.normalizeUtf8(as);
  auto resB = loc.normalizeUtf8(bs);
  ASSERT_EQ(resA, resB);
  ASSERT_EQ(resA, as);
}

// ______________________________________________________________________________________________
TEST(StringSortComparatorTest, TripleComponentComparatorQuarternary) {
  TripleComponentComparator comp("en", "US", false);

  // strange casings must not affect order
  ASSERT_TRUE(comp("\"ALPHA\"", "\"beta\""));
  ASSERT_TRUE(comp("\"alpha\"", "\"BETA\""));
  ASSERT_TRUE(comp("\"AlPha\"", "\"bEtA\""));
  ASSERT_TRUE(comp("\"AlP\"", "\"alPha\""));
  ASSERT_TRUE(comp("\"alP\"", "\"ALPha\""));

  // inverse tests for completeness
  ASSERT_FALSE(comp("\"beta\"", "\"ALPHA\""));
  ASSERT_FALSE(comp("\"BETA\"", "\"alpha\""));
  ASSERT_FALSE(comp("\"bEtA\"", "\"AlPha\""));
  ASSERT_FALSE(comp("\"alPha\"", "\"AlP\""));
  ASSERT_FALSE(comp("\"ALPha\"", "\"alP\""));

  // only if lowercased version is exactly the same we want to sort by the
  // casing (lowercase comes first in the default en_US.utf8-locale
  ASSERT_TRUE(comp("\"alpha\"", "\"ALPHA\""));
  ASSERT_FALSE(comp("\"ALPHA\"", "\"alpha\""));

  ASSERT_TRUE(comp("\"Hannibal\"@en", "\"Hannibal Hamlin\"@en"));

  // language tags are ignored on the default quarternary level
  ASSERT_FALSE(comp("\"Hannibal\"@af", "\"Hannibal\"@en"));
  ASSERT_FALSE(comp("\"Hannibal\"@en", "\"Hannibal\"@af"));

  ASSERT_TRUE(comp("\"Hannibal\"@en", "\"HanNibal\"@en"));

  // something is not smaller than itself
  ASSERT_FALSE(comp("\"beta\"", "\"beta\""));

  // Testing that latin and Hindi numbers mean exactly the same up to the
  // Quarternary level
  using L = TripleComponentComparator::Level;
  ASSERT_FALSE(
      comp("\"151\"", "\"१५१\"", L::QUARTERNARY));  // that is 151 in Hindi
  ASSERT_FALSE(comp("\"१५१\"", "\"151\"", L::QUARTERNARY));
  ASSERT_TRUE(comp("\"151\"", "\"१५१\"", L::IDENTICAL));
  ASSERT_FALSE(comp("\"१५१\"", "\"151\"", L::IDENTICAL));

  ASSERT_TRUE(comp("\"151\"@en", "\"१५१\"", L::IDENTICAL));
  ASSERT_FALSE(comp("\"१५१\"", "\"151\"@en", L::QUARTERNARY));
  ASSERT_FALSE(comp("\"151\"@en", "\"१५१\"", L::QUARTERNARY));
}

TEST(StringSortComparatorTest, TripleComponentComparatorTotal) {
  TripleComponentComparator comparator("en", "US", false);
  auto comp = [&comparator](const auto& a, const auto& b) {
    return comparator(a, b, TripleComponentComparator::Level::TOTAL);
  };
  auto assertTrue = [&comp](const auto& a, const auto& b,
                            source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto tr = generateLocationTrace(l);
    ASSERT_TRUE(comp(a, b));
  };
  auto assertFalse = [&comp](const auto& a, const auto& b,
                             source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto tr = generateLocationTrace(l);
    ASSERT_FALSE(comp(a, b));
  };

  // strange casings must not affect order
  assertTrue("\"ALPHA\"", "\"beta\"");
  assertTrue("\"alpha\"", "\"BETA\"");
  assertTrue("\"AlPha\"", "\"bEtA\"");
  assertTrue("\"AlP\"", "\"alPha\"");
  assertTrue("\"alP\"", "\"ALPha\"");

  // inverse tests for completeness
  assertFalse("\"beta\"", "\"ALPHA\"");
  assertFalse("\"BETA\"", "\"alpha\"");
  assertFalse("\"bEtA\"", "\"AlPha\"");
  assertFalse("\"alPha\"", "\"AlP\"");
  assertFalse("\"ALPha\"", "\"alP\"");

  // only if lowercased version is exactly the same we want to sort by the
  // casing (lowercase comes first in the default en_US.utf8-locale
  assertTrue("\"alpha\"", "\"ALPHA\"");
  assertFalse("\"ALPHA\"", "\"alpha\"");

  assertTrue("\"Hannibal\"@en", "\"Hannibal Hamlin\"@en");

  // language tags matter on the TOTAL level
  assertTrue("\"Hannibal\"@af", "\"Hannibal\"@en");
  assertFalse("\"Hannibal\"@en", "\"Hannibal\"@af");

  assertTrue("\"Hannibal\"@en", "\"HanNibal\"@en");

  // something is not smaller than itself
  assertFalse("\"beta\"", "\"beta\"");

  // Testing that latin and Hindi numbers mean exactly the same up to the
  // Quarternary level
  assertTrue("\"151\"", "\"१५१\"");
  assertFalse("\"१५१\"", "\"151\"");

  assertTrue("\"151\"@en", "\"१५१\"");
  assertFalse("\"१५१\"", "\"151\"@en");
}

// ______________________________________________________________________________________________
TEST(StringSortComparatorTest, IsLessInTotalWithExternalFlag) {
  TripleComponentComparator comp("en", "US", false);

  // When the strings differ on the TOTAL level, the external flags must not
  // influence the result.
  for (bool aExt : {false, true}) {
    for (bool bExt : {false, true}) {
      EXPECT_TRUE(comp.isLessInTotalWithExternalFlag("\"alpha\"", aExt,
                                                     "\"beta\"", bExt));
      EXPECT_FALSE(comp.isLessInTotalWithExternalFlag("\"beta\"", aExt,
                                                      "\"alpha\"", bExt));
      // Language tags are a tiebreaker on the TOTAL level.
      EXPECT_TRUE(comp.isLessInTotalWithExternalFlag("\"Hannibal\"@af", aExt,
                                                     "\"Hannibal\"@en", bExt));
      EXPECT_FALSE(comp.isLessInTotalWithExternalFlag("\"Hannibal\"@en", aExt,
                                                      "\"Hannibal\"@af", bExt));
    }
  }

  // When the strings are equal on the TOTAL level, the external flag breaks
  // the tie: `external == true` comes before `external == false`.
  EXPECT_TRUE(
      comp.isLessInTotalWithExternalFlag("\"beta\"", true, "\"beta\"", false));
  EXPECT_FALSE(
      comp.isLessInTotalWithExternalFlag("\"beta\"", false, "\"beta\"", true));
  // Equal strings with equal flags are never less than each other
  // (irreflexive).
  EXPECT_FALSE(
      comp.isLessInTotalWithExternalFlag("\"beta\"", true, "\"beta\"", true));
  EXPECT_FALSE(
      comp.isLessInTotalWithExternalFlag("\"beta\"", false, "\"beta\"", false));

  // Use the function to sort a sequence of (string, isExternal) entries and
  // verify that equal entries with `isExternal == true` come first.
  using Entry = std::pair<std::string, bool>;
  std::vector<Entry> entries{
      {"\"beta\"", false}, {"\"alpha\"", false}, {"\"beta\"", true},
      {"\"alpha\"", true}, {"\"beta\"", false},
  };
  ql::ranges::sort(entries, [&comp](const Entry& a, const Entry& b) {
    return comp.isLessInTotalWithExternalFlag(a.first, a.second, b.first,
                                              b.second);
  });
  EXPECT_THAT(entries, ::testing::ElementsAre(
                           Entry{"\"alpha\"", true}, Entry{"\"alpha\"", false},
                           Entry{"\"beta\"", true}, Entry{"\"beta\"", false},
                           Entry{"\"beta\"", false}));
}

// ______________________________________________________________________________________________
TEST(StringSortComparatorTest, SimpleStringComparator) {
  SimpleStringComparator comp("en", "US", true);

  // strange casings must not affect order
  ASSERT_TRUE(comp("ALPHA", "beta"));
  ASSERT_TRUE(comp("alpha", "BETA"));
  ASSERT_TRUE(comp("AlPha", "bEtA"));
  ASSERT_TRUE(comp("AlP", "alPha"));
  ASSERT_TRUE(comp("alP", "ALPha"));

  // inverse tests for completeness
  ASSERT_FALSE(comp("beta", "ALPHA"));
  ASSERT_FALSE(comp("BETA", "alpha"));
  ASSERT_FALSE(comp("bEtA", "AlPha"));
  ASSERT_FALSE(comp("alPha", "AlP"));
  ASSERT_FALSE(comp("ALPha", "alP"));

  // only if lowercased version is exactly the same we want to sort by the
  // casing (lowercase comes first in the default en_US.utf8-locale
  ASSERT_TRUE(comp("alpha", "ALPHA"));
  ASSERT_FALSE(comp("ALPHA", "alpha"));

  // something is not smaller than itself
  ASSERT_FALSE(comp("beta", "beta"));

  ASSERT_TRUE(comp("\"@u2", "@u2"));
  ASSERT_FALSE(comp("@u2", "\"@u2"));
}

// ______________________________________________________________________________________________
TEST(LocaleManagerTest, CountPrimaryCollationElements) {
  LocaleManager loc("en", "US", false);
  EXPECT_EQ(loc.countPrimaryCollationElements(""), 0u);
  EXPECT_EQ(loc.countPrimaryCollationElements("hello"), 5u);
  // Accented characters count as one primary element each.
  EXPECT_EQ(loc.countPrimaryCollationElements("héllo"), 5u);
  // Multi-byte UTF-8: é = U+00E9 = 2 bytes, still 1 primary element.
  EXPECT_EQ(loc.countPrimaryCollationElements("\xc3\xa9"), 1u);
  // Punctuation has raw primary weight even with ignorePunctuation=true,
  // because CollationElementIterator returns raw weights; UCOL_SHIFTED is only
  // applied during comparison, not during element iteration.
  EXPECT_EQ(loc.countPrimaryCollationElements(".hello"), 6u);
  EXPECT_EQ(loc.countPrimaryCollationElements("hello world"), 11u);
  EXPECT_EQ(loc.countPrimaryCollationElements("..."), 3u);
}

// ______________________________________________________________________________________________
TEST(LocaleManagerTest, PrimaryCollationPrefixLength) {
  LocaleManager loc("en", "US", false);

  // Edge cases.
  EXPECT_EQ(loc.primaryCollationPrefixLength("hello", 0), 0u);
  EXPECT_EQ(loc.primaryCollationPrefixLength("", 5), 0u);
  // Fewer elements than requested: return the full string length.
  EXPECT_EQ(loc.primaryCollationPrefixLength("hi", 10), 2u);

  // Basic ASCII: one byte per codepoint, one primary element per letter.
  EXPECT_EQ(loc.primaryCollationPrefixLength("hello world", 5), 5u);
  // 6th element is the space, so offset includes it.
  EXPECT_EQ(loc.primaryCollationPrefixLength("hello world", 6), 6u);

  // Multi-byte UTF-8: é = U+00E9 = 2 bytes but 1 primary element.
  // "héllo" = h(1) + é(2) + l(1) + l(1) + o(1) = 6 bytes total.
  EXPECT_EQ(loc.primaryCollationPrefixLength("héllo", 1), 1u);  // "h"
  EXPECT_EQ(loc.primaryCollationPrefixLength("héllo", 2), 3u);  // "hé"
  EXPECT_EQ(loc.primaryCollationPrefixLength("héllo", 5), 6u);  // "héllo"

  // "." has raw primary weight, so it counts as element 1.
  EXPECT_EQ(loc.primaryCollationPrefixLength(".hello", 1), 1u);  // "."
  EXPECT_EQ(loc.primaryCollationPrefixLength(".hello", 6), 6u);  // ".hello"

  // Round-trip: countPrimaryCollationElements(s) elements should cover all of
  // s.
  for (std::string_view s :
       {"hello"sv, "héllo"sv, ".hello"sv, "hello world"sv}) {
    size_t n = loc.countPrimaryCollationElements(s);
    EXPECT_EQ(loc.primaryCollationPrefixLength(s, n), s.size());
  }
}
