// Copyright 2019, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
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
  // Test that the comparison between `a` and  `b` always yields the same
  // result, no matter if it is done on the level of strings or on `SortKey`s.
  auto assertConsistent = [&comparator, &comp](
                              const auto& a, const auto& b,
                              source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto tr = generateLocationTrace(l);
    bool ab = comp(a, b);
    bool ba = comp(b, a);
    auto aSplit = comparator.extractAndTransformComparable(
        a, TripleComponentComparator::Level::TOTAL);
    auto bSplit = comparator.extractAndTransformComparable(
        b, TripleComponentComparator::Level::TOTAL);
    EXPECT_EQ(ab, comp(aSplit, bSplit));
    EXPECT_EQ(ab, comp(a, bSplit));
    EXPECT_EQ(ab, comp(aSplit, b));

    EXPECT_EQ(ba, comp(bSplit, aSplit));
    EXPECT_EQ(ba, comp(b, aSplit));
    EXPECT_EQ(ba, comp(bSplit, a));
  };

  auto assertTrue = [&comp, &assertConsistent](
                        const auto& a, const auto& b,
                        source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto tr = generateLocationTrace(l);
    ASSERT_TRUE(comp(a, b));
    assertConsistent(a, b);
  };
  auto assertFalse = [&comp, &assertConsistent](
                         const auto& a, const auto& b,
                         source_location l = AD_CURRENT_SOURCE_LOC()) {
    auto tr = generateLocationTrace(l);
    ASSERT_FALSE(comp(a, b));
    assertConsistent(a, b);
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

TEST(LocaleManager, PrefixSortKey) {
  SimpleStringComparator comp("en", "US", true);
  LocaleManager locIgnorePunct = comp.getLocaleManager();
  LocaleManager locRespectPunct("en", "US", false);

  auto print = []([[maybe_unused]] const auto& s) {
    // The following code can be used for convenient debug output.
    /*
    for (const auto& ch : s) {
      std::cout << int(ch) << ' ';
    }
    std::cout << std::endl;
     */
  };

  // Assert that all possible prefix sort keys of `s` are indeed prefixes
  // of the `SortKey` of `s`.
  auto testSortKeysForLocale = [print](std::string_view s,
                                       const LocaleManager& loc) {
    auto complete = loc.getSortKey(s, LocaleManager::Level::PRIMARY).get();
    print(complete);
    for (size_t i = 0; i < s.size(); ++i) {
      auto [numCodepoints, partial] = loc.getPrefixSortKey(s, i);
      (void)numCodepoints;
      ASSERT_TRUE(ql::starts_with(complete, partial.get()));
      print(partial.get());
    }
  };

  auto testSortKeys = [&testSortKeysForLocale, &locIgnorePunct,
                       &locRespectPunct](std::string_view s) {
    testSortKeysForLocale(s, locIgnorePunct);
    testSortKeysForLocale(s, locRespectPunct);
  };

  testSortKeys("original");
  testSortKeys("Häll!!ö.ö");

  testSortKeys("vivæ");
  testSortKeys("vivae");
  testSortKeys("vivaret");

  testSortKeys("viɡorous");
  testSortKeys("vigorous");

  // Show the current limitations:
  // The words vivæ and vivae compare equal on the primary level, but they
  // get different prefixSortKeys for prefix length 4, because "ae" are two
  // codepoints, whereas "æ" is one.
  auto a = locIgnorePunct.getPrefixSortKey("vivæ", 4).second;
  auto b = locIgnorePunct.getPrefixSortKey("vivae", 4).second;

  ASSERT_GT(a.size(), b.size());
  ASSERT_TRUE(a.starts_with(b));
  // Also test the defaulted consistent comparison.
  ASSERT_GT(a, b);
  ASSERT_EQ(a, a);
  ASSERT_NE(a, b);
  ASSERT_FALSE(comp("vivæ", "vivae", LocaleManager::Level::PRIMARY));
  ASSERT_FALSE(comp("vivæ", "vivae", LocaleManager::Level::PRIMARY));
}
