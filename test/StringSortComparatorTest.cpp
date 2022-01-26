// Copyright 2019, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>

#include "../src/index/StringSortComparator.h"
using namespace std::literals;

using namespace std::literals;

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

  // something is not smaller thant itself
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

  // language tags matter on the TOTAL level
  ASSERT_TRUE(comp("\"Hannibal\"@af", "\"Hannibal\"@en"));
  ASSERT_FALSE(comp("\"Hannibal\"@en", "\"Hannibal\"@af"));

  ASSERT_TRUE(comp("\"Hannibal\"@en", "\"HanNibal\"@en"));

  // something is not smaller thant itself
  ASSERT_FALSE(comp("\"beta\"", "\"beta\""));

  // Testing that latin and Hindi numbers mean exactly the same up to the
  // Quarternary level
  ASSERT_TRUE(comp("\"151\"", "\"१५१\""));
  ASSERT_FALSE(comp("\"१५१\"", "\"151\""));

  ASSERT_TRUE(comp("\"151\"@en", "\"१५१\""));
  ASSERT_FALSE(comp("\"१५१\"", "\"151\"@en"));
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

  // something is not smaller thant itself
  ASSERT_FALSE(comp("beta", "beta"));

  ASSERT_TRUE(comp("\"@u2", "@u2"));
  ASSERT_FALSE(comp("@u2", "\"@u2"));
}

TEST(LocaleManager, PrefixSortKey) {
  SimpleStringComparator comp("en", "US", true);
  LocaleManager locIgnorePunct = comp.getLocaleManager();
  LocaleManager locRespectPunct("en", "US", false);

  auto print = []([[maybe_unused]] std::string_view s) {
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
      ASSERT_TRUE(ad_utility::startsWith(complete, partial.get()));
      print(partial.get());
    }
    std::cout << std::endl;
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
