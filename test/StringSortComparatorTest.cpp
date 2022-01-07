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
