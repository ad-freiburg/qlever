// Copyright 2026 The QLever Authors, in particular:
//
// 2019 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
// 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "index/vocabulary/StringSortComparator.h"
using namespace std::literals;
using ad_utility::source_location;

// _____________________________________________________________________________
TEST(LocaleManagerTest, Levels) {
  using L = LocaleManager::Level;
  LocaleManager loc;

  ASSERT_EQ(loc.compare("alpha", "ALPHA", L::SECONDARY), 0);
  ASSERT_LT(loc.compare("alpha", "ALPHA", L::TERTIARY), 0);
  ASSERT_EQ(loc.compare("älpha", "ALPHA", L::PRIMARY), 0);
  ASSERT_GT(loc.compare("älpha", "ALPHA", L::SECONDARY), 0);
}

// _____________________________________________________________________________
TEST(LocaleManagerTest, getLowercaseUtf8) {
  LocaleManager loc;
  ASSERT_EQ("schindler's list", loc.getLowercaseUtf8("Schindler's List"));
  ASSERT_EQ("#+-_foo__bar++", loc.getLowercaseUtf8("#+-_foo__Bar++"));
  ASSERT_EQ("fôéßaéé", loc.getLowercaseUtf8("FÔÉßaéÉ"));
}

// _____________________________________________________________________________
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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(LocaleManagerTest, StartsWithOnPrimaryLevel) {
  LocaleManager loc("en", "US", false);
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("hello", ""));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("", ""));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("hello", "hel"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("hello", "HEL"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("hello", "hello"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("héllo", "hel"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("hello", "hé"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("hello", "help"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("he", "hello"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("", "a"));

  // Characters that expand to several collation elements.
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("groß", "gros"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("große", "gross"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("gross", "groß"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("vivæ", "viva"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("vivæ", "vivb"));

  // Characters without a primary weight (here a combining acute accent).
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("cafe\xcc\x81s", "caf\xc3\xa9"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("cafes", "cafe\xcc\x81"));

  // Punctuation is relevant if it is not ignored.
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel(".hello", "hello"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("a.b", "ab"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("a.b", "a."));

  // Characters with a primary weight longer than 16 bits. The upper 16 bits of
  // the weights of these two characters are equal.
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("中文", "中"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("中", "中文"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("丮", "中"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("中", "丮"));
}

// _____________________________________________________________________________
TEST(LocaleManagerTest, StartsWithOnPrimaryLevelIgnorePunctuation) {
  LocaleManager loc("en", "US", true);
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel(".hello", "hello"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("hello", ".h.e"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("a.b", "ab"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("ab", "a."));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("abc", "..."));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("", "\"<@ "));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("hello world", "hellow"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("a.c", "ab"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("...", "a"));
}

// _____________________________________________________________________________
TEST(LocaleManagerTest, HaveEqualPrimaryPrefix) {
  LocaleManager loc("en", "US", false);
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("abcdx", "abcdy", 4));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("abcdx", "abcdy", 5));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("abcd", "ABCD", 4));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("ab", "ab", 4));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("ab", "AB", 4));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("ab", "abcd", 4));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("abcd", "ab", 4));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("a", "b", 0));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("", "", 4));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("", "a", 4));

  // "æ" has the same primary weights as "ae".
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("vivæ", "vivae", 4));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("vivæt", "vivaeb", 5));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("vivæt", "vivaeb", 6));

  // A primary weight longer than 16 bits is a single element.
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("中a", "中b", 1));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("中a", "中b", 2));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("中a", "丮a", 1));

  LocaleManager ignorePunct("en", "US", true);
  EXPECT_TRUE(ignorePunct.haveEqualPrimaryPrefix("a.bcd", "abce", 3));
  EXPECT_FALSE(ignorePunct.haveEqualPrimaryPrefix("a.bcd", "abce", 4));
  EXPECT_TRUE(ignorePunct.haveEqualPrimaryPrefix("...", "", 4));
}

#ifndef QLEVER_NO_UNICODE
// The following tests are specific to the ICU-based `LocaleManagerICU`.

// _____________________________________________________________________________
TEST(LocaleManagerTest, BogusLocaleThrows) {
  // A language string that is too long for ICU yields a "bogus" locale, which
  // the constructor must reject.
  AD_EXPECT_THROW_WITH_MESSAGE(
      LocaleManagerICU(std::string(1000, 'a'), "US", false),
      ::testing::HasSubstr("Could not create locale"));
}

// _____________________________________________________________________________
TEST(LocaleManagerTest, CopyAssignment) {
  using L = LocaleManager::Level;
  LocaleManagerICU ignorePunct("en", "US", true);
  LocaleManagerICU respectPunct("en", "US", false);
  // Precondition: the two managers disagree on punctuation handling.
  ASSERT_EQ(ignorePunct.compare(".a", "a", L::PRIMARY), 0);
  ASSERT_LT(respectPunct.compare(".a", "a", L::PRIMARY), 0);
  // Copy-assignment transfers the locale settings.
  respectPunct = ignorePunct;
  EXPECT_EQ(respectPunct.compare(".a", "a", L::PRIMARY), 0);
  // Self-assignment is a no-op (via a reference to avoid `-Wself-assign`).
  LocaleManagerICU& ref = respectPunct;
  respectPunct = ref;
  EXPECT_EQ(respectPunct.compare(".a", "a", L::PRIMARY), 0);
}

// _____________________________________________________________________________
TEST(LocaleManagerTest, RaiseThrowsOnIcuError) {
  // Force an ICU error through the public `compare`: a `string_view` with null
  // data but nonzero size makes `compareUTF8` fail with
  // `U_ILLEGAL_ARGUMENT_ERROR`, which `raise` turns into an exception. The null
  // pointer is never dereferenced, as ICU reports the error first.
  LocaleManagerICU loc;
  std::string_view nullView{static_cast<const char*>(nullptr), 5};
  AD_EXPECT_THROW_WITH_MESSAGE(
      (void)loc.compare(nullView, "a", LocaleManager::Level::PRIMARY),
      ::testing::HasSubstr("U_ILLEGAL_ARGUMENT_ERROR"));
}
#endif  // QLEVER_NO_UNICODE

// The following tests exercise the ICU-free (bytewise) `LocaleManagerNoICU`.
// They are always compiled and run, regardless of whether QLever is built with
// ICU, so that the ICU-free code path is covered.

// _____________________________________________________________________________
TEST(LocaleManager, NoICUPrimaryPrefix) {
  LocaleManagerNoICU loc;
  // Every byte is its own collation element.
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("abc", "ab"));
  EXPECT_TRUE(loc.startsWithOnPrimaryLevel("abc", ""));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("abc", "AB"));
  EXPECT_FALSE(loc.startsWithOnPrimaryLevel("ab", "abc"));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("abcd", "abce", 3));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("abcd", "abce", 4));
  EXPECT_TRUE(loc.haveEqualPrimaryPrefix("ab", "ab", 4));
  EXPECT_FALSE(loc.haveEqualPrimaryPrefix("ab", "abc", 4));
}

// _____________________________________________________________________________
TEST(LocaleManager, NoICU) {
  using L = LocaleManagerNoICU::Level;
  LocaleManagerNoICU loc;

  // Comparison is bytewise, so (unlike ICU) case and punctuation matter and the
  // collation level is irrelevant.
  for (L level : {L::PRIMARY, L::SECONDARY, L::TERTIARY, L::QUARTERNARY,
                  L::IDENTICAL, L::TOTAL}) {
    // 'A' (65) < 'a' (97).
    EXPECT_LT(loc.compare("ALPHA", "alpha", level), 0);
    EXPECT_GT(loc.compare("alpha", "ALPHA", level), 0);
    EXPECT_EQ(loc.compare("alpha", "alpha", level), 0);
    EXPECT_LT(loc.compare("alpha", "beta", level), 0);
  }

  // As a preparatory step the lowercasing still reuses the ICU-based
  // `ad_utility::utf8ToLower`.
  EXPECT_EQ("schindler's list", loc.getLowercaseUtf8("Schindler's List"));
  EXPECT_EQ("café", loc.getLowercaseUtf8("CAFé"));

  // Normalization is a no-op.
  std::string composed = "\xc3\xa9";     // é as a single codepoint
  std::string decomposed = "e\xcc\x81";  // é as e + combining accent
  EXPECT_EQ(loc.normalizeUtf8(composed), composed);
  EXPECT_EQ(loc.normalizeUtf8(decomposed), decomposed);
}
