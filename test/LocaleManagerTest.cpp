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
#include "backports/StartsWithAndEndsWith.h"
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
TEST(LocaleManager, PrefixSortKey) {
  SimpleStringComparator comp("en", "US", true);
  LocaleManager locIgnorePunct = comp.getLocaleManager();
  LocaleManager locRespectPunct("en", "US", false);

  // Assert that all possible prefix sort keys of `s` are indeed prefixes
  // of the `SortKey` of `s`.
  auto testSortKeysForLocale = [](std::string_view s,
                                  const LocaleManager& loc) {
    auto complete = loc.getSortKey(s, LocaleManager::Level::PRIMARY).get();
    for (size_t i = 0; i < s.size(); ++i) {
      auto [numCodepoints, partial] = loc.getPrefixSortKey(s, i);
      (void)numCodepoints;
      ASSERT_TRUE(ql::starts_with(complete, partial.get()));
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
TEST(LocaleManager, NoICUPrefixSortKey) {
  using L = LocaleManagerNoICU::Level;
  LocaleManagerNoICU loc;
  // The bytewise prefix sort key is the first `min(prefixLength, size)` bytes.
  auto expectPrefix = [&loc](std::string_view s, size_t prefixLength,
                             size_t expectedNum,
                             std::string_view expectedBytes) {
    auto [num, key] = loc.getPrefixSortKey(s, prefixLength);
    EXPECT_EQ(num, expectedNum);
    EXPECT_EQ(key.get(), loc.getSortKey(expectedBytes, L::PRIMARY).get());
  };
  // Prefix shorter than the string.
  expectPrefix("abcdef", 3, 3, "abc");
  // Prefix length exceeding the string: the whole string is returned.
  expectPrefix("abc", 10, 3, "abc");
  // Empty string.
  expectPrefix("", 5, 0, "");
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

  // The sort key is just the bytes of the input.
  auto sortKey = loc.getSortKey("abc", L::PRIMARY);
  EXPECT_EQ(sortKey.get().size(), 3u);
  EXPECT_EQ(loc.compare(loc.getSortKey("abc", L::PRIMARY),
                        loc.getSortKey("abd", L::PRIMARY)),  // codespell-ignore
            -1);
}
