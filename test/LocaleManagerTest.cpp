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

// _____________________________________________________________________________
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
TEST(LocaleManager, NoICUPrimaryCollationElements) {
  LocaleManagerNoICU loc;
  // Every byte is its own collation element.
  EXPECT_EQ(loc.countPrimaryCollationElements(""), 0u);
  EXPECT_EQ(loc.countPrimaryCollationElements("abc"), 3u);
  EXPECT_EQ(loc.countPrimaryCollationElements("\xc3\xa9"), 2u);
  EXPECT_EQ(loc.primaryCollationPrefixLength("abcdef", 3), 3u);
  EXPECT_EQ(loc.primaryCollationPrefixLength("abc", 10), 3u);
  EXPECT_EQ(loc.primaryCollationPrefixLength("", 5), 0u);
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
