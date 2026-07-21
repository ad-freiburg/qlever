// Copyright 2019, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#include <gtest/gtest.h>

#include "./util/GTestHelpers.h"
#include "backports/StartsWithAndEndsWith.h"
#include "index/StringSortComparator.h"
using namespace std::literals;
using ad_utility::source_location;

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

// The following tests exercise the ICU-free (bytewise) comparators. They are
// always compiled and run, regardless of whether QLever is built with ICU, so
// that the ICU-free code paths are covered.

// ______________________________________________________________________________
TEST(StringSortComparatorNoICU, SimpleStringComparator) {
  SimpleStringComparatorNoICU comp("en", "US", true);

  // Bytewise ordering: uppercase letters come before lowercase ones.
  EXPECT_TRUE(comp("ALPHA", "alpha"));
  EXPECT_FALSE(comp("alpha", "ALPHA"));
  EXPECT_TRUE(comp("alpha", "beta"));
  EXPECT_FALSE(comp("beta", "alpha"));

  // Something is not smaller than itself.
  EXPECT_FALSE(comp("beta", "beta"));

  // Consistency with the `SortKey`-based overload on the PRIMARY level.
  using L = SimpleStringComparatorNoICU::Level;
  auto sortKeyBeta = comp.getLocaleManager().getSortKey("beta", L::PRIMARY);
  EXPECT_TRUE(comp("alpha", sortKeyBeta, L::PRIMARY));
  EXPECT_FALSE(comp("gamma", sortKeyBeta, L::PRIMARY));
}

// ______________________________________________________________________________
TEST(StringSortComparatorNoICU, TripleComponentComparator) {
  TripleComponentComparatorNoICU comp("en", "US", false);
  using L = TripleComponentComparatorNoICU::Level;

  // The inner value is compared bytewise, so casing DOES affect the order
  // (in contrast to the ICU-based comparator).
  EXPECT_TRUE(comp("\"ALPHA\"", "\"beta\""));   // 'A' (65) < 'b' (98)
  EXPECT_TRUE(comp("\"ALPHA\"", "\"alpha\""));  // 'A' (65) < 'a' (97)
  EXPECT_FALSE(comp("\"alpha\"", "\"ALPHA\""));
  EXPECT_TRUE(comp("\"alpha\"", "\"beta\""));

  // Something is not smaller than itself.
  EXPECT_FALSE(comp("\"beta\"", "\"beta\""));

  // The datatype (first character) is compared first.
  EXPECT_TRUE(comp("\"zzz\"", "<aaa>"));  // '"' (34) < '<' (60)

  // On the TOTAL level the language tag is a tiebreaker.
  EXPECT_TRUE(comp("\"Hannibal\"@af", "\"Hannibal\"@en", L::TOTAL));
  EXPECT_FALSE(comp("\"Hannibal\"@en", "\"Hannibal\"@af", L::TOTAL));

  // `isLessInTotalWithExternalFlag` breaks ties on equal values by the flag.
  EXPECT_TRUE(
      comp.isLessInTotalWithExternalFlag("\"beta\"", true, "\"beta\"", false));
  EXPECT_FALSE(
      comp.isLessInTotalWithExternalFlag("\"beta\"", false, "\"beta\"", true));

  // `normalizeUtf8` is a no-op in the ICU-free variant.
  EXPECT_EQ(comp.normalizeUtf8("\xc3\xa9"), "\xc3\xa9");
}
