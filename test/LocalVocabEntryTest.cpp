// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "./util/GTestHelpers.h"
#include "./util/IndexTestHelpers.h"
#include "index/IndexImpl.h"
#include "index/LocalVocabEntry.h"

// Build two `LocalVocabEntry`s for the same word, but belonging to two
// different indices, and expect that `comparison(entry1, entry2)` throws,
// because comparing entries of different contexts is not allowed. The test is
// skipped if expensive checks are disabled, because the check that produces
// the error is one of them.
template <typename Comparison>
void testComparisonRequiresMatchingContexts(
    Comparison comparison,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP() << "This test only makes sense with expensive checks enabled.";
  }
  using ad_utility::testing::makeTestIndex;
  Index index1 = makeTestIndex(gtestCurrentTestName() + "-1", "<a> <b> <c> .");
  Index index2 = makeTestIndex(gtestCurrentTestName() + "-2", "<a> <b> <c> .");

  LocalVocabEntry entry1 = LocalVocabEntry::fromIriref(
      "<x>", index1.getImpl().getLocalVocabContext());
  LocalVocabEntry entry2 = LocalVocabEntry::fromIriref(
      "<x>", index2.getImpl().getLocalVocabContext());

  AD_EXPECT_THROW_WITH_MESSAGE(
      comparison(entry1, entry2),
      ::testing::HasSubstr(
          "Contexts of LocalVocabEntries have to be identical"));
}

// _____________________________________________________________________________
TEST(LocalVocabEntry, compareThreeWayRequiresMatchingContexts) {
  testComparisonRequiresMatchingContexts(
      [](const LocalVocabEntry& a, const LocalVocabEntry& b) {
        (void)(a < b);
      });
}

// _____________________________________________________________________________
TEST(LocalVocabEntry, compareThreeWayOnlyOneEntryIsContainedInVocabulary) {
  using namespace ad_utility::testing;
  // The comparison only looks at the position in the vocabulary if the index
  // has a secondary vocabulary, so create one. Note that this must not leak
  // into other tests, so build a fresh index instead of using a shared one.
  TestIndexConfig config{"<a> <b> <c> ."};
  config.secondaryVocabWords = std::vector<std::string>{"<zzz>"};
  Index index = makeTestIndex(gtestCurrentTestName(), std::move(config));
  const auto& ctx = index.getImpl().getLocalVocabContext();

  // `<b>` is contained in the vocabulary of the main index, so its position is
  // the single `Id` at which it is stored. `<aa>` is contained in none of the
  // vocabularies, so its position is the empty range at which it would be
  // sorted into the vocabulary of the main index, which is exactly the position
  // of `<b>`. The two entries hence have the same lower bound, and differ only
  // in whether they are contained.
  LocalVocabEntry contained = LocalVocabEntry::fromIriref("<b>", ctx);
  LocalVocabEntry notContained = LocalVocabEntry::fromIriref("<aa>", ctx);
  auto containedPosition = contained.positionInVocab();
  auto notContainedPosition = notContained.positionInVocab();
  ASSERT_EQ(containedPosition.lowerBound_, notContainedPosition.lowerBound_);
  ASSERT_NE(containedPosition.lowerBound_, containedPosition.upperBound_);
  ASSERT_EQ(notContainedPosition.lowerBound_, notContainedPosition.upperBound_);

  // The contained word is the greater one, because the word that is contained
  // in none of the vocabularies is strictly smaller than the word at its
  // position. This covers `isContained == true, rhsIsContained == false`.
  EXPECT_EQ(contained.compareThreeWay(notContained),
            ql::strong_ordering::greater);
  EXPECT_GT(contained, notContained);
  // The mirrored case, which covers
  // `isContained == false, rhsIsContained == true`.
  EXPECT_EQ(notContained.compareThreeWay(contained), ql::strong_ordering::less);
  EXPECT_LT(notContained, contained);
}

// _____________________________________________________________________________
// `compareThreeWaySemantically` compares the string values, whereas
// `compareThreeWay` implements the internal order, in which a word of the
// secondary vocabulary is positioned after all words of the main vocabulary,
// see the class comment of `LocalVocabEntry`.
TEST(LocalVocabEntry, compareThreeWaySemantically) {
  using namespace ad_utility::testing;
  // The two orders only differ if the index has a secondary vocabulary, so
  // create one. Note that this must not leak into other tests, so build a fresh
  // index instead of using a shared one.
  TestIndexConfig config{"<s> <p> <a> . <s> <p> <c> ."};
  config.secondaryVocabWords = std::vector<std::string>{"<b>"};
  Index index = makeTestIndex(gtestCurrentTestName(), std::move(config));
  const auto& ctx = index.getImpl().getLocalVocabContext();
  ASSERT_TRUE(ctx.hasSecondaryVocabulary());
  auto entry = [&ctx](std::string_view iriref) {
    return LocalVocabEntry::fromIriref(iriref, ctx);
  };

  // `<a>` is a word of the main vocabulary, `<b>` is stored in the secondary
  // vocabulary, and `<e>` is in neither.
  LocalVocabEntry entryA = entry("<a>");
  LocalVocabEntry entryB = entry("<b>");
  LocalVocabEntry entryE = entry("<e>");

  // Semantically, the entries are ordered by their string values.
  EXPECT_EQ(entryA.compareThreeWaySemantically(entryB),
            ql::strong_ordering::less);
  EXPECT_EQ(entryB.compareThreeWaySemantically(entryE),
            ql::strong_ordering::less);
  EXPECT_EQ(entryE.compareThreeWaySemantically(entryB),
            ql::strong_ordering::greater);
  EXPECT_EQ(entryB.compareThreeWaySemantically(entryB),
            ql::strong_ordering::equal);
  EXPECT_EQ(entryB.compareThreeWaySemantically(entry("<b>")),
            ql::strong_ordering::equal);

  // Internally, `<b>` is greater than both of the others, because it is
  // positioned in the secondary vocabulary, and that is also what the
  // comparison operators (and hence sorting) use.
  EXPECT_EQ(entryB.compareThreeWay(entryA), ql::strong_ordering::greater);
  EXPECT_EQ(entryB.compareThreeWay(entryE), ql::strong_ordering::greater);
  EXPECT_EQ(entryE.compareThreeWay(entryB), ql::strong_ordering::less);
  EXPECT_LT(entryE, entryB);
  EXPECT_GT(entryB, entryE);

  // The two orders agree on entries that are not stored in the secondary
  // vocabulary.
  EXPECT_EQ(entryA.compareThreeWay(entryE),
            entryA.compareThreeWaySemantically(entryE));
  EXPECT_EQ(entryE.compareThreeWay(entryA),
            entryE.compareThreeWaySemantically(entryA));
  EXPECT_EQ(entryA.compareThreeWay(entryA), ql::strong_ordering::equal);
}

// _____________________________________________________________________________
// Just like `compareThreeWay`, the semantic comparison requires that both
// entries belong to the same index.
TEST(LocalVocabEntry, compareThreeWaySemanticallyRequiresMatchingContexts) {
  testComparisonRequiresMatchingContexts(
      [](const LocalVocabEntry& a, const LocalVocabEntry& b) {
        (void)a.compareThreeWaySemantically(b);
      });
}
