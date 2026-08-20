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

// _____________________________________________________________________________
TEST(LocalVocabEntry, compareThreeWayRequiresMatchingContexts) {
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
      (void)(entry1 < entry2),
      ::testing::HasSubstr(
          "Contexts of LocalVocabEntries have to be identical"));
}

// _____________________________________________________________________________
TEST(LocalVocabEntry, compareThreeWayOnlyOneEntryIsContainedInVocabulary) {
  using namespace ad_utility::testing;
  // The comparison only looks at the position in the vocabulary if the index
  // has an auxiliary vocabulary, so create one. Note that this must not leak
  // into other tests, so build a fresh index instead of using a shared one.
  TestIndexConfig config{"<a> <b> <c> ."};
  config.auxVocabWords = std::vector<std::string>{"<zzz>"};
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
