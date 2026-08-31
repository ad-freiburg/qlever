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
#include "index/LocalVocabContext.h"
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
TEST(LocalVocabContext, lookupWordInVocabulariesForContainedWord) {
  const IndexImpl& index =
      ad_utility::testing::getQec("<a> <b> <c> .")->getIndex().getImpl();
  const LocalVocabContext& context = index.getLocalVocabContext();

  // A word that is contained in the vocabulary is reported via its `Id`, which
  // is the (unique) position of that word in the vocabulary.
  for (std::string_view word : {"<a>", "<b>", "<c>"}) {
    auto [lower, upper] = index.getVocab().getPositionOfWord(word);
    ASSERT_EQ(upper.get(), lower.get() + 1)
        << "The word " << word << " should be in the vocabulary.";
    EXPECT_THAT(context.lookupWordInVocabularies(word),
                ::testing::VariantWith<Id>(Id::makeFromVocabIndex(lower)));
  }
}

// _____________________________________________________________________________
TEST(LocalVocabContext, lookupWordInVocabulariesForWordNotContained) {
  const IndexImpl& index =
      ad_utility::testing::getQec("<a> <b> <c> .")->getIndex().getImpl();
  const LocalVocabContext& context = index.getLocalVocabContext();

  // A word that is not contained in the vocabulary is reported via the bounds
  // of the position at which it would be sorted into the vocabulary. Those
  // bounds are equal, precisely because the word is not contained.
  std::string_view word = "<not-contained>";
  auto bounds = index.getVocab().getPositionOfWord(word);
  ASSERT_EQ(bounds.first, bounds.second)
      << "The word " << word << " should not be in the vocabulary.";
  EXPECT_THAT(context.lookupWordInVocabularies(word),
              ::testing::VariantWith<LocalVocabContext::VocabBounds>(bounds));
}

// _____________________________________________________________________________
TEST(LocalVocabEntry, comparisonWithPlainLiteralOrIri) {
  using LiteralOrIri = ad_utility::triple_component::LiteralOrIri;
  Index index = ad_utility::testing::makeTestIndex(gtestCurrentTestName(),
                                                   "<a> <b> <c> .");
  const auto& ctx = index.getLocalVocabContext();

  LocalVocabEntry entry = LocalVocabEntry::fromIriref("<x>", ctx);
  LocalVocabEntry sameEntry = LocalVocabEntry::fromIriref("<x>", ctx);
  LocalVocabEntry otherEntry = LocalVocabEntry::fromIriref("<y>", ctx);
  LiteralOrIri same = LiteralOrIri::iriref("<x>");
  LiteralOrIri other = LiteralOrIri::iriref("<y>");

  // Comparison against a plain `LiteralOrIri` (the base class).
  EXPECT_TRUE(entry == same);
  EXPECT_FALSE(entry != same);
  EXPECT_FALSE(entry == other);
  EXPECT_TRUE(entry != other);

  // Comparison against another `LocalVocabEntry`.
  EXPECT_TRUE(entry == sameEntry);
  EXPECT_FALSE(entry != sameEntry);
  EXPECT_FALSE(entry == otherEntry);
  EXPECT_TRUE(entry != otherEntry);
}

// __________________________________________________________________________
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
