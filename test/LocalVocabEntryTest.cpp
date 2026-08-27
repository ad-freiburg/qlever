// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

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
