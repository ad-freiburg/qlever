//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "VocabularyTestHelpers.h"
#include "index/vocabulary/StringSortComparator.h"
#include "index/vocabulary/UnicodeVocabulary.h"
#include "index/vocabulary/VocabularyInMemory.h"

using Vocab = UnicodeVocabulary<VocabularyInMemory, SimpleStringComparator>;
using namespace vocabulary_test;

auto createVocabulary(const std::vector<std::string>& words) {
  SimpleStringComparator comparator{"en", "us", false};
  Vocab v{comparator};
  VocabularyInMemory::Words w;
  w.build(words);
  return Vocab(comparator, std::move(w));
}

using Level = SimpleStringComparator::Level;
TEST(UnicodeVocabulary, LowercaseAscii) {
  const std::vector<std::string> words{"alpha", "beta",    "camma",
                                       "delta", "epsilon", "frikadelle"};
  std::vector<Level> levels{Level::PRIMARY,   Level::SECONDARY,
                            Level::TERTIARY,  Level::QUARTERNARY,
                            Level::IDENTICAL, Level::TOTAL};

  for (const auto level : levels) {
    auto makeWordSmaller = [](std::string word) {
      word.back()--;
      return word;
    };
    auto makeWordLarger = [](std::string word) {
      word.back()++;
      return word;
    };

    testUpperAndLowerBoundContiguousIDs(createVocabulary(words), makeWordLarger,
                                        makeWordSmaller, level, words);
  }
}

TEST(UnicodeVocabulary, UpperAndLowercase) {
  const std::vector<std::string> words{"alpha", "ALPHA", "beta", "BETA"};

  // On the `PRIMARY` and `SECONDARY` Level, uppercase letters are equal to
  // their lowercase equivalents, so we cannot use these levels here.
  std::vector<Level> levels{Level::TERTIARY, Level::QUARTERNARY,
                            Level::IDENTICAL, Level::TOTAL};

  for (const auto level : levels) {
    auto makeWordSmaller = [](std::string word) {
      if (word.back() == 'A') {
        word.back() = 'a';
      } else {
        word.back()--;
      }
      return word;
    };

    auto makeWordLarger = [](std::string word) {
      if (word.back() == 'a') {
        word.back() = 'A';
      } else {
        word.back()++;
      }
      return word;
    };

    testUpperAndLowerBoundContiguousIDs(createVocabulary(words), makeWordLarger,
                                        makeWordSmaller, level, words);
  }
}

TEST(UnicodeVocabulary, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
}

TEST(UnicodeVocabulary, EmptyVocabulary) {
  testEmptyVocabularyWithComparator(createVocabulary, Level::PRIMARY);
  testEmptyVocabularyWithComparator(createVocabulary, Level::TOTAL);
}

// _____________________________________________________________________________
TEST(UnicodeVocabulary, ScanAll) {
  // `scanAll` must yield all words in order (it simply delegates to the
  // underlying vocabulary).
  const std::vector<std::string> words{"alpha", "beta", "gamma", "delta"};
  auto vocab = createVocabulary(words);
  EXPECT_THAT(scanAllToVector(vocab.scanAll()),
              ::testing::ElementsAreArray(words));
}

// _____________________________________________________________________________
TEST(UnicodeVocabulary, ScanAllEmptyVocabulary) {
  auto vocab = createVocabulary({});
  EXPECT_TRUE(scanAllToVector(vocab.scanAll()).empty());
}

// _____________________________________________________________________________
// `VocabularyInMemory` has no `lookupBatch` overload that decodes into a
// builder, so `lookupBatch` with a builder must take the fallback path that
// copies the owning result into the builder.
TEST(UnicodeVocabulary, LookupBatchWithoutBuilderSupport) {
  static_assert(!SupportsBuilderLookupBatch<VocabularyInMemory>);
  const std::vector<std::string> words{"alpha", "beta", "delta", "gamma"};
  auto vocab = createVocabulary(words);
  const std::array<size_t, 4> indices{3, 0, 2, 1};
  ArenaVocabBatchBuilder builder(indices.size());
  vocab.lookupBatch(indices, builder);
  const auto result = std::move(builder).finalize();
  assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
}
