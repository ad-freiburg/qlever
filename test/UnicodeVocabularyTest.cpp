//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/StringSortComparator.h"
#include "../src/index/vocabulary/UnicodeVocabulary.h"
#include "../src/index/vocabulary/VocabularyInMemory.h"
#include "./VocabularyTestHelpers.h"

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
  const std::vector<string> words{"alpha", "beta",    "camma",
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
  const std::vector<string> words{"alpha", "ALPHA", "beta", "BETA"};

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
