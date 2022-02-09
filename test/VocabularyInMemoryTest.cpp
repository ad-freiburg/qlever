//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/VocabularyInMemory.h"
#include "./VocabularyTestHelpers.h"
using Vocab = VocabularyInMemory;

// This operator provides human-readable output for a `WordAndIndex`.
std::ostream& operator<<(std::ostream& o, const Vocab::WordAndIndex& w) {
  o << w._index << ", ";
  o << (w._word.value_or("nullopt"));
  return o;
}

namespace {

using namespace vocabulary_test;

using WordAndIndex = VocabularyInMemory::WordAndIndex;

auto createVocabulary(const std::vector<std::string>& words) {
  Vocab::Words w;
  w.build(words);
  return Vocab(std::move(w));
}

TEST(VocabularyInMemory, UpperLowerBound) {
  const std::vector<string> words{"alpha", "beta",    "camma",
                                  "delta", "epsilon", "frikadelle"};
  auto comparator = std::less<>{};
  auto makeWordSmaller = [](std::string word) {
    word.back()--;
    return word;
  };
  auto makeWordLarger = [](std::string word) {
    word.back()++;
    return word;
  };

  testUpperAndLowerBound(createVocabulary, makeWordLarger, makeWordSmaller,
                         comparator, words);
}

TEST(VocabularyInMemory, UpperLowerBoundAlternativeComparator) {
  const std::vector<string> words{"4", "33", "222", "1111"};
  auto comparator = [](const auto& a, const auto& b) {
    return std::stoi(std::string{a}) < std::stoi(std::string{b});
  };
  auto makeWordSmaller = [](std::string word) {
    return std::to_string(std::stoi(word) - 1);
  };
  auto makeWordLarger = [](std::string word) {
    return std::to_string(std::stoi(word) + 1);
  };

  testUpperAndLowerBound(createVocabulary, makeWordLarger, makeWordSmaller,
                         comparator, words);
}

TEST(VocabularyInMemory, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
}

TEST(VocabularyInMemory, ReadAndWriteFromFile) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};
  const auto vocab = createVocabulary(words);
  const std::string vocabularyFilename = "testvocab.dat";
  vocab.writeToFile(vocabularyFilename);

  Vocab readVocab;
  readVocab.readFromFile(vocabularyFilename);
  assertThatRangesAreEqual(vocab, readVocab);
  ad_utility::deleteFile(vocabularyFilename);
}
}  // namespace
