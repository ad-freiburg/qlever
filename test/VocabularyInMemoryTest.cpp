//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/VocabularyInMemory.h"
#include "./VocabularyTestHelpers.h"
using Vocab = VocabularyInMemory;

namespace {

using namespace vocabulary_test;

auto createVocabulary(const std::vector<std::string>& words) {
  Vocab::Words w;
  w.build(words);
  return Vocab(std::move(w));
}

TEST(VocabularyInMemory, UpperLowerBound) {
  testUpperAndLowerBoundWithStdLess(createVocabulary);
}

TEST(VocabularyInMemory, UpperLowerBoundAlternativeComparator) {
  testUpperAndLowerBoundWithNumericComparator(createVocabulary);
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
  readVocab.open(vocabularyFilename);
  assertThatRangesAreEqual(vocab, readVocab);
  ad_utility::deleteFile(vocabularyFilename);
}

TEST(VocabularyInMemory, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary);
}
}  // namespace
