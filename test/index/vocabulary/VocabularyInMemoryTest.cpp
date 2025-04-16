//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "./VocabularyTestHelpers.h"
#include "backports/algorithm.h"
#include "index/vocabulary/VocabularyInMemory.h"
using Vocab = VocabularyInMemory;

namespace {

using namespace vocabulary_test;

auto createVocabulary(const std::vector<std::string>& words) {
  auto filename = "vocabInMemoryCreation.tmp";
  {
    Vocab v;
    auto writer = v.makeDiskWriter(filename);
    for (const auto& [i, word] : ::ranges::views::enumerate(words)) {
      auto idx = writer(word);
      EXPECT_EQ(idx, static_cast<uint64_t>(i));
    }
    writer.readableName() = "blubb";
    EXPECT_EQ(writer.readableName(), "blubb");
  }
  Vocab v;
  v.open(filename);
  return v;
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
