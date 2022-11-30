//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/CompressedVocabulary.h"
#include "../src/index/vocabulary/PrefixCompressor.h"
#include "../src/index/vocabulary/VocabularyInMemory.h"
#include "./VocabularyTestHelpers.h"

// A stateless "compressor" that applies a trivial transformation to a string
struct DummyCompressor {
  static std::string compress(std::string_view uncompressed) {
    std::string result{uncompressed};
    for (auto& c : result) {
      c += 2;
    }
    return result;
  }

  static std::string decompress(std::string_view compressed) {
    std::string result{compressed};
    for (char& c : result) {
      c -= 2;
    }
    return result;
  }
};

using Vocab = CompressedVocabulary<VocabularyInMemory, DummyCompressor>;

namespace {

using namespace vocabulary_test;

auto createVocabulary(const std::vector<std::string>& words) {
  Vocab v;
  std::string vocabularyFilename = "vocab.test.dat";
  auto writer = v.makeDiskWriter(vocabularyFilename);
  for (const auto& word : words) {
    writer.push(word);
  }
  writer.finish();
  v.open(vocabularyFilename);
  ad_utility::deleteFile(vocabularyFilename);
  return v;
}

TEST(CompressedVocabulary, UpperLowerBound) {
  testUpperAndLowerBoundWithStdLess(createVocabulary);
}

TEST(CompressedVocabulary, UpperLowerBoundAlternativeComparator) {
  testUpperAndLowerBoundWithNumericComparator(createVocabulary);
}

TEST(CompressedVocabulary, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createVocabulary);
}

TEST(CompressedVocabulary, EmptyVocabulary) {
  testEmptyVocabulary(createVocabulary);
}

TEST(CompressedVocabulary, CompressionIsActuallyApplied) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  Vocab v;
  auto writer = v.makeDiskWriter("vocabtmp.txt");
  for (const auto& word : words) {
    writer.push(word);
  }
  writer.finish();

  VocabularyInMemory simple;
  simple.open("vocabtmp.txt");

  ASSERT_EQ(simple.size(), words.size());
  for (size_t i = 0; i < simple.size(); ++i) {
    ASSERT_NE(simple[i], words[i]);
    ASSERT_EQ(DummyCompressor::decompress(simple[i]), words[i]);
  }
}

}  // namespace