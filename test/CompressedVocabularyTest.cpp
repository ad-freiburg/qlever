//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/CompressedVocabulary.h"
#include "../src/index/vocabulary/PrefixCompressor.h"
#include "../src/index/vocabulary/VocabularyInMemory.h"
#include "./VocabularyTestHelpers.h"

// A stateless "compressor" that applies a trivial transormation to a string
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

// Human readable output for `WordAndIndex`.
std::ostream& operator<<(std::ostream& o, const Vocab::WordAndIndex& w) {
  o << w._index << ", ";
  o << w._word.value_or("nullopt");
  return o;
}

namespace {

using namespace vocabulary_test;

using WordAndIndex = Vocab::WordAndIndex;

auto createVocabulary(const std::vector<std::string>& words) {
  Vocab v;
  std::string vocabularyFilename = "vocab.test.dat";
  auto writer = v.makeDiskWriter(vocabularyFilename);
  for (const auto& word : words) {
    writer.push(word);
  }
  writer.finish();
  v.readFromFile(vocabularyFilename);
  ad_utility::deleteFile(vocabularyFilename);
  return v;
}

TEST(CompressedVocabulary, UpperLowerBound) {
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

TEST(CompressedVocabulary, UpperLowerBoundAlternativeComparator) {
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

TEST(UnicodeVocabulary, CompressionIsActuallyApplied) {
  const std::vector<std::string> words{"alpha", "delta", "beta", "42",
                                       "31",    "0",     "al"};

  Vocab v;
  auto writer = v.makeDiskWriter("vocabtmp.txt");
  for (const auto& word : words) {
    writer.push(word);
  }
  writer.finish();

  VocabularyInMemory simple;
  simple.readFromFile("vocabtmp.txt");

  ASSERT_EQ(simple.size(), words.size());
  for (size_t i = 0; i < simple.size(); ++i) {
    ASSERT_NE(simple[i], words[i]);
    ASSERT_EQ(DummyCompressor::decompress(simple[i]), words[i]);
  }
}

}  // namespace