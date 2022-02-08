//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/CombinedVocabulary.h"
#include "../src/index/vocabulary/VocabularyInMemory.h"
#include "./VocabularyTestHelpers.h"

using namespace vocabulary_test;

// TODO<joka921> Also test a scenario, where the first Ids have to be modified.

// This class fulfills the `IndexConverterConcept` for any combined vocabulary.
// It refers to the following situation: The private indices in both
// underlying vocabularies are [0 .. n) and [0 .. m), the words in the
// first vocabulary all stand before the words in the second vocabulary.
// For ids in the second vocabulary we thus have to add/subtract `n` (the size)
// of the first vocabulary, to transform private to public indices and vice
// versa.
struct FirstAThenB {
  static bool isInFirst(uint64_t index, const auto& vocab) {
    return index < vocab.sizeFirstVocab();
  }

  static uint64_t fromFirst(uint64_t index, const auto&) { return index; }

  static uint64_t toFirst(uint64_t index, const auto&) { return index; }
  static uint64_t fromSecond(uint64_t index, const auto& vocab) {
    return index + vocab.sizeFirstVocab();
  }
  static uint64_t toSecond(uint64_t index, const auto& vocab) {
    return index - vocab.sizeFirstVocab();
  }
};

auto createVocabularyInMemory(const std::vector<std::string>& words) {
  VocabularyInMemory::Words w;
  w.build(words);
  return VocabularyInMemory(std::move(w));
}

/// The first half of the words go to the first vocabulary, the second
/// half of the words go to the second vocabulary.
auto createCombinedVocabulary(const std::vector<std::string>& words) {
  std::vector<std::string> a, b;
  for (size_t i = 0; i < words.size(); ++i) {
    if (i < words.size() / 2) {
      a.push_back(words[i]);
    } else {
      b.push_back(words[i]);
    }
  }

  return CombinedVocabulary{createVocabularyInMemory(a),
                            createVocabularyInMemory(b), FirstAThenB{}};
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

  testUpperAndLowerBound(createCombinedVocabulary, makeWordLarger,
                         makeWordSmaller, comparator, words);
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

  testUpperAndLowerBound(createCombinedVocabulary, makeWordLarger,
                         makeWordSmaller, comparator, words);
}

TEST(VocabularyInMemory, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createCombinedVocabulary);
}
