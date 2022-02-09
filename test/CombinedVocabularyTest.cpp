//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "../src/index/vocabulary/CombinedVocabulary.h"
#include "../src/index/vocabulary/VocabularyInMemory.h"
#include "./VocabularyTestHelpers.h"

using namespace vocabulary_test;

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

  static uint64_t toGlobalFirst(uint64_t index, const auto&) { return index; }

  static uint64_t toLocalFirst(uint64_t index, const auto&) { return index; }
  static uint64_t toGlobalSecond(uint64_t index, const auto& vocab) {
    return index + vocab.sizeFirstVocab();
  }
  static uint64_t toLocalSecond(uint64_t index, const auto& vocab) {
    return index - vocab.sizeFirstVocab();
  }
};

// This class fulfills the `IndexConverterConcept` for any combined vocabulary.
// It refers to the following situation: The words with even global indices
// stand in the first vocabulary, and the words with odd global indices in the
// second vocabulary. Within each of the vocabularies, the local inidices are
// contiguous and start at 0.
struct EvenAndOdd {
  static bool isInFirst(uint64_t index, const auto&) {
    return (index % 2) == 0;
  }

  static uint64_t toGlobalFirst(uint64_t index, const auto&) {
    return 2 * index;
  }

  static uint64_t toLocalFirst(uint64_t index, const auto&) {
    return index / 2;
  }
  static uint64_t toGlobalSecond(uint64_t index, const auto&) {
    return 2 * index + 1;
  }
  static uint64_t toLocalSecond(uint64_t index, const auto&) {
    return index / 2;
  }
};

auto createVocabularyInMemory(const std::vector<std::string>& words) {
  VocabularyInMemory::Words w;
  w.build(words);
  return VocabularyInMemory(std::move(w));
}

/// The first half of the words go to the first vocabulary, the second
/// half of the words go to the second vocabulary.
auto createFirstAThenBVocabulary(const std::vector<std::string>& words) {
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

/// The words with even index go to the first vocabulary, the words with the odd
/// index go to the second vocabulary
auto createEvenOddVocabulary(const std::vector<std::string>& words) {
  std::vector<std::string> a, b;
  for (size_t i = 0; i < words.size(); ++i) {
    if (i % 2 == 0) {
      a.push_back(words[i]);
    } else {
      b.push_back(words[i]);
    }
  }

  return CombinedVocabulary{createVocabularyInMemory(a),
                            createVocabularyInMemory(b), EvenAndOdd{}};
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

  testUpperAndLowerBound(createFirstAThenBVocabulary, makeWordLarger,
                         makeWordSmaller, comparator, words);

  testUpperAndLowerBound(createEvenOddVocabulary, makeWordLarger,
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

  testUpperAndLowerBound(createFirstAThenBVocabulary, makeWordLarger,
                         makeWordSmaller, comparator, words);
}

TEST(VocabularyInMemory, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createFirstAThenBVocabulary);
}
