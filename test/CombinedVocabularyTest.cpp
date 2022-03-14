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
struct LeftAndRight {
  static bool isInFirst(uint64_t index, const auto& vocab) {
    return index < vocab.sizeFirstVocab();
  }

  static uint64_t localFirstToGlobal(uint64_t index, const auto&) {
    return index;
  }

  static uint64_t globalToLocalFirst(uint64_t index, const auto&) {
    return index;
  }
  static uint64_t localSecondToGlobal(uint64_t index, const auto& vocab) {
    return index + vocab.sizeFirstVocab();
  }
  static uint64_t globalToLocalSecond(uint64_t index, const auto& vocab) {
    return index - vocab.sizeFirstVocab();
  }
};

// This class fulfills the `IndexConverterConcept` for any combined vocabulary.
// It refers to the following situation: The words with even global indices are
// in the first vocabulary, and the words with odd global indices are in the
// second vocabulary. Within each of the vocabularies, the local inidices are
// contiguous and start at 0.
struct EvenAndOdd {
  static bool isInFirst(uint64_t index, const auto&) {
    return (index % 2) == 0;
  }

  static uint64_t localFirstToGlobal(uint64_t index, const auto&) {
    return 2 * index;
  }

  static uint64_t globalToLocalFirst(uint64_t index, const auto&) {
    return index / 2;
  }
  static uint64_t localSecondToGlobal(uint64_t index, const auto&) {
    return 2 * index + 1;
  }
  static uint64_t globalToLocalSecond(uint64_t index, const auto&) {
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
auto createLeftAndRightVocabulary(const std::vector<std::string>& words) {
  std::vector<std::string> left, right;
  for (size_t i = 0; i < words.size(); ++i) {
    if (i < words.size() / 2) {
      left.push_back(words[i]);
    } else {
      right.push_back(words[i]);
    }
  }

  return CombinedVocabulary{createVocabularyInMemory(left),
                            createVocabularyInMemory(right), LeftAndRight{}};
}

/// The words with even index go to the first vocabulary, the words with the odd
/// index go to the second vocabulary
auto createEvenOddVocabulary(const std::vector<std::string>& words) {
  std::vector<std::string> even, odd;
  for (size_t i = 0; i < words.size(); ++i) {
    if (i % 2 == 0) {
      even.push_back(words[i]);
    } else {
      odd.push_back(words[i]);
    }
  }

  return CombinedVocabulary{createVocabularyInMemory(even),
                            createVocabularyInMemory(odd), EvenAndOdd{}};
}

TEST(CombinedVocabulary, UpperLowerBound) {
  testUpperAndLowerBoundWithStdLess(createLeftAndRightVocabulary);
  testUpperAndLowerBoundWithStdLess(createEvenOddVocabulary);
}

TEST(CombinedVocabulary, UpperLowerBoundAlternativeComparator) {
  testUpperAndLowerBoundWithNumericComparator(createLeftAndRightVocabulary);
  testUpperAndLowerBoundWithNumericComparator(createEvenOddVocabulary);
}

TEST(CombinedVocabulary, AccessOperator) {
  testAccessOperatorForUnorderedVocabulary(createLeftAndRightVocabulary);
}

TEST(CombinedVocabulary, EmptyVocabulary) {
  testEmptyVocabulary(createLeftAndRightVocabulary);
  testEmptyVocabulary(createEvenOddVocabulary);
}
