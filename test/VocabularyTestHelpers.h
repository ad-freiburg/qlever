//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VOCABULARYTESTHELPERS_H
#define QLEVER_VOCABULARYTESTHELPERS_H

#include <gtest/gtest.h>

namespace vocabulary_test {

// Can be used to compare arbitrary vocabularies to each other and to
// `std::vector<string>`.
inline auto assertThatRangesAreEqual = [](const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
};

/**
 *
 * @brief Assert that `upper_bound` and `lower_bound` work as expected for a
 *        given vocabulary.
 * @param vocab The vocabulary that is tested.
 * @param makeWordLarger Function that takes a `std::string` from the
 *        vocabulary and returns a `std::string` that is larger than the
 *        input, but smaller than the next larger word in the vocabulary.
 * @param makeWordSmaller The complement of `makeWordLarger`
 * @param comparator The second argument that is passed to the corresponding
 *        `upper_bound` and `lower_bound` functions.
 * @param words The vocabulary is expected to have the same contents as `words`.
 * @param ids Must have the same size as `words` The tests expect that
 * `vocab[ids[i]] == words[i]` for all i.
 */
inline void testUpperAndLowerBound(const auto& vocab, auto makeWordLarger,
                                   auto makeWordSmaller, auto comparator,
                                   const auto& words,
                                   std::vector<uint64_t> ids) {
  ASSERT_FALSE(words.empty());
  ASSERT_EQ(vocab.size(), words.size());

  auto maxId = vocab.getHighestId();

  for (size_t i = 0; i < vocab.size(); ++i) {
    WordAndIndex wi{words[i], ids[i]};
    EXPECT_EQ(vocab.lower_bound(words[i], comparator), wi);
    auto lexicographicallySmallerWord = makeWordSmaller(words[i]);
    EXPECT_EQ(vocab.lower_bound(lexicographicallySmallerWord, comparator), wi);
  }

  {
    WordAndIndex wi{std::nullopt, maxId + 1};
    ASSERT_EQ(vocab.lower_bound(makeWordLarger(words.back()), comparator), wi);
  }

  for (size_t i = 1; i < vocab.size(); ++i) {
    WordAndIndex wi{words[i], ids[i]};
    EXPECT_EQ(vocab.upper_bound(words[i - 1], comparator), wi);
    auto lexicographicallyLargerWord = makeWordLarger(words[i - 1]);
    EXPECT_EQ(vocab.upper_bound(lexicographicallyLargerWord, comparator), wi);
  }

  {
    WordAndIndex wi{words.front(), ids[0]};
    ASSERT_EQ(vocab.upper_bound(makeWordSmaller(words.front()), comparator),
              wi);
  }

  {
    WordAndIndex wi{std::nullopt, maxId + 1};
    ASSERT_EQ(vocab.upper_bound(words.back(), comparator), wi);
  }
};

/**
 *
 * @brief Assert that `upper_bound` and `lower_bound` work as expected for a
 *        given vocabulary. Assume that the IDs in the vocabulary are contiguous
 * and start at 0.
 * @param vocab The vocabulary that will be tested.
 * @param makeWordLarger Function that takes a `std::string` from the
 *        vocabulary and returns a `std::string` that is larger than the
 *        input, but smaller than the next larger word in the vocabulary.
 * @param makeWordSmaller The complement of `makeWordLarger`
 * @param comparator The second argument that is passed to the corresponding
 *        `upper_bound` and `lower_bound` functions.
 * @param words The vocabulary is expected to have the same contents as `words`.
 */
void testUpperAndLowerBoundContiguousIDs(const auto& vocab, auto makeWordLarger,
                                         auto makeWordSmaller, auto comparator,
                                         const auto& words) {
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < words.size(); ++i) {
    ids.push_back(i);
  }

  testUpperAndLowerBound(vocab, makeWordLarger, makeWordSmaller, comparator,
                         words, ids);
}

// Same as the previous function, but explictly state, which IDs are expected in
// the vocabulary.
void testUpperAndLowerBoundWithStdLessFromWordsAndIds(auto vocabulary,
                                                      const auto& words,
                                                      const auto& ids) {
  auto comparator = std::less<>{};
  auto makeWordSmaller = [](std::string word) {
    word.back()--;
    return word;
  };
  auto makeWordLarger = [](std::string word) {
    word.back()++;
    return word;
  };

  testUpperAndLowerBound(vocabulary, makeWordLarger, makeWordSmaller,
                         comparator, words, ids);
}

/**
 * @brief Assert that `upper_bound` and `lower_bound` work as expected for a
 *        given vocabulary when using words that are sorted by `std::less`.
 * @param createVocabulary Function that takes a `std::vector<string>` and
 *           returns a vocabulary.
 */
void testUpperAndLowerBoundWithStdLess(auto createVocabulary) {
  const std::vector<string> words{"alpha", "beta",    "camma",
                                  "delta", "epsilon", "frikadelle"};

  std::vector<uint64_t> ids;
  for (size_t i = 0; i < words.size(); ++i) {
    ids.push_back(i);
  }

  testUpperAndLowerBoundWithStdLessFromWordsAndIds(createVocabulary(words),
                                                   words, ids);
}

/**
 * @brief Assert that `upper_bound` and `lower_bound` work as expected for a
 *        given vocabulary when using words that are sorted by `std::less`.
 * @param createVocabulary Function that takes a `std::vector<string>` and
 *           returns a vocabulary.
 */
void testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
    auto vocabulary, const auto& words, const auto& ids) {
  auto comparator = [](const auto& a, const auto& b) {
    return std::stoi(std::string{a}) < std::stoi(std::string{b});
  };
  auto makeWordSmaller = [](std::string word) {
    return std::to_string(std::stoi(word) - 1);
  };
  auto makeWordLarger = [](std::string word) {
    return std::to_string(std::stoi(word) + 1);
  };

  testUpperAndLowerBound(vocabulary, makeWordLarger, makeWordSmaller,
                         comparator, words, ids);
}
/**
 * @brief Assert that `upper_bound` and `lower_bound` work as expected for a
 *        given vocabulary when using numeric strings with the numeric
 *        ordering ("4" < "11" b.c. 4 < 11).
 * @param createVocabulary Function that takes a `std::vector<string>` and
 *        returns a vocabulary.
 */
void testUpperAndLowerBoundWithNumericComparator(auto createVocabulary) {
  const std::vector<string> words{"4", "33", "222", "1111"};
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < words.size(); ++i) {
    ids.push_back(i);
  }

  testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
      createVocabulary(words), words, ids);
}

// Check that the `operator[]` works as expected for an unordered vocabulary.
// Checks that vocabulary[ids[i]] == words[i].
auto testAccessOperatorFromWordsAndIds(auto vocabulary, const auto& words,
                                       const auto& ids) {
  // Not in any particulary order.
  AD_CONTRACT_CHECK(words.size() == ids.size());
  ASSERT_EQ(words.size(), vocabulary.size());
  for (size_t i = 0; i < words.size(); ++i) {
    ASSERT_EQ(words[i], vocabulary[ids[i]]);
  }
}
// Check that the `operator[]` works as expected for an unordered vocabulary,
// created via `createVocabulary(std::vector<std::string>)`.
auto testAccessOperatorForUnorderedVocabulary(auto createVocabulary) {
  // Not in any particulary order.
  const std::vector<std::string> words{"alpha", "delta", "ALPHA", "beta", "42",
                                       "31",    "0a",    "a0",    "al"};
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < words.size(); ++i) {
    ids.push_back(i);
  }
  testAccessOperatorFromWordsAndIds(createVocabulary(words), words, ids);
}

// Check that an empty vocabulary, created via
// `createVocabulary(std::vector<std::string>{})`, works as expected with the
// given comparator.
auto testEmptyVocabularyWithComparator(auto createVocabulary, auto comparator) {
  auto vocab = createVocabulary(std::vector<std::string>{});
  ASSERT_EQ(0u, vocab.size());
  WordAndIndex expected{std::nullopt, 0};
  ASSERT_EQ(expected, vocab.lower_bound("someWord", comparator));
  ASSERT_EQ(expected, vocab.upper_bound("someWord", comparator));
}

// Check that an empty vocabulary, created via
// `createVocabulary(std::vector<std::string>{})` works as expected.
auto testEmptyVocabulary(auto createVocabulary) {
  testEmptyVocabularyWithComparator(createVocabulary, std::less<>{});
  testEmptyVocabularyWithComparator(createVocabulary, std::greater<>{});
}

}  // namespace vocabulary_test

#endif  // QLEVER_VOCABULARYTESTHELPERS_H
