//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VOCABULARYTESTHELPERS_H
#define QLEVER_VOCABULARYTESTHELPERS_H

#include <gtest/gtest.h>

namespace vocabulary_test {

// Can be used to compare arbitrary vocabularies to each other and to
// `std::vector<string>`.
auto assertThatRangesAreEqual = [](const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
};

/**
 *
 * @brief Assert that `upper_bound` and `lower_bound` work as expected for a
 *        given vocabulary.
 * @param vocabularyCreator Function that takes a `std::vector<string>` and
 *           returns a vocabulary.
 * @param makeWordLarger Function that takes a `std::string` from the
 *           vocabulary and returns a `std::string` that is larger than the
 *           input, but smaller than the next larger word in the vocabulary.
 * @param makeWordSmaller The complement of `makeWordLarger`
 * @param comparator The second argument that is passed to the corresponding
 *           `upper_bound` and `lower_bound` functions.
 * @param words The words that will become the vocabulary. They have to be
 *            sorted wrt `comparator`.
 */
void testUpperAndLowerBound(auto vocabularyCreator, auto makeWordLarger,
                            auto makeWordSmaller, auto comparator,
                            const auto& words) {
  ASSERT_FALSE(words.empty());
  const auto vocab = vocabularyCreator(words);
  ASSERT_EQ(vocab.size(), words.size());

  using WordAndIndex = typename decltype(vocab)::WordAndIndex;
  for (size_t i = 0; i < vocab.size(); ++i) {
    WordAndIndex wi{words[i], i};
    EXPECT_EQ(vocab.lower_bound(words[i], comparator), wi);
    auto lexicographicallySmallerWord = makeWordSmaller(words[i]);
    EXPECT_EQ(vocab.lower_bound(lexicographicallySmallerWord, comparator), wi);
  }

  {
    WordAndIndex wi{std::nullopt, words.size()};
    ASSERT_EQ(vocab.lower_bound(makeWordLarger(words.back()), comparator), wi);
  }

  for (size_t i = 1; i < vocab.size(); ++i) {
    WordAndIndex wi{words[i], i};
    EXPECT_EQ(vocab.upper_bound(words[i - 1], comparator), wi);
    auto lexicographicallyLargerWord = makeWordLarger(words[i - 1]);
    EXPECT_EQ(vocab.upper_bound(lexicographicallyLargerWord, comparator), wi);
  }

  {
    WordAndIndex wi{words.front(), 0};
    ASSERT_EQ(vocab.upper_bound(makeWordSmaller(words.front()), comparator),
              wi);
  }

  {
    WordAndIndex wi{std::nullopt, words.size()};
    ASSERT_EQ(vocab.upper_bound(words.back(), comparator), wi);
  }
};

// Check that the `operator[]` works as expected for an unordered vocabulary,
// created via `createVocabulary(std::vector<std::string>)`.
auto testAccessOperatorForUnorderedVocabulary(auto createVocabulary) {
  // Not in any particulary order.
  const std::vector<std::string> words{"alpha", "delta", "ALPHA", "beta", "42",
                                       "31",    "0a",    "a0",    "al"};
  const auto vocab = createVocabulary(words);
  assertThatRangesAreEqual(vocab, words);
}

}  // namespace vocabulary_test

#endif  // QLEVER_VOCABULARYTESTHELPERS_H
