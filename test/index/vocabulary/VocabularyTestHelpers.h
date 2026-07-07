//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_VOCABULARYTESTHELPERS_H
#define QLEVER_VOCABULARYTESTHELPERS_H

#include <gmock/gmock.h>

#include <array>

#include "../../util/GTestHelpers.h"
#include "backports/span.h"
#include "index/vocabulary/VocabularyTypes.h"
#include "util/Exception.h"

// human-readable output for the `WordAndIndex` class within GTest.
inline void PrintTo(const WordAndIndex& wi, std::ostream* osPtr) {
  auto& os = *osPtr;
  os << "WordAndIndex :";
  if (wi.isEnd()) {
    os << "[end]";
  } else {
    os << '"' << wi.word() << '"';
    os << wi.index();
  }
}

namespace vocabulary_test {

// Can be used to compare arbitrary vocabularies to each other and to
// `std::vector<string>`.
inline auto assertThatRangesAreEqual = [](const auto& a, const auto& b) {
  ASSERT_EQ(a.size(), b.size());
  for (size_t i = 0; i < a.size(); ++i) {
    ASSERT_EQ(a[i], b[i]);
  }
};

// A matcher for the `WordAndIndex` class. It currently ignores the
// `previousIndex_` member, which is mostly redundant.
constexpr auto matchWordAndIndex =
    [](const WordAndIndex& wi) -> ::testing::Matcher<const WordAndIndex&> {
  auto isEndMatcher =
      AD_PROPERTY(WordAndIndex, isEnd, ::testing::Eq(wi.isEnd()));
  if (wi.isEnd()) {
    return isEndMatcher;
  }
  return ::testing::AllOf(
      isEndMatcher, AD_PROPERTY(WordAndIndex, word, ::testing::Eq(wi.word())),
      AD_PROPERTY(WordAndIndex, index, ::testing::Eq(wi.index())));
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
template <typename Vocab, typename MakeLarger, typename MakeSmaller,
          typename Comparator, typename Words>
inline void testUpperAndLowerBound(const Vocab& vocab,
                                   MakeLarger makeWordLarger,
                                   MakeSmaller makeWordSmaller,
                                   Comparator comparator, const Words& words,
                                   std::vector<uint64_t> ids) {
  ASSERT_FALSE(words.empty());
  ASSERT_EQ(vocab.size(), words.size());

  for (size_t i = 0; i < vocab.size(); ++i) {
    WordAndIndex wi{words[i], ids[i]};
    EXPECT_THAT(vocab.lower_bound(words[i], comparator), matchWordAndIndex(wi));
    auto lexicographicallySmallerWord = makeWordSmaller(words[i]);
    auto res = vocab.lower_bound(lexicographicallySmallerWord, comparator);
    EXPECT_THAT(vocab.lower_bound(lexicographicallySmallerWord, comparator),
                matchWordAndIndex(wi));
  }

  {
    auto wi = WordAndIndex::end();
    EXPECT_THAT(vocab.lower_bound(makeWordLarger(words.back()), comparator),
                matchWordAndIndex(wi));
  }

  for (size_t i = 1; i < vocab.size(); ++i) {
    WordAndIndex wi{words[i], ids[i]};
    EXPECT_THAT(vocab.upper_bound(words[i - 1], comparator),
                matchWordAndIndex(wi));
    auto lexicographicallyLargerWord = makeWordLarger(words[i - 1]);
    EXPECT_THAT(vocab.upper_bound(lexicographicallyLargerWord, comparator),
                matchWordAndIndex(wi));
  }

  {
    WordAndIndex wi{words.front(), ids[0]};
    ASSERT_THAT(vocab.upper_bound(makeWordSmaller(words.front()), comparator),
                matchWordAndIndex(wi));
  }

  {
    auto wi = WordAndIndex::end();
    ASSERT_THAT(vocab.upper_bound(words.back(), comparator),
                matchWordAndIndex(wi));
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
template <typename Vocab, typename MakeLarger, typename MakeSmaller,
          typename Comparator, typename Words>
void testUpperAndLowerBoundContiguousIDs(const Vocab& vocab,
                                         MakeLarger makeWordLarger,
                                         MakeSmaller makeWordSmaller,
                                         Comparator comparator,
                                         const Words& words) {
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < words.size(); ++i) {
    ids.push_back(i);
  }

  testUpperAndLowerBound(vocab, makeWordLarger, makeWordSmaller, comparator,
                         words, ids);
}

// Same as the previous function, but explicitly state, which IDs are expected
// in the vocabulary.
template <typename Vocab, typename Words, typename Ids>
void testUpperAndLowerBoundWithStdLessFromWordsAndIds(Vocab vocabulary,
                                                      const Words& words,
                                                      const Ids& ids) {
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
template <typename F>
void testUpperAndLowerBoundWithStdLess(F createVocabulary) {
  const std::vector<std::string> words{"alpha", "beta",    "camma",
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
template <typename Vocab, typename Words, typename Ids>
void testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
    Vocab vocabulary, const Words& words, const Ids& ids) {
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
template <typename F>
void testUpperAndLowerBoundWithNumericComparator(F createVocabulary) {
  const std::vector<std::string> words{"4", "33", "222", "1111"};
  std::vector<uint64_t> ids;
  for (size_t i = 0; i < words.size(); ++i) {
    ids.push_back(i);
  }

  testUpperAndLowerBoundWithNumericComparatorFromWordsAndIds(
      createVocabulary(words), words, ids);
}

// Check that the `operator[]` works as expected for an unordered vocabulary.
// Checks that vocabulary[ids[i]] == words[i].
template <typename Vocab, typename Words, typename Ids>
auto testAccessOperatorFromWordsAndIds(Vocab vocabulary, const Words& words,
                                       const Ids& ids) {
  // Not in any particularly order.
  AD_CONTRACT_CHECK(words.size() == ids.size());
  ASSERT_EQ(words.size(), vocabulary.size());
  for (size_t i = 0; i < words.size(); ++i) {
    ASSERT_EQ(words[i], vocabulary[ids[i]]);
  }
}
// Check that the `operator[]` works as expected for an unordered vocabulary,
// created via `createVocabulary(std::vector<std::string>)`.
template <typename F>
auto testAccessOperatorForUnorderedVocabulary(F createVocabulary) {
  // Not in any particularly order.
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
template <typename F, typename C>
auto testEmptyVocabularyWithComparator(F createVocabulary, C comparator) {
  auto vocab = createVocabulary(std::vector<std::string>{});
  ASSERT_EQ(0u, vocab.size());
  auto expected = WordAndIndex::end();
  EXPECT_THAT(vocab.lower_bound("someWord", comparator),
              matchWordAndIndex(expected));
  EXPECT_THAT(vocab.upper_bound("someWord", comparator),
              matchWordAndIndex(expected));
}

// Check that an empty vocabulary, created via
// `createVocabulary(std::vector<std::string>{})` works as expected.
template <typename F>
auto testEmptyVocabulary(F createVocabulary) {
  testEmptyVocabularyWithComparator(createVocabulary, std::less<>{});
  testEmptyVocabularyWithComparator(createVocabulary, std::greater<>{});
}

// The default set of words written by `writeWordsAndFinish`.
// Sorted lexicographically, since the underlying vocabularies require sorted
// input at write time.
inline constexpr std::array<std::string_view, 4> defaultTestWords{
    "alpha", "beta", "delta", "gamma"};

// Feed `words` into an already-constructed word `writer` and `finish()` it. The
// `words` must be sorted.
template <typename Writer>
void writeWordsAndFinish(
    Writer& writer, ql::span<const std::string_view> words = defaultTestWords) {
  for (const auto& word : words) {
    writer(word, false);
  }
  writer.finish();
}

// Assert that the vocabulary contains `expectedWords[i]` at vocabulary index
// `indices[i]`, for all positions `i`.
template <typename Vocab, typename Indices>
void assertVocabularyMatchesAtIndices(
    const Vocab& vocab, const Indices& indices,
    std::initializer_list<std::string_view> expectedWords) {
  ASSERT_EQ(ql::ranges::distance(indices), expectedWords.size());

  for (const auto& [idx, expectedWord] :
       ::ranges::views::zip(indices, expectedWords)) {
    EXPECT_EQ(std::string{vocab[idx]}, expectedWord)
        << "at vocabulary index " << idx;
  }
}

// Assert that the vocabulary contains `expectedWords[i]` at vocabulary index
// `i`, for all indices `i` in `[0, expectedWords.size())`.
template <typename Vocab>
void assertVocabularyMatchesContiguousIndices(
    const Vocab& vocab, std::initializer_list<std::string_view> expectedWords) {
  assertVocabularyMatchesAtIndices(
      vocab, ql::views::iota(size_t{0}, expectedWords.size()), expectedWords);
}

// Assert that `lookupResult[i]` equals `vocab[indices[i]]`, for all positions
// `i`.
template <typename Vocab, typename Indices>
void assertLookupResultMatchesVocabularyAtIndices(
    const Vocab& vocab, const VocabBatchLookupResult& lookupResult,
    const Indices& indices) {
  ASSERT_EQ(lookupResult->size(), ql::ranges::distance(indices));

  auto at = [&](size_t i) -> decltype(auto) {
    if constexpr (requires { vocab[i]; }) {
      return vocab[i];
    } else {
      using IndexType = typename Vocab::IndexType;
      return vocab[IndexType::make(i)];
    }
  };

  for (const auto& [resultWord, idx] :
       ::ranges::views::zip(*lookupResult, indices)) {
    EXPECT_EQ(resultWord, at(idx)) << " at  vocabulary index " << idx;
  }
}

// Assert that every streamed lookup result equals the individual `vocab[]`
// lookups for the corresponding batch in `expectedBatches`, and that the
// batches are yielded in order.
template <typename Vocab, typename ExpectedBatches>
void assertStreamedLookupMatchesVocabularyAtIndices(
    const Vocab& vocab, VocabLookupOutput& streamedResults,
    const ExpectedBatches& expectedBatches) {
  auto results = ::ranges::to_vector(streamedResults);
  ASSERT_EQ(results.size(), expectedBatches.size());

  for (const auto& [result, indices] :
       ::ranges::views::zip(results, expectedBatches)) {
    assertLookupResultMatchesVocabularyAtIndices(vocab, result, indices);
  }
}

}  // namespace vocabulary_test

#endif  // QLEVER_VOCABULARYTESTHELPERS_H
