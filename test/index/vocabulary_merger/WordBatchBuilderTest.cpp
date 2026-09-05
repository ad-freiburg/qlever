// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_format.h>
#include <gmock/gmock.h>

#include <array>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary_merger/WordBatchBuilder.h"

using namespace ad_utility::vocabulary_merger;
using ad_utility::vocabulary_merger::detail::idMapEntryBatchSize;
using ad_utility::vocabulary_merger::detail::QueueWord;
using ad_utility::vocabulary_merger::detail::WordBatch;
using ad_utility::vocabulary_merger::detail::WordBatchBuilder;
using ::testing::Pair;

namespace {
// An index mapping of a batch, in the order `(partial vocabulary, index of the
// word within the batch, index of the word in the partial vocabulary)`.
using Mapping = std::array<uint64_t, 3>;

// A `WordComparator` that simply compares the words lexicographically.
constexpr auto lessThan = [](std::string_view a, std::string_view b) {
  return std::less<>{}(a, b);
};

// Create the `QueueWord` for the occurrence of `word` with the given
// `localIndex` in the partial vocabulary `partialFileId`.
QueueWord makeQueueWord(std::string word, bool isExternal, size_t partialFileId,
                        uint64_t localIndex) {
  return QueueWord{
      TripleComponentWithIndex{std::move(word), isExternal, localIndex},
      partialFileId};
}

// The distinct words of a `batch`, each with its `isExternal` flag.
std::vector<std::pair<std::string, bool>> wordsOf(const WordBatch& batch) {
  std::vector<std::pair<std::string, bool>> result;
  for (const auto& uniqueWord : batch.uniqueWords_) {
    result.emplace_back(uniqueWord.word_, uniqueWord.isExternal_);
  }
  return result;
}

// The valid index mappings of a `batch`.
std::vector<Mapping> mappingsOf(const WordBatch& batch) {
  std::vector<Mapping> result;
  const auto& localIdxMappings = batch.localIdxMappings_;
  for (size_t i = 0; i < localIdxMappings.numMappings_; ++i) {
    const auto& mapping = localIdxMappings.mappings_[i];
    result.push_back(Mapping{mapping.partialVocabularyIndex_,
                             mapping.indexOfWordInBatch_,
                             mapping.indexOfWordInPartialVocabulary_});
  }
  return result;
}

// The `i`-th of the words that are used to fill up a whole batch. They are
// zero-padded, such that their lexicographic order is the order of their
// indices.
std::string fillWord(size_t i) { return absl::StrFormat("\"word%08d\"", i); }
}  // namespace

// _____________________________________________________________________________
// The duplicates are eliminated, and each occurrence of a word yields one ID
// index mapping that refers to the distinct word it belongs to.
TEST(WordBatchBuilder, deduplicationAndMappings) {
  std::vector<WordBatch> batches;
  auto collect = [&batches](WordBatch batch) {
    batches.push_back(std::move(batch));
  };

  WordBatchBuilder builder;
  builder.addMergedWords(
      {makeQueueWord("\"a\"", false, 0, 0), makeQueueWord("\"b\"", false, 0, 1),
       makeQueueWord("\"b\"", false, 1, 0)},
      lessThan, collect);
  // A batch that is not full is only handed on by `finish()`.
  EXPECT_THAT(batches, ::testing::IsEmpty());

  builder.finish(collect);
  ASSERT_EQ(batches.size(), 1u);
  EXPECT_THAT(
      wordsOf(batches[0]),
      ::testing::ElementsAre(Pair("\"a\"", false), Pair("\"b\"", false)));
  EXPECT_THAT(mappingsOf(batches[0]),
              ::testing::ElementsAre(Mapping{0, 0, 0}, Mapping{0, 1, 1},
                                     Mapping{1, 1, 0}));
}

// _____________________________________________________________________________
// A builder to which no word was added hands on no batch at all.
TEST(WordBatchBuilder, noWords) {
  std::vector<WordBatch> batches;
  auto collect = [&batches](WordBatch batch) {
    batches.push_back(std::move(batch));
  };
  WordBatchBuilder builder;
  builder.finish(collect);
  EXPECT_THAT(batches, ::testing::IsEmpty());
}

// _____________________________________________________________________________
// If a word occurs with different values for `isExternal`, then the merged
// word is externalized, no matter in which order the occurrences arrive.
TEST(WordBatchBuilder, externalizationWithinOneBatch) {
  for (bool firstIsExternal : {false, true}) {
    std::vector<WordBatch> batches;
    auto collect = [&batches](WordBatch batch) {
      batches.push_back(std::move(batch));
    };
    WordBatchBuilder builder;
    builder.addMergedWords({makeQueueWord("\"a\"", firstIsExternal, 0, 0),
                            makeQueueWord("\"a\"", !firstIsExternal, 1, 0)},
                           lessThan, collect);
    builder.finish(collect);
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_THAT(wordsOf(batches[0]),
                ::testing::ElementsAre(Pair("\"a\"", true)));
  }
}

// _____________________________________________________________________________
// The same, but with the occurrences of the word split by a batch boundary.
// The last distinct word of a batch is held back exactly for this reason, so
// the second occurrence still has to change its `isExternal` flag, and both of
// its index mappings have to end up in the *second* batch.
TEST(WordBatchBuilder, externalizationAcrossBatchBoundary) {
  std::vector<WordBatch> batches;
  auto collect = [&batches](WordBatch batch) {
    batches.push_back(std::move(batch));
  };
  WordBatchBuilder builder;

  // Exactly enough distinct words to fill a whole batch, followed by the word
  // that will be split by the batch boundary.
  std::vector<QueueWord> buffer;
  for (size_t i = 0; i < idMapEntryBatchSize; ++i) {
    buffer.push_back(makeQueueWord(fillWord(i), false, 0, i));
  }
  buffer.push_back(makeQueueWord("\"zzz\"", false, 0, idMapEntryBatchSize));
  builder.addMergedWords(std::move(buffer), lessThan, collect);

  // The first batch was handed on. It contains exactly the words that can no
  // longer change, i.e. all but the last one.
  ASSERT_EQ(batches.size(), 1u);
  ASSERT_EQ(batches[0].uniqueWords_.size(), idMapEntryBatchSize);
  EXPECT_EQ(batches[0].uniqueWords_.back().word_,
            fillWord(idMapEntryBatchSize - 1));
  EXPECT_EQ(mappingsOf(batches[0]).size(), idMapEntryBatchSize);

  // Another occurrence of the held-back word, this time marked as external.
  builder.addMergedWords({makeQueueWord("\"zzz\"", true, 1, 0)}, lessThan,
                         collect);
  builder.finish(collect);
  ASSERT_EQ(batches.size(), 2u);

  // Destroy the merged words of the first batch, exactly as the pipeline does
  // once that batch has been written. The held-back word must survive this,
  // because the second batch owns its own copy of it.
  batches[0] = WordBatch{};

  // The word was externalized, although its first occurrence (which arrived
  // before the batch boundary) was not marked as external.
  EXPECT_THAT(wordsOf(batches[1]),
              ::testing::ElementsAre(Pair("\"zzz\"", true)));
  // Both occurrences are in the second batch and refer to its only word.
  EXPECT_THAT(mappingsOf(batches[1]),
              ::testing::ElementsAre(Mapping{0, 0, idMapEntryBatchSize},
                                     Mapping{1, 0, 0}));
}

// _____________________________________________________________________________
// Add enough distinct words for several batches and check the invariants that
// the pipeline relies on: every mapping of a batch refers to a word of that
// batch, the words of the batches concatenate to the input, and every
// occurrence yields exactly one mapping.
TEST(WordBatchBuilder, severalBatches) {
  static constexpr size_t numWords = 5 * idMapEntryBatchSize / 2;
  std::vector<WordBatch> batches;
  auto collect = [&batches](WordBatch batch) {
    batches.push_back(std::move(batch));
  };
  WordBatchBuilder builder;

  // Hand the words to the builder in small buffers, as the merging does.
  std::vector<QueueWord> buffer;
  for (size_t i = 0; i < numWords; ++i) {
    buffer.push_back(makeQueueWord(fillWord(i), false, 0, i));
    if (buffer.size() == 100) {
      builder.addMergedWords(std::move(buffer), lessThan, collect);
      buffer.clear();
    }
  }
  builder.addMergedWords(std::move(buffer), lessThan, collect);
  builder.finish(collect);
  EXPECT_GE(batches.size(), 3u);

  size_t numWordsSeen = 0;
  size_t numMappingsSeen = 0;
  for (const auto& batch : batches) {
    for (const auto& mapping : mappingsOf(batch)) {
      // The mapping refers to a word of its own batch, and to the word with
      // the matching local index.
      ASSERT_LT(mapping[1], batch.uniqueWords_.size());
      EXPECT_EQ(batch.uniqueWords_[mapping[1]].word_, fillWord(mapping[2]));
      ++numMappingsSeen;
    }
    for (const auto& uniqueWord : batch.uniqueWords_) {
      EXPECT_EQ(uniqueWord.word_, fillWord(numWordsSeen));
      ++numWordsSeen;
    }
  }
  EXPECT_EQ(numWordsSeen, numWords);
  EXPECT_EQ(numMappingsSeen, numWords);
}

// _____________________________________________________________________________
// Words that are not in the order of the comparator are detected (but only if
// the expensive checks are enabled).
TEST(WordBatchBuilder, violatedOrderIsDetected) {
  if constexpr (!ad_utility::areExpensiveChecksEnabled) {
    GTEST_SKIP();
  }
  std::vector<WordBatch> batches;
  auto collect = [&batches](WordBatch batch) {
    batches.push_back(std::move(batch));
  };
  WordBatchBuilder builder;
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      builder.addMergedWords({makeQueueWord("\"b\"", false, 0, 0),
                              makeQueueWord("\"a\"", false, 0, 1)},
                             lessThan, collect),
      ::testing::HasSubstr("vocabulary order violated"), ad_utility::Exception);
}
