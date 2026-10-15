// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/thread_pool.hpp>
#include <string>
#include <tuple>
#include <vector>

#include "./vocabulary_merger/VocabularyMergerTestHelpers.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/vocabulary_merger/PartialVocabularyFile.h"
#include "util/parallelBlockMerge/InMemoryBlockStorage.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
using ad_utility::vocabulary_merger::detail::MergeBlock;
using ad_utility::vocabulary_merger::detail::MergeWord;

namespace {
// The basename of the partial vocabulary files of the tests below.
const std::string partialVocabBasename = "partial-vocab-";

// The words of the partial vocabulary with the given index in the tests below:
// `numWords` words that are already sorted by their byte order, all distinct
// within the partial vocabulary, with the local indices `0, 1, ...`. The
// partial vocabularies overlap: the word with number `k` is contained in the
// partial vocabulary `p` iff `k % (p + 1) == 0`.
std::vector<std::string> wordsOfPartialVocabulary(size_t p, size_t numWords) {
  std::vector<std::string> words;
  for (size_t k = 0; k < numWords; ++k) {
    if (k % (p + 1) == 0) {
      words.push_back(absl::StrFormat("\"word%08d\"", k));
    }
  }
  return words;
}

// Write the `words` as the partial vocabulary file `filename`. Every third
// word is marked as external.
void writePartialVocabulary(const std::string& filename,
                            const std::vector<std::string>& words) {
  PartialVocabularyWriter writer{filename};
  for (size_t i = 0; i < words.size(); ++i) {
    writer(words[i], PartialVocabIndexWithExternalFlag{i, i % 3 == 2});
  }
  writer.finish();
}

// The words of a block as (word, isExternal, local index) tuples.
std::vector<std::tuple<std::string, bool, uint64_t>> tuplesOfBlock(
    const MergeBlock& block) {
  std::vector<std::tuple<std::string, bool, uint64_t>> result;
  for (const auto& word : block) {
    result.emplace_back(word.iriOrLiteral(), word.isExternal(), word.id());
  }
  return result;
}
}  // namespace

// The runs input over partial vocabulary files fulfills the input policy of
// the parallel block merge.
static_assert(
    ad_utility::parallelBlockMerge::InputConcept<PartialVocabularyRunsInput>);

// Test that the block index of a partial vocabulary file describes its blocks
// exactly: the blocks are non-empty, contiguous, together hold all the words
// in order, and their first and last words are the ones in the index. The
// words are long enough that a partial vocabulary spans several blocks.
TEST(PartialVocabularyFile, blockIndexAndBlocks) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 3);
  size_t numWords = 3 * PARTIAL_VOCAB_BLOCK_SIZE.getBytes() / 16;
  std::vector<std::vector<std::string>> words;
  for (size_t p = 0; p < 3; ++p) {
    words.push_back(wordsOfPartialVocabulary(p, numWords));
    writePartialVocabulary(filenames.wordsFiles_[p], words.back());
  }

  PartialVocabularyRunsInput input{partialVocabBasename, 3};
  ASSERT_EQ(input.numRuns(), 3u);
  // The statistics are the totals over all blocks of all partial vocabularies.
  PartialVocabularyRunsInput::Statistics expectedStatistics;
  for (size_t p = 0; p < 3; ++p) {
    expectedStatistics.numBlocks_ += input.numBlocks(p);
    for (size_t b = 0; b < input.numBlocks(p); ++b) {
      expectedStatistics.numWords_ += input.numElementsInBlock(p, b);
      expectedStatistics.numBytes_ += input.numBytesInBlock(p, b);
    }
  }
  const auto statistics = input.statistics();
  EXPECT_EQ(statistics.numBlocks_, expectedStatistics.numBlocks_);
  EXPECT_EQ(statistics.numWords_, expectedStatistics.numWords_);
  EXPECT_EQ(statistics.numBytes_, expectedStatistics.numBytes_);
  const size_t totalNumWords =
      words[0].size() + words[1].size() + words[2].size();
  EXPECT_EQ(statistics.numWords_, totalNumWords);
  EXPECT_GE(statistics.numBlocks_, 5u);
  // The serialized form of a word takes at least 17 bytes plus the word.
  EXPECT_GT(statistics.numBytes_, totalNumWords * 17);
  for (size_t p = 0; p < 3; ++p) {
    // The first partial vocabulary has all the words and hence spans several
    // blocks, the last one about a third of them.
    EXPECT_GE(input.numBlocks(p), p == 0 ? 3u : 1u);
    std::vector<std::tuple<std::string, bool, uint64_t>> allWords;
    for (size_t b = 0; b < input.numBlocks(p); ++b) {
      auto block = input.getBlock(p, b);
      ASSERT_FALSE(block.empty());
      EXPECT_EQ(block.size(), input.numElementsInBlock(p, b));
      EXPECT_EQ(input.firstElement(p, b).iriOrLiteral(),
                block.front().iriOrLiteral());
      EXPECT_EQ(input.lastElement(p, b).iriOrLiteral(),
                block.back().iriOrLiteral());
      EXPECT_EQ(input.firstElement(p, b).partialFileId_, p);
      for (const auto& word : block) {
        EXPECT_EQ(word.partialFileId_, p);
      }
      auto tuples = tuplesOfBlock(block);
      allWords.insert(allWords.end(), tuples.begin(), tuples.end());
    }
    ASSERT_EQ(allWords.size(), words[p].size());
    for (size_t i = 0; i < allWords.size(); ++i) {
      EXPECT_EQ(allWords[i],
                std::make_tuple(words[p][i], i % 3 == 2, uint64_t{i}));
    }
  }

  // The words of a file can still be read the old way: the number of words,
  // then that many words.
  ad_utility::serialization::FileReadSerializer reader{
      filenames.wordsFiles_[2]};
  uint64_t numWordsInFile;
  reader >> numWordsInFile;
  EXPECT_EQ(numWordsInFile, words[2].size());
  TripleComponentWithIndex first;
  reader >> first;
  EXPECT_EQ(first.iriOrLiteral(), words[2].front());
}

// Test that an empty partial vocabulary has no blocks, and that a file without
// a block index is rejected.
TEST(PartialVocabularyFile, emptyAndInvalid) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 2);
  writePartialVocabulary(filenames.wordsFiles_[0], {});
  {
    PartialVocabularyRunsInput input{partialVocabBasename, 1};
    EXPECT_EQ(input.numRuns(), 1u);
    EXPECT_EQ(input.numBlocks(0), 0u);
  }
  // A file that only holds the (zero) number of words.
  {
    ad_utility::serialization::FileWriteSerializer file{
        filenames.wordsFiles_[1]};
    file << uint64_t{0};
  }
  EXPECT_ANY_THROW((PartialVocabularyRunsInput{partialVocabBasename, 2}));
}

// Test that the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`) merges the partial
// vocabularies through `PartialVocabularyRunsInput`, on its parallel path
// (several chunks on a thread pool): the merged words are sorted, every
// occurrence of every word in every partial vocabulary arrives exactly once
// with its local index and its external flag, and the occurrences of the
// same word are consecutive.
TEST(PartialVocabularyFile, mergeInParallel) {
  using namespace ad_utility::parallelBlockMerge;
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 4);
  // Well above the threshold below which the merge is serial.
  size_t numWords = 250'000;
  std::vector<std::vector<std::string>> words;
  for (size_t p = 0; p < 4; ++p) {
    words.push_back(wordsOfPartialVocabulary(p, numWords));
    writePartialVocabulary(filenames.wordsFiles_[p], words.back());
  }

  // The expected occurrences, sorted by word and then by partial vocabulary.
  using Occurrence = std::tuple<std::string, uint32_t, uint64_t, bool>;
  std::vector<Occurrence> expected;
  for (size_t k = 0; k < numWords; ++k) {
    for (size_t p = 0; p < 4; ++p) {
      if (k % (p + 1) == 0) {
        size_t localIndex = k / (p + 1);
        expected.emplace_back(words[0][k], p, localIndex, localIndex % 3 == 2);
      }
    }
  }

  auto comparator = [](const MergeWord& a, const MergeWord& b) {
    return a.iriOrLiteral() < b.iriOrLiteral();
  };
  MergeOptions options;
  options.parallelismHint = 4;
  options.serialNumElementsThreshold = 0;
  boost::asio::thread_pool pool{4};
  std::vector<Occurrence> merged;
  {
    auto blocks = parallelBlockMergeToRange<true>(
        pool.get_executor(),
        PartialVocabularyRunsInput{partialVocabBasename, 4}, comparator,
        makeInMemoryStorageFactory<MergeBlock>(4), options);
    for (const MergeBlock& block : blocks) {
      for (const MergeWord& word : block) {
        // The words arrive in sorted order.
        if (!merged.empty()) {
          EXPECT_LE(std::get<0>(merged.back()), word.iriOrLiteral());
        }
        merged.emplace_back(word.iriOrLiteral(), word.partialFileId_, word.id(),
                            word.isExternal());
      }
    }
  }
  pool.join();
  ASSERT_EQ(merged.size(), expected.size());
  // The occurrences of the same word are consecutive, but their order among
  // the partial vocabularies is not specified.
  auto first = merged.begin();
  while (first != merged.end()) {
    auto last = first;
    while (last != merged.end() && std::get<0>(*last) == std::get<0>(*first)) {
      ++last;
    }
    std::sort(first, last);
    first = last;
  }
  EXPECT_EQ(merged, expected);
}

// Test the accessors of a `MergeBlock` of merged words and that `clear`
// releases its words, and that the weight of a block of a file is its size in
// bytes.
TEST(PartialVocabularyFile, mergeBlockAccessorsAndBlockWeight) {
  // A block of three merged words.
  MergeBlock block;
  block.push("\"a\"", false, 0, 7);
  block.push("\"b\"", true, 1, 8);
  block.push("\"c\"", false, 2, 9);
  const MergeBlock& constBlock = block;
  ASSERT_EQ(block.size(), 3u);
  EXPECT_EQ(block[1].iriOrLiteral(), "\"b\"");
  EXPECT_EQ(constBlock[2].id(), 9u);
  EXPECT_EQ(block.front().partialFileId_, 0u);
  EXPECT_EQ(constBlock.front().id(), 7u);
  EXPECT_EQ(block.back().partialFileId_, 2u);
  EXPECT_EQ(constBlock.back().id(), 9u);

  // The external flag can be changed in place.
  block[0].isExternal() = true;
  EXPECT_TRUE(constBlock[0].isExternal());

  // A block can be moved (the words keep pointing into its storage), the
  // moved-from block is empty.
  MergeBlock moved{std::move(block)};
  EXPECT_TRUE(block.empty());
  ASSERT_EQ(moved.size(), 3u);
  EXPECT_EQ(moved[1].iriOrLiteral(), "\"b\"");
  block = std::move(moved);
  EXPECT_TRUE(moved.empty());
  ASSERT_EQ(block.size(), 3u);
  EXPECT_EQ(block[2].iriOrLiteral(), "\"c\"");

  // A cleared block is empty, and can be filled again.
  block.clear();
  EXPECT_TRUE(block.empty());
  EXPECT_EQ(block.size(), 0u);
  block.push("\"d\"", false, 3, 10);
  ASSERT_EQ(block.size(), 1u);
  EXPECT_EQ(block[0].iriOrLiteral(), "\"d\"");

  // A partial vocabulary with two blocks. The weight of a block is its size in
  // bytes, and a block ends after the word that makes it at least
  // `PARTIAL_VOCAB_BLOCK_SIZE` bytes long, so every block but the last is at
  // least that large.
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  size_t numWords = 2 * PARTIAL_VOCAB_BLOCK_SIZE.getBytes() / 16;
  auto words = wordsOfPartialVocabulary(0, numWords);
  writePartialVocabulary(filenames.wordsFiles_[0], words);
  PartialVocabularyRunsInput input{partialVocabBasename, 1};
  ASSERT_GE(input.numBlocks(0), 2u);
  for (size_t b = 0; b < input.numBlocks(0); ++b) {
    EXPECT_EQ(input.blockWeight(0, b), input.numBytesInBlock(0, b));
    if (b + 1 < input.numBlocks(0)) {
      EXPECT_GE(input.numBytesInBlock(0, b),
                PARTIAL_VOCAB_BLOCK_SIZE.getBytes());
    }
  }
  auto firstBlock = input.getBlock(0, 0);
  EXPECT_EQ(firstBlock[0].iriOrLiteral(), words.front());
}

// Test that a buffer of a `BufferPool` that was taken for a word longer than
// the capacity of the pool is shrunk back to the capacity when it is given
// back, directly and through a `MergeBlock` with pools.
TEST(PartialVocabularyFile, bufferPoolShrinksOversizedBuffers) {
  using ad_utility::vocabulary_merger::detail::BufferPools;
  constexpr size_t capacity = 32;
  auto pools = std::make_shared<BufferPools>(capacity, capacity);
  auto& pool = pools->mergedWords_;

  auto large = pool.take(3 * capacity);
  EXPECT_GE(large.capacity(), 3 * capacity);
  pool.giveBack(std::move(large));
  EXPECT_EQ(pool.take().capacity(), capacity);

  // A block stores a word that is longer than the capacity in a buffer of its
  // own, and the next word in a buffer of the capacity. Both go back to the
  // pool when the block is destroyed, the larger one shrunk.
  {
    MergeBlock block{pools};
    const std::string longWord(2 * capacity, 'x');
    block.push(longWord, false, 0, 0);
    block.push("\"a\"", false, 0, 1);
    ASSERT_EQ(block.size(), 2u);
    EXPECT_EQ(block[0].iriOrLiteral(), longWord);
    EXPECT_EQ(block[1].iriOrLiteral(), "\"a\"");
  }
  for (size_t i = 0; i < 2; ++i) {
    EXPECT_EQ(pool.take().capacity(), capacity);
  }
}

// Test that a second `finish` of a `PartialVocabularyWriter` does nothing and
// that no word can be added after `finish`.
TEST(PartialVocabularyFile, finishTwice) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  PartialVocabularyWriter writer{filenames.wordsFiles_[0]};
  writer("\"a\"", PartialVocabIndexWithExternalFlag{0, false});
  writer.finish();
  writer.finish();
  EXPECT_THROW(writer("\"b\"", PartialVocabIndexWithExternalFlag{1, false}),
               ad_utility::Exception);

  // The file holds its single word in a single block.
  PartialVocabularyRunsInput input{partialVocabBasename, 1};
  ASSERT_EQ(input.numBlocks(0), 1u);
  EXPECT_EQ(input.numElementsInBlock(0, 0), 1u);
  EXPECT_EQ(input.firstElement(0, 0).iriOrLiteral(), "\"a\"");
}
