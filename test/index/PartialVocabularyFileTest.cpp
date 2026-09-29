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

#include <string>
#include <vector>

#include "./vocabulary_merger/VocabularyMergerTestHelpers.h"
#include "index/ConstantsIndexBuilding.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary_merger/PartialVocabularyFile.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
using ad_utility::vocabulary_merger::detail::QueueWord;

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
    writer(words[i], i % 3 == 2, i);
  }
  writer.finish();
}

// The words of a block as (word, isExternal, local index) tuples.
std::vector<std::tuple<std::string, bool, uint64_t>> tuplesOfBlock(
    const std::vector<QueueWord>& block) {
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

// Test that `appendToBlock` folds a word that is equal to the last word of the
// block into that word: the occurrence is recorded, the external flag is
// combined, and a different word starts a new entry.
TEST(PartialVocabularyFile, appendToBlockFoldsDuplicates) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  writePartialVocabulary(filenames.wordsFiles_[0], {"\"a\""});
  PartialVocabularyRunsInput input{partialVocabBasename, 1};
  auto block = input.makeEmptyBlock();
  auto word = [](std::string w, bool isExternal, uint64_t localIndex,
                 size_t partial) {
    return QueueWord{
        TripleComponentWithIndex{std::move(w), isExternal, localIndex},
        partial};
  };
  input.appendToBlock(block, word("\"a\"", false, 3, 0));
  input.appendToBlock(block, word("\"a\"", true, 5, 2));
  input.appendToBlock(block, word("\"a\"", false, 1, 4));
  input.appendToBlock(block, word("\"b\"", false, 7, 1));
  ASSERT_EQ(block.size(), 2u);
  EXPECT_EQ(block[0].iriOrLiteral(), "\"a\"");
  EXPECT_TRUE(block[0].isExternal());
  EXPECT_EQ(block[0].partialFileId_, 0u);
  EXPECT_EQ(block[0].id(), 3u);
  EXPECT_THAT(block[0].moreOccurrences_,
              ::testing::ElementsAre(std::pair{2u, uint64_t{5}},
                                     std::pair{4u, uint64_t{1}}));
  EXPECT_EQ(block[1].iriOrLiteral(), "\"b\"");
  EXPECT_FALSE(block[1].isExternal());
  EXPECT_TRUE(block[1].moreOccurrences_.empty());
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

// Test the merge of partial vocabularies that are large enough for the
// parallel path of the block merge (several chunks on the global thread pool):
// the merged vocabulary is the sorted union of the words, a word that occurs in
// several partial vocabularies is written once, it is external if it is
// external in any of them, and every partial ID map maps each local index to
// the global ID of its word.
TEST(PartialVocabularyFile, mergeInParallel) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 4);
  // Well above the threshold below which the merge is serial.
  size_t numWords = 250'000;
  std::vector<std::vector<std::string>> words;
  for (size_t p = 0; p < 4; ++p) {
    words.push_back(wordsOfPartialVocabulary(p, numWords));
    writePartialVocabulary(filenames.wordsFiles_[p], words.back());
  }

  std::vector<std::pair<std::string, bool>> vocabulary;
  auto wordCallback = makeCollectingWordCallback(vocabulary);
  auto comparator = [](std::string_view a, std::string_view b) {
    return a < b;
  };
  auto metaData =
      mergeVocabulary(partialVocabBasename, 4, comparator, wordCallback,
                      ad_utility::MemorySize::gigabytes(1));
  ASSERT_EQ(vocabulary.size(), numWords);
  EXPECT_EQ(metaData.numWordsTotal(), numWords);
  for (size_t k = 0; k < numWords; ++k) {
    EXPECT_EQ(vocabulary[k].first, words[0][k]);
    // The word `k` is external iff its local index in one of the partial
    // vocabularies that contain it is 2 modulo 3.
    bool expectedExternal = false;
    for (size_t p = 0; p < 4; ++p) {
      if (k % (p + 1) == 0) {
        expectedExternal |= (k / (p + 1)) % 3 == 2;
      }
    }
    EXPECT_EQ(vocabulary[k].second, expectedExternal) << k;
  }
  for (size_t p = 0; p < 4; ++p) {
    auto idMap = IdMapFromPartialIdMapFile(filenames.idMapFiles_[p]);
    ASSERT_EQ(idMap.size(), words[p].size());
    for (size_t i = 0; i < words[p].size(); ++i) {
      EXPECT_EQ(idMap.at(VocabIndex::make(i)),
                Id::makeFromVocabIndex(VocabIndex::make(i * (p + 1))));
    }
  }
}
