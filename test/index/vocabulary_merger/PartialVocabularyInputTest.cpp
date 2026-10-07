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

#include <cstdint>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/ParallelBlockMergeTestHelpers.h"
#include "VocabularyMergerTestHelpers.h"
#include "backports/algorithm.h"
#include "index/VocabularyMerger.h"
#include "index/vocabulary_merger/PartialVocabularyInput.h"
#include "util/Random.h"
#include "util/parallelBlockMerge/ParallelBlockMerge.h"

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include "../../parallelBlockMerge/InMemoryBlockStorage.h"
#endif

using namespace ad_utility::vocabulary_merger;
using namespace vocabularyMergerTestHelpers;
using namespace ad_utility::memory_literals;
using ad_utility::parallelBlockMerge::MergeOptions;
using ad_utility::parallelBlockMerge::OutputBlockSize;
using ad_utility::vocabulary_merger::detail::QueueWord;

namespace {
// The base name of the partial vocabularies of the tests below, each of which
// runs in its own fresh working directory.
const std::string partialVocabBasename = "vocab";

// A word of a partial vocabulary in a form that is easy to compare:
// `(word, partialFileId, localIndex, isExternal)`.
using Word = std::tuple<std::string, size_t, uint64_t, bool>;

// Convert a `QueueWord` to a `Word`.
Word toWord(const QueueWord& word) {
  return {word.iriOrLiteral(), word.partialFileId_, word.id(),
          word.isExternal()};
}

// Write `numFiles` partial vocabularies with skip pointers every
// `skipPointerInterval` words, where the `i`-th one has `sizes[i]` words.
// The words are random (and in particular may occur in several of the partial
// vocabularies), sorted, and distinct within each partial vocabulary. The
// `i`-th word of a partial vocabulary has the local index `i` and a random
// `isExternal` flag. Return the words of each partial vocabulary.
std::vector<std::vector<Word>> writePartialVocabularies(
    const std::vector<size_t>& sizes, size_t skipPointerInterval) {
  ad_utility::SlowRandomIntGenerator<size_t> randomInt{
      0, 999, ad_utility::RandomSeed::make(42)};
  std::vector<std::vector<Word>> result;
  for (size_t fileId = 0; fileId < sizes.size(); ++fileId) {
    std::vector<std::string> words;
    for (size_t i = 0; i < sizes.at(fileId); ++i) {
      // The words have different lengths.
      words.push_back(absl::StrFormat("\"%d%s\"", randomInt(),
                                      std::string(randomInt() % 20, 'x')));
    }
    ql::ranges::sort(words);
    words.erase(std::unique(words.begin(), words.end()), words.end());
    ItemVec items;
    auto& expected = result.emplace_back();
    for (size_t i = 0; i < words.size(); ++i) {
      bool isExternal = randomInt() % 2 == 0;
      items.emplace_back(words.at(i),
                         PartialVocabIndexWithExternalFlag{i, isExternal});
      expected.emplace_back(words.at(i), fileId, i, isExternal);
    }
    writePartialVocabularyToFile(
        items, partialVocabularyWordsFilename(partialVocabBasename, fileId),
        skipPointerInterval);
  }
  return result;
}

// Read all the words of the given block.
std::vector<Word> readBlock(const PartialVocabularyInput& input, size_t runIdx,
                            size_t blockIdx) {
  std::vector<Word> result;
  for (const auto& word : input.getBlock(runIdx, blockIdx)) {
    result.push_back(toWord(word));
  }
  return result;
}

// Compare `QueueWord`s by their words only, as the vocabulary merger does
// (with a more complex comparator).
struct WordLess {
  bool operator()(const QueueWord& a, const QueueWord& b) const {
    return a.iriOrLiteral() < b.iriOrLiteral();
  }
};

// Check that the `mergedBlocks` (the result of a merge of the partial
// vocabularies with the given `expected` words) are sorted by the words, and
// contain exactly the `expected` words.
template <typename Blocks>
void expectMergeResult(
    Blocks&& mergedBlocks, const std::vector<std::vector<Word>>& expected,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  std::vector<Word> result;
  for (auto& block : mergedBlocks) {
    EXPECT_FALSE(block.empty());
    for (auto& word : block) {
      result.push_back(toWord(word));
    }
  }
  EXPECT_TRUE(ql::ranges::is_sorted(
      result, {},
      [](const Word& word) -> const auto& { return std::get<0>(word); }));
  std::vector<Word> expectedWords;
  for (const auto& words : expected) {
    expectedWords.insert(expectedWords.end(), words.begin(), words.end());
  }
  EXPECT_THAT(result, ::testing::UnorderedElementsAreArray(expectedWords));
}
}  // namespace

// _____________________________________________________________________________
// The metadata of the blocks is exactly the one of the skip pointers, and every
// block yields exactly its words, also when it is read several times and
// concurrently.
TEST(PartialVocabularyInput, metadataAndBlocks) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 4);
  // The third partial vocabulary is empty, the fourth one has exactly two
  // blocks.
  auto expected = writePartialVocabularies({100, 1, 0, 14}, 7);
  ASSERT_EQ(expected.at(3).size(), 14u);

  for (auto bufferSize : {1_B, 3_B, 64_kB}) {
    PartialVocabularyInput input{partialVocabBasename, 4, bufferSize};
    ASSERT_EQ(input.numRuns(), 4u);
    for (size_t runIdx = 0; runIdx < input.numRuns(); ++runIdx) {
      const auto& words = expected.at(runIdx);
      size_t numBlocks = (words.size() + 6) / 7;
      ASSERT_EQ(input.numBlocks(runIdx), numBlocks);
      for (size_t blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
        size_t begin = blockIdx * 7;
        size_t end = std::min(begin + 7, words.size());
        EXPECT_EQ(input.numElementsInBlock(runIdx, blockIdx), end - begin);
        EXPECT_EQ(toWord(input.firstElement(runIdx, blockIdx)),
                  words.at(begin));
        EXPECT_EQ(toWord(input.lastElement(runIdx, blockIdx)),
                  words.at(end - 1));
        std::vector<Word> expectedBlock(words.begin() + begin,
                                        words.begin() + end);
        EXPECT_THAT(readBlock(input, runIdx, blockIdx),
                    ::testing::ElementsAreArray(expectedBlock));
        // Reading the same block a second time yields the same words.
        EXPECT_THAT(readBlock(input, runIdx, blockIdx),
                    ::testing::ElementsAreArray(expectedBlock));
      }
    }
  }

  // Read the same blocks concurrently from several threads.
  PartialVocabularyInput input{partialVocabBasename, 4, 5_B};
  std::vector<size_t> numMismatches(8, 0);
  std::vector<std::thread> threads;
  for (size_t t = 0; t < numMismatches.size(); ++t) {
    threads.emplace_back([&input, &expected, &numMismatches, t]() {
      for (size_t round = 0; round < 10; ++round) {
        for (size_t blockIdx = 0; blockIdx < input.numBlocks(0); ++blockIdx) {
          auto block = readBlock(input, 0, blockIdx);
          for (size_t i = 0; i < block.size(); ++i) {
            numMismatches.at(t) +=
                block.at(i) != expected.at(0).at(blockIdx * 7 + i);
          }
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_THAT(numMismatches, ::testing::Each(0u));
}

// _____________________________________________________________________________
// Without a single partial vocabulary, or with only empty ones, there are no
// blocks at all.
TEST(PartialVocabularyInput, emptyInput) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 2);
  PartialVocabularyInput noRuns{partialVocabBasename, 0, 64_kB};
  EXPECT_EQ(noRuns.numRuns(), 0u);
  writePartialVocabularies({0, 0}, 7);
  PartialVocabularyInput input{partialVocabBasename, 2, 64_kB};
  EXPECT_EQ(input.numRuns(), 2u);
  EXPECT_EQ(input.numBlocks(0), 0u);
  EXPECT_EQ(input.numBlocks(1), 0u);
  expectMergeResult(
      ad_utility::parallelBlockMerge::serialBlockMergeToRange<true>(
          std::move(input), WordLess{}),
      {{}, {}});
}

// _____________________________________________________________________________
// A partial vocabulary without skip pointers (or a missing one) is rejected.
TEST(PartialVocabularyInput, missingSkipPointers) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 1);
  EXPECT_ANY_THROW((PartialVocabularyInput{partialVocabBasename, 1, 64_kB}));
  {
    ad_utility::serialization::FileWriteSerializer writer{
        filenames.wordsFiles_.at(0)};
    writer << uint64_t{0};
  }
  AD_EXPECT_THROW_WITH_MESSAGE(
      (PartialVocabularyInput{partialVocabBasename, 1, 64_kB}),
      ::testing::HasSubstr("has no skip pointers"));
  EXPECT_ANY_THROW((PartialVocabularyInput{partialVocabBasename, 1, 0_B}));
}

// _____________________________________________________________________________
TEST(PartialVocabularyInput, readBufferSizeForBudget) {
  using I = PartialVocabularyInput;
  // The budget is split evenly between all the buffers.
  EXPECT_EQ(I::readBufferSizeForBudget(80_MB, 10, 4), 2_MB);
  // The result is clamped.
  EXPECT_EQ(I::readBufferSizeForBudget(1_GB, 2, 2), I::maxReadBufferSize);
  EXPECT_EQ(I::readBufferSizeForBudget(1_kB, 100, 8), I::minReadBufferSize);
  EXPECT_EQ(I::readBufferSizeForBudget(1_GB, 0, 0), I::maxReadBufferSize);

  // The number of chunks in flight for which the minimal buffers still fit.
  auto minBytes = I::minReadBufferSize.getBytes();
  auto bytes = [](size_t numBytes) {
    return ad_utility::MemorySize::bytes(numBytes);
  };
  EXPECT_EQ(I::maxNumChunksInFlightForBudget(bytes(10 * 3 * minBytes), 10, 8),
            3u);
  EXPECT_EQ(I::maxNumChunksInFlightForBudget(1_GB, 10, 8), 8u);
  // At least one chunk is always allowed.
  EXPECT_EQ(I::maxNumChunksInFlightForBudget(1_B, 10, 8), 1u);
  EXPECT_EQ(I::maxNumChunksInFlightForBudget(1_GB, 10, 0), 1u);
}

// _____________________________________________________________________________
// Merging the partial vocabularies (serially and in parallel) yields all of
// their words in sorted order.
TEST(PartialVocabularyInput, merge) {
  auto [filenames, cleanup] =
      makePartialVocabularyFilenamesInFreshDirectory(partialVocabBasename, 5);
  auto expected = writePartialVocabularies({300, 0, 250, 1, 400}, 3);

  MergeOptions options;
  options.outputBlockSize = OutputBlockSize::numElements(17);
  options.parallelismHint = 4;
  options.targetChunksPerThread = 3;
  options.serialNumElementsThreshold = 0;

  expectMergeResult(
      ad_utility::parallelBlockMerge::serialBlockMergeToRange<true>(
          PartialVocabularyInput{partialVocabBasename, 5, 16_B}, WordLess{},
          options),
      expected);
  expectMergeResult(
      ad_utility::parallelBlockMerge::serialBlockMergeToRange<false>(
          PartialVocabularyInput{partialVocabBasename, 5, 64_kB}, WordLess{},
          options),
      expected);

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  using OutputBlock = PartialVocabularyInput::OutputBlock;
  auto mergeInParallel = [&](auto moveElements) {
    expectMergeResult(
        ad_utility::parallelBlockMerge::parallelBlockMergeToRange<
            decltype(moveElements)::value>(
            parallelBlockMergeTestHelpers::sharedTestExecutor(),
            PartialVocabularyInput{partialVocabBasename, 5, 16_B}, WordLess{},
            ad_utility::parallelBlockMerge::makeInMemoryStorageFactory<
                OutputBlock>(2),
            options),
        expected);
  };
  mergeInParallel(std::true_type{});
  mergeInParallel(std::false_type{});
#endif
}
