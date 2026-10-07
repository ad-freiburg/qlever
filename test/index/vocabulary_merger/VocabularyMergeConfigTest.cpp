// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <cstddef>
#include <limits>

#include "../../util/GTestHelpers.h"
#include "index/vocabulary_merger/VocabularyMergeConfig.h"

using namespace ad_utility::vocabulary_merger;
using ad_utility::MemorySize;

namespace {
// Compute the parameters for a merge of `numRuns` runs with the given
// `memoryToUse` and `parallelism` (see `computeVocabularyMergeParameters`),
// check their invariants, and return them.
VocabularyMergeParameters computeAndCheck(
    MemorySize memoryToUse, size_t numRuns, size_t parallelism,
    ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  auto parameters =
      computeVocabularyMergeParameters(memoryToUse, numRuns, parallelism);
  EXPECT_GE(parameters.numChunksInFlight_, 1u);
  EXPECT_LE(parameters.numChunksInFlight_, parallelism);
  EXPECT_GE(parameters.readBufferSize_,
            PartialVocabularyInput::minReadBufferSize);
  EXPECT_LE(parameters.readBufferSize_,
            PartialVocabularyInput::maxReadBufferSize);
  EXPECT_GE(parameters.outputBlockMemory_,
            ABSOLUTE_MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  EXPECT_LE(parameters.outputBlockMemory_, MAX_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  EXPECT_EQ(parameters.numBufferedBlocksPerChunk_,
            VOCAB_MERGE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK);
  EXPECT_EQ(parameters.numPrefetchedOutputBlocks_,
            VOCAB_MERGE_NUM_PREFETCHED_OUTPUT_BLOCKS);

  // Unless the parameters are at their minimum (in which case the memory limit
  // is simply too small), the merge stays within its share of the memory.
  auto [inputMemory, outputMemory] = vocabularyMergeMemorySplit(memoryToUse);
  if (parameters.numChunksInFlight_ > 1 ||
      parameters.readBufferSize_ > PartialVocabularyInput::minReadBufferSize) {
    EXPECT_LE(
        parameters.readBufferSize_ * (numRuns * parameters.numChunksInFlight_),
        inputMemory);
  }
  size_t numLiveBlocks = numLiveVocabularyMergeOutputBlocks(
      parameters.numChunksInFlight_, parameters.numBufferedBlocksPerChunk_,
      parameters.numPrefetchedOutputBlocks_);
  if (parameters.outputBlockMemory_ >
      ABSOLUTE_MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY) {
    EXPECT_LE(parameters.outputBlockMemory_ * numLiveBlocks, outputMemory);
  }
  return parameters;
}
}  // namespace

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, memorySplit) {
  auto [input, output] = vocabularyMergeMemorySplit(MemorySize::bytes(1000));
  EXPECT_EQ(input, MemorySize::bytes(200));
  EXPECT_EQ(output, MemorySize::bytes(600));
  // The output memory is split evenly between all the live output blocks.
  EXPECT_EQ(
      vocabularyMergeOutputBlockMemory(MemorySize::bytes(1000), 2, 1, 3),
      MemorySize::bytes(1000 / numLiveVocabularyMergeOutputBlocks(2, 1, 3)));
  EXPECT_EQ(numLiveVocabularyMergeOutputBlocks(2, 1, 3), 2u * 3u + 3u + 2u);
}

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, largeMemoryUsesFullParallelism) {
  auto memory = MemorySize::gigabytes(10);
  auto parameters = computeAndCheck(memory, 100, 8);
  EXPECT_EQ(parameters.numChunksInFlight_, 8u);
  // The output blocks are capped.
  EXPECT_EQ(parameters.outputBlockMemory_, MAX_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  // The read buffers use the input budget, which is far from both bounds.
  EXPECT_EQ(parameters.readBufferSize_,
            PartialVocabularyInput::readBufferSizeForBudget(
                vocabularyMergeMemorySplit(memory).input_, 100, 8));
}

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, manyRunsLimitTheNumberOfChunks) {
  // The read buffers of minimal size for 1000 runs only leave room for a few
  // chunks in flight.
  auto memory = MemorySize::gigabytes(1);
  auto parameters = computeAndCheck(memory, 1000, 32);
  EXPECT_EQ(parameters.numChunksInFlight_,
            PartialVocabularyInput::maxNumChunksInFlightForBudget(
                vocabularyMergeMemorySplit(memory).input_, 1000, 32));
  EXPECT_LT(parameters.numChunksInFlight_, 32u);
  EXPECT_GT(parameters.numChunksInFlight_, 1u);
  // The read buffers are close to their minimal size.
  EXPECT_LT(parameters.readBufferSize_,
            2 * PartialVocabularyInput::minReadBufferSize);
}

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, smallOutputBlocksLimitTheNumberOfChunks) {
  // Few runs, so the read buffers would allow all the chunks, but then the
  // output blocks would be smaller than `MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY`.
  auto memory = MemorySize::megabytes(40);
  auto parameters = computeAndCheck(memory, 2, 64);
  EXPECT_GT(parameters.numChunksInFlight_, 1u);
  EXPECT_LT(parameters.numChunksInFlight_, 64u);
  EXPECT_GE(parameters.outputBlockMemory_, MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  // One more chunk would make the output blocks too small.
  EXPECT_LT(vocabularyMergeOutputBlockMemory(
                vocabularyMergeMemorySplit(memory).output_,
                parameters.numChunksInFlight_ + 1),
            MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
}

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, tinyMemory) {
  // Not even a single chunk fits, so everything is at its minimum.
  for (size_t numRuns : {0, 1, 100}) {
    auto parameters = computeAndCheck(MemorySize::kilobytes(10), numRuns, 8);
    EXPECT_EQ(parameters.numChunksInFlight_, 1u);
    EXPECT_EQ(parameters.readBufferSize_,
              PartialVocabularyInput::minReadBufferSize);
    EXPECT_EQ(parameters.outputBlockMemory_,
              ABSOLUTE_MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  }
  // A parallelism of zero is a bug of the caller.
  EXPECT_ANY_THROW(
      computeVocabularyMergeParameters(MemorySize::gigabytes(1), 10, 0));
}

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, makeVocabularyMergeOptions) {
  auto parameters = computeAndCheck(MemorySize::gigabytes(1), 10, 4);
  auto options = makeVocabularyMergeOptions(parameters, 4);
  EXPECT_EQ(options.parallelismHint, 4u);
  EXPECT_EQ(options.maxNumChunksInFlight, parameters.numChunksInFlight_);
  EXPECT_EQ(options.firstChunkSize, FIRST_VOCAB_MERGE_CHUNK_SIZE);
  EXPECT_EQ(options.numPrefetchedOutputBlocks,
            parameters.numPrefetchedOutputBlocks_);
  EXPECT_EQ(options.outputBlockSize.maxMemory(), parameters.outputBlockMemory_);
  EXPECT_EQ(options.outputBlockSize.maxNumElements(),
            std::numeric_limits<size_t>::max());
}
