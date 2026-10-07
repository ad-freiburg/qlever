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
// Check the invariants of the `parameters` for a merge of `numRuns` runs with
// the given `memoryToUse` and `parallelism`, see
// `computeVocabularyMergeParameters`.
void expectInvariants(const VocabularyMergeParameters& parameters,
                      MemorySize memoryToUse, size_t numRuns,
                      size_t parallelism,
                      ad_utility::source_location l = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(l);
  EXPECT_GE(parameters.numChunksInFlight_, 1u);
  EXPECT_LE(parameters.numChunksInFlight_, parallelism);
  EXPECT_GE(parameters.readBufferSize_,
            PartialVocabularyInput::minReadBufferSize);
  EXPECT_LE(parameters.readBufferSize_,
            PartialVocabularyInput::maxReadBufferSize);
  EXPECT_GE(parameters.outputBlockMemory_,
            ABSOLUTE_MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  EXPECT_LE(parameters.outputBlockMemory_, MAX_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);

  // Unless the parameters are at their minimum (in which case the memory limit
  // is simply too small), the merge stays within its share of the memory.
  MemorySize mergeMemory = VOCAB_MERGE_MEMORY_FRACTION * memoryToUse;
  MemorySize inputMemory = VOCAB_MERGE_INPUT_MEMORY_FRACTION * mergeMemory;
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
    EXPECT_LE(parameters.outputBlockMemory_ * numLiveBlocks,
              mergeMemory - inputMemory);
  }
}
}  // namespace

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, largeMemoryUsesFullParallelism) {
  auto memory = MemorySize::gigabytes(10);
  auto parameters = computeVocabularyMergeParameters(memory, 100, 8);
  expectInvariants(parameters, memory, 100, 8);
  EXPECT_EQ(parameters.numChunksInFlight_, 8u);
  // The output blocks are capped.
  EXPECT_EQ(parameters.outputBlockMemory_, MAX_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  // The read buffers use the input budget, which is far from both bounds.
  auto inputMemory = VOCAB_MERGE_INPUT_MEMORY_FRACTION *
                     (VOCAB_MERGE_MEMORY_FRACTION * memory);
  EXPECT_EQ(
      parameters.readBufferSize_,
      PartialVocabularyInput::readBufferSizeForBudget(inputMemory, 100, 8));
  EXPECT_EQ(parameters.numBufferedBlocksPerChunk_,
            VOCAB_MERGE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK);
  EXPECT_EQ(parameters.numPrefetchedOutputBlocks_,
            VOCAB_MERGE_NUM_PREFETCHED_OUTPUT_BLOCKS);
}

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, manyRunsLimitTheNumberOfChunks) {
  // The read buffers of minimal size for 1000 runs only leave room for a few
  // chunks in flight.
  auto memory = MemorySize::gigabytes(1);
  auto parameters = computeVocabularyMergeParameters(memory, 1000, 32);
  expectInvariants(parameters, memory, 1000, 32);
  auto inputMemory = VOCAB_MERGE_INPUT_MEMORY_FRACTION *
                     (VOCAB_MERGE_MEMORY_FRACTION * memory);
  EXPECT_EQ(parameters.numChunksInFlight_,
            PartialVocabularyInput::maxNumChunksInFlightForBudget(inputMemory,
                                                                  1000, 32));
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
  auto parameters = computeVocabularyMergeParameters(memory, 2, 64);
  expectInvariants(parameters, memory, 2, 64);
  EXPECT_GT(parameters.numChunksInFlight_, 1u);
  EXPECT_LT(parameters.numChunksInFlight_, 64u);
  EXPECT_GE(parameters.outputBlockMemory_, MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  // One more chunk would make the output blocks too small.
  auto outputMemory = (1.0 - VOCAB_MERGE_INPUT_MEMORY_FRACTION) *
                      (VOCAB_MERGE_MEMORY_FRACTION * memory);
  EXPECT_LT(MemorySize::bytes(outputMemory.getBytes() /
                              numLiveVocabularyMergeOutputBlocks(
                                  parameters.numChunksInFlight_ + 1,
                                  parameters.numBufferedBlocksPerChunk_,
                                  parameters.numPrefetchedOutputBlocks_)),
            MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
}

// _____________________________________________________________________________
TEST(VocabularyMergeConfig, tinyMemory) {
  // Not even a single chunk fits, so everything is at its minimum.
  for (size_t numRuns : {0, 1, 100}) {
    auto parameters =
        computeVocabularyMergeParameters(MemorySize::kilobytes(10), numRuns, 8);
    expectInvariants(parameters, MemorySize::kilobytes(10), numRuns, 8);
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
  auto parameters =
      computeVocabularyMergeParameters(MemorySize::gigabytes(1), 10, 4);
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
