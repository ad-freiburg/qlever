// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYMERGECONFIG_H
#define QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYMERGECONFIG_H

#include <algorithm>
#include <cstddef>
#include <string>
#include <utility>
#include <variant>

#include "backports/asio.h"
#include "index/vocabulary_merger/PartialVocabularyInput.h"
#include "index/vocabulary_merger/QueueWordBlockCodec.h"
#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/parallelBlockMerge/MergeOptions.h"

// How the parallel merge of the partial vocabularies (see `mergeVocabulary` in
// `index/VocabularyMerger.h`) is configured: the way in which it splits its
// memory limit between the read buffers of the input blocks (see
// `PartialVocabularyInput`) and the output blocks, the number of chunks that
// it keeps in flight, and the storage that it spills its output blocks to.
// Like `engine/idTable/ExternalIdTableSorterMergeConfig.h` for the external
// sorter, this is a self-contained computation that can be tested without
// running a single merge.
namespace ad_utility::vocabulary_merger {

// The fraction of the memory limit of `mergeVocabulary` that the merge itself
// may use. The rest is for the batches of merged words that are handed on to
// the writing of the vocabulary and the ID maps (see
// `VOCAB_MERGER_WORD_BATCH_MEMORY_SIZE` in `index/ConstantsIndexBuilding.h`),
// the memory of which is hard to measure exactly.
constexpr inline double VOCAB_MERGE_MEMORY_FRACTION = 0.8;

// The fraction of the memory of the merge (see above) that is used for the
// read buffers of the input blocks. A read buffer only has to be large enough
// to make the reads from the partial vocabularies efficient, so the larger part
// of the memory goes to the output blocks.
constexpr inline double VOCAB_MERGE_INPUT_MEMORY_FRACTION = 0.25;

// The bounds for the memory of a single output block. The number of chunks in
// flight is reduced until the output blocks are at least as large as the
// lower bound (unless a single chunk is already too much), and larger blocks
// than the upper bound don't make the merge any faster.
constexpr inline MemorySize MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY =
    MemorySize::kilobytes(256);
constexpr inline MemorySize MAX_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY =
    MemorySize::megabytes(16);

// The absolute floor for the memory of a single output block, which is only
// used if the memory limit is too small for even a single chunk with an output
// block of `MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY`.
constexpr inline MemorySize ABSOLUTE_MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY =
    MemorySize::kilobytes(64);

// The number of finished output blocks that the chunk which the consumer
// currently reads keeps in memory before it spills (every other chunk spills
// all of its blocks), see `parallelBlockMerge::SpillingBlockStorage`.
constexpr inline size_t VOCAB_MERGE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK = 1;

// The number of output blocks that the consumer of the merge reads ahead (and
// thereby reads back from the spill files concurrently), see
// `parallelBlockMerge::MergeOptions::numPrefetchedOutputBlocks`.
constexpr inline size_t VOCAB_MERGE_NUM_PREFETCHED_OUTPUT_BLOCKS = 3;

// The size (in words) of the first chunk of the merge, see
// `parallelBlockMerge::MergeOptions::firstChunkSize`. The consumer has to wait
// for the first chunk before it can hand on the first batch of merged words
// (of `VOCAB_MERGER_WORD_BATCH_SIZE` words) to the writing of the vocabulary,
// so a first chunk of the size of such a batch keeps the pipeline of
// `mergeVocabulary` busy right from the start.
constexpr inline size_t FIRST_VOCAB_MERGE_CHUNK_SIZE = 100'000;

// The parameters of a merge of the partial vocabularies, see
// `computeVocabularyMergeParameters`.
struct VocabularyMergeParameters {
  // The number of chunks that are merged concurrently.
  size_t numChunksInFlight_;
  // The size of the read buffer of a single input block, see
  // `PartialVocabularyInput`.
  MemorySize readBufferSize_;
  // The memory limit of a single output block.
  MemorySize outputBlockMemory_;
  // See `VOCAB_MERGE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK`.
  size_t numBufferedBlocksPerChunk_ =
      VOCAB_MERGE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK;
  // See `VOCAB_MERGE_NUM_PREFETCHED_OUTPUT_BLOCKS`.
  size_t numPrefetchedOutputBlocks_ = VOCAB_MERGE_NUM_PREFETCHED_OUTPUT_BLOCKS;
};

// Return the number of output blocks that are alive at the same time when
// `numChunksInFlight` chunks are merged concurrently: per chunk the one that is
// being filled, the one that may be on its way to the spill file, and the
// buffered ones, and on the consumer side the prefetched blocks, the one that
// the read-ahead is handing over, and the one that the consumer holds.
constexpr size_t numLiveVocabularyMergeOutputBlocks(
    size_t numChunksInFlight, size_t numBufferedBlocksPerChunk,
    size_t numPrefetchedOutputBlocks) {
  return numChunksInFlight * (numBufferedBlocksPerChunk + 2) +
         numPrefetchedOutputBlocks + 2;
}

// The memory of a merge of the partial vocabularies, split between the read
// buffers of the input blocks and the output blocks, see
// `vocabularyMergeMemorySplit`.
struct VocabularyMergeMemorySplit {
  MemorySize input_;
  MemorySize output_;
};

// Split the memory limit `memoryToUse` of `mergeVocabulary` as follows:
// `VOCAB_MERGE_MEMORY_FRACTION` of it is used by the merge, of which
// `VOCAB_MERGE_INPUT_MEMORY_FRACTION` goes to the read buffers of the input
// blocks and the rest to the output blocks.
inline VocabularyMergeMemorySplit vocabularyMergeMemorySplit(
    MemorySize memoryToUse) {
  const MemorySize mergeMemory = VOCAB_MERGE_MEMORY_FRACTION * memoryToUse;
  const MemorySize inputMemory =
      VOCAB_MERGE_INPUT_MEMORY_FRACTION * mergeMemory;
  return {inputMemory, mergeMemory - inputMemory};
}

// Return the memory of a single output block (before the clamping, see
// `computeVocabularyMergeParameters`) if the `outputMemory` is split evenly
// between all the output blocks that are alive at the same time, see
// `numLiveVocabularyMergeOutputBlocks`.
inline MemorySize vocabularyMergeOutputBlockMemory(
    MemorySize outputMemory, size_t numChunksInFlight,
    size_t numBufferedBlocksPerChunk =
        VOCAB_MERGE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK,
    size_t numPrefetchedOutputBlocks =
        VOCAB_MERGE_NUM_PREFETCHED_OUTPUT_BLOCKS) {
  return MemorySize::bytes(outputMemory.getBytes() /
                           numLiveVocabularyMergeOutputBlocks(
                               numChunksInFlight, numBufferedBlocksPerChunk,
                               numPrefetchedOutputBlocks));
}

// Compute the parameters of a merge of `numRuns` partial vocabularies with the
// given `memoryToUse` (the memory limit of `mergeVocabulary`, which is split
// by `vocabularyMergeMemorySplit`) and `parallelism` (the number of threads
// that the merge may use). Every chunk in flight reads one block of every run
// at a time. As many chunks as the `parallelism` allows are kept in flight,
// unless either the read buffers of minimal size (see
// `PartialVocabularyInput::maxNumChunksInFlightForBudget`) or the output blocks
// of size `MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY` (see
// `vocabularyMergeOutputBlockMemory`) don't fit for that many chunks.
inline VocabularyMergeParameters computeVocabularyMergeParameters(
    MemorySize memoryToUse, size_t numRuns, size_t parallelism) {
  AD_CONTRACT_CHECK(parallelism > 0);
  const auto [inputMemory, outputMemory] =
      vocabularyMergeMemorySplit(memoryToUse);

  VocabularyMergeParameters result{};
  // Return the memory of a single output block for `numChunksInFlight`.
  auto outputBlockMemory = [&result,
                            outputMemory = outputMemory](size_t numInFlight) {
    return vocabularyMergeOutputBlockMemory(outputMemory, numInFlight,
                                            result.numBufferedBlocksPerChunk_,
                                            result.numPrefetchedOutputBlocks_);
  };

  size_t numChunksInFlight =
      PartialVocabularyInput::maxNumChunksInFlightForBudget(
          inputMemory, numRuns, parallelism);
  while (numChunksInFlight > 1 && outputBlockMemory(numChunksInFlight) <
                                      MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY) {
    --numChunksInFlight;
  }
  result.numChunksInFlight_ = numChunksInFlight;
  result.readBufferSize_ = PartialVocabularyInput::readBufferSizeForBudget(
      inputMemory, numRuns, numChunksInFlight);
  result.outputBlockMemory_ =
      std::clamp(outputBlockMemory(numChunksInFlight),
                 ABSOLUTE_MIN_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY,
                 MAX_VOCAB_MERGE_OUTPUT_BLOCK_MEMORY);
  return result;
}

// Turn the `parameters` into the options of the parallel merge.
inline parallelBlockMerge::MergeOptions makeVocabularyMergeOptions(
    const VocabularyMergeParameters& parameters, size_t parallelism) {
  parallelBlockMerge::MergeOptions options;
  // The words have very different sizes, so the memory is the only criterion
  // for finishing an output block.
  options.outputBlockSize = parallelBlockMerge::OutputBlockSize::memory(
      parameters.outputBlockMemory_);
  options.parallelismHint = parallelism;
  options.maxNumChunksInFlight = parameters.numChunksInFlight_;
  options.firstChunkSize = FIRST_VOCAB_MERGE_CHUNK_SIZE;
  options.numPrefetchedOutputBlocks = parameters.numPrefetchedOutputBlocks_;
  return options;
}

// The factory for the storage of the output blocks of the merge, which spills
// them to files that start with the `spillFilenamePrefix`, see
// `makeQueueWordBlockStorageFactory`. That storage is only used by the
// coroutine-based parallel merge and hence does not exist in the C++17
// backports mode, where `parallelBlockMergeToRange` merges serially and
// ignores the factory altogether. A placeholder therefore suffices in that
// mode.
inline auto makeVocabularyMergeStorageFactory(
    [[maybe_unused]] ql::any_io_executor ioExecutor,
    [[maybe_unused]] std::string spillFilenamePrefix,
    [[maybe_unused]] const VocabularyMergeParameters& parameters) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  return std::monostate{};
#else
  return makeQueueWordBlockStorageFactory(
      std::move(ioExecutor), std::move(spillFilenamePrefix),
      parameters.numBufferedBlocksPerChunk_);
#endif
}

}  // namespace ad_utility::vocabulary_merger

#endif  // QLEVER_SRC_INDEX_VOCABULARY_MERGER_VOCABULARYMERGECONFIG_H
