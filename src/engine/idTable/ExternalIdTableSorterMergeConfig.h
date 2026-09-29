// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_IDTABLE_EXTERNALIDTABLESORTERMERGECONFIG_H
#define QLEVER_SRC_ENGINE_IDTABLE_EXTERNALIDTABLESORTERMERGECONFIG_H

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

#include "backports/asio.h"
#include "engine/idTable/CompressedIdTableBlockStorage.h"
#include "engine/idTable/IdTable.h"
#include "util/CompressedBlockFile.h"
#include "util/Exception.h"
#include "util/GlobalExecutor.h"
#include "util/MemorySize/MemorySize.h"
#include "util/parallelBlockMerge/MergeOptions.h"

// How the merge phase of a `CompressedExternalIdTableSorter` (see
// `engine/idTable/CompressedExternalIdTable.h`) is configured: the constants
// that it is tuned with, the executor that it runs on by default, the way in
// which it splits the memory limit of its sorter between the two things that
// compete for it, and the storage that it spills its output blocks to. All of
// this is a policy of the *sorter* and not of the merge itself (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`), and it lives in a header of
// its own because it is a self-contained computation that can be tested
// without running a single merge.
namespace ad_utility::compressedExternalIdTable {

// The smallest number of finished output blocks that the merge phase keeps in
// memory per chunk before it starts spilling them to disk, see
// `CompressedIdTableBlockStorage`. The actual number is derived from the memory
// that is left over once the size of the output blocks and the number of
// concurrent chunks are fixed, see `numBufferedOutputBlocksPerChunk`.
constexpr inline size_t MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK = 1;

// The largest number of output blocks that the merge phase keeps in memory per
// chunk, see `numBufferedOutputBlocksPerChunk`. The memory limit is the
// criterion that normally decides this, and this ceiling only bounds the
// bookkeeping (and the per-block overhead of the allocator, which the memory
// accounting does not see) for a caller that pins a very small output block
// size and has a very large memory limit.
constexpr inline size_t MAX_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK = 1024;

// The number of output blocks that a single in-flight chunk of the merge phase
// occupies at the same time: the one that it is currently merging into, the one
// that may be on its way to the spill file, and the
// `numBufferedBlocksPerChunk` that the block storage keeps in memory (see
// above).
constexpr size_t mergePhaseOutputBlocksPerChunk(
    size_t numBufferedBlocksPerChunk) {
  return numBufferedBlocksPerChunk + 2;
}

// The compression that the merge phase applies to the output blocks that it
// spills, see `makeMergePhaseBlockStorageFactory`. A positive value is an
// ordinary ZSTD level (higher compresses better, but costs more CPU), a
// negative value is one of the fast ZSTD levels (`zstd --fast=N`, much cheaper
// and still effective on the long runs of equal `Id`s of sorted columns), `0`
// is the default level of ZSTD (3), and `NO_BLOCK_COMPRESSION` stores the
// blocks uncompressed. The compression competes with the merge for CPU time,
// but with many chunks in flight a large part of the merged data is spilled,
// so a cheap compression that keeps the spill files (and the page cache) small
// pays off. The low positive levels 1 and 2 are a bad choice for this data.
constexpr inline CompressedBlockFile::CompressionLevel
    MERGE_PHASE_SPILL_COMPRESSION = -5;

// The smallest number of rows that an output block of the merge phase may have.
// The number of chunks that are merged concurrently is chosen as large as the
// memory limit allows, but never so large that the output blocks would fall
// below this size, see `computeMergePhaseParameters`.
constexpr inline size_t MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE = 100'000;

// The hard floor for the size of an output block of the merge phase: if not
// even a single chunk leaves room for a block of that many rows, then the merge
// phase gives up and reports that the memory limit is insufficient. Below that
// size the per-block overhead dominates completely.
constexpr inline size_t MIN_USABLE_MERGE_PHASE_OUTPUT_BLOCK_SIZE = 10'000;

// Return the executor that a `CompressedExternalIdTableSorter` merges on if its
// owner does not supply one, which is `ad_utility::globalExecutor()`. This is a
// policy of the sorter, the parallel merge itself has no default executor.
// Concurrent merges may safely share this pool, but a merge must never be
// consumed from a thread of its own executor, see
// `parallelBlockMerge::parallelBlockMergeToRange`.
inline ql::any_io_executor defaultSorterMergeExecutor() {
  return ad_utility::globalExecutor();
}

// Everything that the merge phase of a `CompressedExternalIdTableSorter` has to
// know about that sorter in order to derive its parameters, see
// `computeMergePhaseParameters`.
//
// NOTE: Every member has a default, such that a caller that forgets one gets a
// deterministic (and, for the `parallelism_`, immediately rejected) value
// instead of an indeterminate one.
struct MergePhaseConfig {
  // The number of presorted runs that are merged.
  size_t numRuns_ = 0;
  // The number of columns of the `IdTable` that is sorted.
  size_t numColumns_ = 0;
  // The memory limit of the sorter, which has to cover the input blocks and the
  // output blocks of the merge alike.
  MemorySize memoryLimit_{};
  // The uncompressed size of a single input block, that is of one block of one
  // column of one presorted run.
  MemorySize inputBlockSizePerColumn_{};
  // The number of output blocks that are buffered between the merge and the
  // consumer of the sorted result.
  size_t numBufferedOutputBlocks_ = 0;
  // The upper bound for the memory of a single output block.
  MemorySize maxOutputBlockSize_ = MemorySize::max();
  // The number of threads that the merge phase runs on, which is the largest
  // number of chunks that it makes sense to keep in flight. Must be positive.
  size_t parallelism_ = 0;
  // If set, the size (in rows) of a single output block, which the caller has
  // pinned explicitly. Only the number of concurrent chunks is then derived.
  std::optional<size_t> outputBlockSizeOverride_ = std::nullopt;
  // If true, then the `memoryLimit_` is ignored completely, see
  // `EXTERNAL_ID_TABLE_SORTER_IGNORE_MEMORY_LIMIT_FOR_TESTING`.
  bool ignoreMemoryLimit_ = false;
};

// The numbers that the memory limit has to be split between in the merge phase,
// see `computeMergePhaseParameters`.
struct MergePhaseParameters {
  // The number of rows of a single output block.
  size_t outputBlockSize_;
  // The number of chunks that are merged concurrently.
  size_t numChunksInFlight_;
  // The number of finished output blocks that a single chunk keeps in memory
  // before it starts spilling them to disk, see
  // `numBufferedOutputBlocksPerChunk`.
  size_t numBufferedBlocksPerChunk_ =
      MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK;
};

// Return the number of finished output blocks that a single chunk may keep in
// memory (and therefore does not spill, see `CompressedIdTableBlockStorage`),
// given the `outputBlockSize` (in rows) and the `numChunksInFlight`. This is
// the memory that is left over once those two are fixed, divided evenly among
// the chunks, and clamped to the range between
// `MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK` and
// `MAX_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK`. It only exceeds the
// minimum if the caller has pinned an output block size that leaves part of the
// memory limit unspent.
inline size_t numBufferedOutputBlocksPerChunk(const MergePhaseConfig& config,
                                              size_t outputBlockSize,
                                              size_t numChunksInFlight) {
  AD_CORRECTNESS_CHECK(numChunksInFlight > 0);
  const MemorySize inputMemory = config.numRuns_ * config.numColumns_ *
                                 config.inputBlockSizePerColumn_ *
                                 numChunksInFlight;
  const MemorySize blockMemory =
      MemorySize::bytes(outputBlockSize * config.numColumns_ * sizeof(Id));
  // A block size of zero is rejected by `OutputBlockSize` anyway, and a table
  // without columns has nothing to sort.
  AD_CORRECTNESS_CHECK(blockMemory.getBytes() > 0);
  if (config.ignoreMemoryLimit_ || inputMemory >= config.memoryLimit_) {
    return MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK;
  }
  // The blocks that are not buffered by the chunks: those between the merge and
  // the consumer, and the two per chunk that `mergePhaseOutputBlocksPerChunk`
  // adds on top of the buffered ones.
  const size_t numUnbufferedBlocks =
      config.numBufferedOutputBlocks_ + 2 * numChunksInFlight;
  const size_t numAffordableBlocks =
      (config.memoryLimit_ - inputMemory).getBytes() / blockMemory.getBytes();
  if (numAffordableBlocks <= numUnbufferedBlocks) {
    return MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK;
  }
  return std::clamp(
      (numAffordableBlocks - numUnbufferedBlocks) / numChunksInFlight,
      MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK,
      MAX_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK);
}

// Split the memory limit of the merge phase between the size of the output
// blocks and the number of chunks that are merged concurrently. Each chunk in
// flight holds one decompressed input block per run and column, and on top of
// that come the `numBufferedOutputBlocks_` between the merge and its consumer
// plus `mergePhaseOutputBlocksPerChunk` output blocks per chunk. The strategy
// is to use as many chunks as the memory limit allows without letting the
// output blocks fall below `MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE` rows.
// The `outputBlockSizeOverride_`, if present, pins the output block size, so
// that only the number of chunks is derived. Whatever memory is left over is
// used for buffered output blocks, see `numBufferedOutputBlocksPerChunk`.
//
// NOTE: Smaller output blocks allow more parallelism, but the per-block
// overhead and the input blocks at the chunk boundaries (which are decompressed
// by both adjacent chunks) grow. Keeping a thread free for the sink and the
// spill compression (i.e. fewer chunks than threads) is measurably faster than
// one chunk per thread.
inline MergePhaseParameters computeMergePhaseParameters(
    const MergePhaseConfig& config) {
  AD_CONTRACT_CHECK(config.parallelism_ > 0);
  // One decompressed input block per run, for a single chunk.
  const MemorySize inputMemoryPerChunk =
      config.numRuns_ * config.numColumns_ * config.inputBlockSizePerColumn_;

  // Turn the two numbers that the memory limit is split between into the
  // complete parameters, by deriving how many output blocks a single chunk may
  // additionally keep in memory, see `numBufferedOutputBlocksPerChunk`.
  auto withBufferedBlocks = [&config](size_t outputBlockSize,
                                      size_t numChunksInFlight) {
    return MergePhaseParameters{
        outputBlockSize, numChunksInFlight,
        numBufferedOutputBlocksPerChunk(config, outputBlockSize,
                                        numChunksInFlight)};
  };

  if (config.ignoreMemoryLimit_) {
    // Without a memory limit, let all the chunks that the parallelism offers be
    // in flight, and use the ordinary minimal output block size unless the
    // caller has pinned one.
    //
    // NOTE: Unit tests that want to exercise many small blocks have to pin a
    // small `outputBlockSizeOverride_` themselves.
    return withBufferedBlocks(config.outputBlockSizeOverride_.value_or(
                                  MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE),
                              config.parallelism_);
  }

  // Return the largest number of rows per output block that leaves room for
  // `numInFlight` concurrent chunks, or `std::nullopt` if the input blocks of
  // those chunks alone already exceed the memory limit.
  //
  // NOTE: This deliberately assumes the *minimal* number of buffered blocks per
  // chunk, because it is the size of an output block that is being derived
  // here, and the buffering only gets what that size leaves over.
  auto largestOutputBlockSize = [&config,
                                 inputMemoryPerChunk](size_t numInFlight) {
    const MemorySize inputMemory = inputMemoryPerChunk * numInFlight;
    if (inputMemory >= config.memoryLimit_) {
      return std::optional<size_t>{};
    }
    const size_t numOutputBlocks =
        config.numBufferedOutputBlocks_ +
        mergePhaseOutputBlocksPerChunk(
            MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK) *
            numInFlight;
    const MemorySize perBlock =
        std::min((config.memoryLimit_ - inputMemory) / numOutputBlocks,
                 config.maxOutputBlockSize_);
    return std::optional<size_t>{perBlock.getBytes() /
                                 (sizeof(Id) * config.numColumns_)};
  };

  if (config.outputBlockSizeOverride_.has_value()) {
    // The caller has pinned the size of the output blocks, so only the number
    // of concurrent chunks is left to derive.
    //
    // NOTE: If not even a single chunk fits, then we merge with a single chunk
    // (which is exactly the serial merge) instead of throwing, because the
    // caller has explicitly asked for that block size.
    const size_t numRows = config.outputBlockSizeOverride_.value();
    for (size_t numInFlight = config.parallelism_; numInFlight > 1;
         --numInFlight) {
      if (largestOutputBlockSize(numInFlight) >= numRows) {
        return withBufferedBlocks(numRows, numInFlight);
      }
    }
    return withBufferedBlocks(numRows, 1);
  }

  // Use as much parallelism as the memory limit allows, but never at the
  // price of output blocks that are too small, see above.
  for (size_t numInFlight = config.parallelism_; numInFlight > 1;
       --numInFlight) {
    auto numRows = largestOutputBlockSize(numInFlight);
    if (numRows >= MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE) {
      return withBufferedBlocks(numRows.value(), numInFlight);
    }
  }
  // Not even two chunks leave room for a reasonably sized output block, so
  // merge with a single chunk and give it everything that is left.
  auto numRows = largestOutputBlockSize(1);
  if (numRows <= MIN_USABLE_MERGE_PHASE_OUTPUT_BLOCK_SIZE) {
    throw std::runtime_error{
        absl::StrCat("Insufficient memory for merging ", config.numRuns_,
                     " blocks. Please increase the memory settings")};
  }
  return withBufferedBlocks(numRows.value(), 1);
}

// Turn the `parameters` that `computeMergePhaseParameters` has derived into the
// options of the parallel merge.
inline parallelBlockMerge::MergeOptions makeMergeOptions(
    const MergePhaseConfig& config, const MergePhaseParameters& parameters) {
  parallelBlockMerge::MergeOptions options;
  // The number of rows is the only criterion for finishing an output block,
  // exactly as it was before the merge phase was parallelized.
  options.outputBlockSize = parallelBlockMerge::OutputBlockSize::numElements(
      parameters.outputBlockSize_);
  options.parallelismHint = config.parallelism_;
  options.maxNumChunksInFlight = parameters.numChunksInFlight_;
  return options;
}

// The common prefix of the names of the files that a single merge phase spills
// its output blocks to. Every chunk gets a file of its own below that prefix,
// see `CompressedIdTableBlockStorage::spillFilename`.
//
// NOTE: The prefix has to be unique per merge phase, because the storage of a
// previous merge phase may still be alive when the next one starts, and it
// deletes the files that it holds. The `sorterFilename` is what makes it unique
// among concurrent sorters, and the `mergePhaseIndex` (the number of merge
// phases that the sorter has started so far) among the merge phases of one and
// the same sorter.
inline std::string makeSpillFilename(const std::string& sorterFilename,
                                     size_t mergePhaseIndex) {
  return absl::StrCat(sorterFilename, ".merge-spill.", mergePhaseIndex);
}

// The factory for the intermediate storage of the output blocks of the merge
// phase, see the `parallelBlockMerge::BlockStorageConcept`. The blocks are
// spilled to a temporary file of their own, so that a chunk that has run far
// ahead of the consumer can be merged to completion instead of suspending its
// producer. A suspended producer would hold on to its slot among the chunks
// that are in flight, which is the scarce resource of the merge phase (see
// `computeMergePhaseParameters`). As long as the consumer keeps up, no block is
// ever written, see `CompressedIdTableBlockStorage`, and neither is one written
// while a chunk can still buffer it, which is what
// `numBufferedBlocksPerChunk` (from
// `MergePhaseParameters::numBufferedBlocksPerChunk_`) decides.
//
// NOTE: That storage is only ever used by the coroutine-based sink and hence
// does not exist in the C++17 backports mode, where
// `parallelBlockMergeToRange` merges serially and ignores the factory
// altogether (see there). A placeholder therefore suffices in that mode.
template <size_t NumCols>
auto makeMergePhaseBlockStorageFactory(
    ql::any_io_executor ioExecutor, std::string spillFilenamePrefix,
    AllocatorWithLimit<Id> allocator,
    size_t numBufferedBlocksPerChunk =
        MIN_MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK,
    CompressedBlockFile::CompressionLevel compression =
        MERGE_PHASE_SPILL_COMPRESSION) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  (void)ioExecutor, (void)spillFilenamePrefix, (void)allocator,
      (void)numBufferedBlocksPerChunk, (void)compression;
  return std::monostate{};
#else
  return makeCompressedIdTableStorageFactory<NumCols>(
      std::move(ioExecutor), std::move(spillFilenamePrefix),
      std::move(allocator), numBufferedBlocksPerChunk, compression);
#endif
}

}  // namespace ad_utility::compressedExternalIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_EXTERNALIDTABLESORTERMERGECONFIG_H
