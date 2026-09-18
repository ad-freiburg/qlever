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
#include <boost/asio/any_io_executor.hpp>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

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

// The number of finished output blocks that the merge phase keeps in memory per
// chunk before it starts spilling them to disk, see
// `CompressedIdTableBlockStorage`.
constexpr inline size_t MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK = 1;

// The number of output blocks that a single in-flight chunk of the merge phase
// occupies at the same time: the one that it is currently merging into, the one
// that may be on its way to the spill file, and the ones that the block storage
// keeps in memory (see above).
constexpr inline size_t MERGE_PHASE_OUTPUT_BLOCKS_PER_CHUNK =
    MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK + 2;

// The compression that the merge phase applies to the output blocks that it
// spills, see `makeMergePhaseBlockStorageFactory`. In contrast to the presorted
// runs, this file is short-lived and every block is read back almost
// immediately, so the compression competes with the merge itself for CPU. It
// cannot simply be turned off, though (which `NO_BLOCK_COMPRESSION` would do),
// because roughly 85 % of everything that is merged is written to these files
// and read back again. Their peak size is bounded, because each chunk has a
// file of its own that is deleted as soon as that chunk has been consumed, but
// the *bytes* still go through the page cache and are therefore paid for in
// memory bandwidth, and on a machine with less RAM in real disk I/O. A
// *negative* ZSTD level (`zstd --fast=5`) wins on both counts.
//
// The wall time of the merge phase, for 48M rows of 4 columns in 16 presorted
// runs with a memory limit of 192 MB, on a 16-core Ryzen 9 7950X with an NVMe
// RAID and 128 GB of RAM. NOTE: This table was measured while all the runs
// still shared a single spill file, so its absolute numbers are the ones of
// that scheme; it is the *ordering* of the levels that it is evidence for, see
// below for the current numbers.
//
//   level | realistic Ids (9x) | uniformly random Ids (1.5x)
//         | 16 thr      8 thr  | 16 thr      8 thr
//   ------+--------------------+---------------------------
//       3 | 0.58 s     0.86 s  | 1.27 s     1.65 s
//       1 | 0.81 s     1.01 s  | 1.06 s     1.23 s
//      -5 | 0.48 s     0.70 s  | 0.66 s     0.83 s
//    none | 0.62 s     0.69 s  | 0.62 s     0.69 s
//
// Note that the low *positive* levels are the worst of both worlds: they cost
// more CPU than level 3 and do not compress better. The reason is that the
// columns of a sorted output block consist of long runs of equal `Id`s, which
// the `ZSTD_dfast` strategy of level 3 skips over almost for free, while the
// `ZSTD_fast` strategy of levels 1 and 2 pays its per-byte price everywhere.
// The negative levels use `ZSTD_fast` as well, but with an acceleration that
// makes that per-byte price small, and the runs are so long that they are
// found anyway: at level -5 the realistic data still compresses 5.7x, and
// `libzstd` drops from 43 % of the profile (level 3) to 34 %.
//
// Since every chunk spills to a file of its own that is deleted as soon as that
// chunk has been consumed, the choice against `NO_BLOCK_COMPRESSION` is a
// closer call than it used to be. At 16 threads today, level -5 takes 0.48 s
// against 0.50 s and keeps the files five times smaller (48 MB against 267 MB
// of peak total size), which is what keeps the page cache out of the way:
// inside a 1 GB cgroup, level -5 peaks at 588 MB of that budget and
// `NO_BLOCK_COMPRESSION` at 788 MB, and neither writes a single byte to the
// device anymore (before the files were reclaimed per chunk,
// `NO_BLOCK_COMPRESSION` wrote ~1 GB there and took 0.69 s).
//
// So compressing is kept for the data that a real index build produces. On
// uniformly random `Id`s, where the compression buys almost nothing,
// `NO_BLOCK_COMPRESSION` is in fact about 8 % faster (0.55 s against 0.59 s),
// which is a trade this default deliberately declines.
constexpr inline CompressedBlockFile::CompressionLevel
    MERGE_PHASE_SPILL_COMPRESSION = -5;

// The smallest number of rows that an output block of the merge phase may have.
// The number of chunks that are merged concurrently is chosen as large as the
// memory limit allows, but never so large that the output blocks would fall
// below this size, see `computeMergePhaseParameters`.
constexpr inline size_t MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE = 100'000;

// The size (in elements) of the first chunk of the merge phase, and the number
// of chunks over which that size is doubled: the leading chunks of the merge
// have 1M, 2M, 4M, 8M and 16M elements, and all the following ones have the
// uniform size that the parallelism implies (see
// `parallelBlockMerge::MergeOptions::firstChunkSizes`).
//
// The consumer of the merge has to drain the chunks in the order of their
// index, so the very first sorted rows are only available once the first chunk
// has produced its first output block. Small leading chunks make that happen
// much sooner, while the doubling makes sure that the ramp-up is over after a
// negligible fraction of a large input and the merge then runs with the large
// chunks that give it its throughput. Leading sizes that are not smaller than a
// uniform chunk are ignored, so small inputs are unaffected.
constexpr inline size_t FIRST_MERGE_PHASE_CHUNK_SIZE = 1'000'000;
constexpr inline size_t NUM_RAMPED_UP_MERGE_PHASE_CHUNKS = 5;

// The sizes of the leading chunks of the merge phase, see
// `FIRST_MERGE_PHASE_CHUNK_SIZE`.
inline std::vector<size_t> mergePhaseFirstChunkSizes() {
  std::vector<size_t> sizes;
  sizes.reserve(NUM_RAMPED_UP_MERGE_PHASE_CHUNKS);
  size_t size = FIRST_MERGE_PHASE_CHUNK_SIZE;
  for (size_t i = 0; i < NUM_RAMPED_UP_MERGE_PHASE_CHUNKS; ++i) {
    sizes.push_back(size);
    size *= 2;
  }
  return sizes;
}

// The hard floor for the size of an output block of the merge phase: if not
// even a single chunk leaves room for a block of that many rows, then the merge
// phase gives up and reports that the memory limit is insufficient. Below that
// size the per-block overhead dominates completely.
constexpr inline size_t MIN_USABLE_MERGE_PHASE_OUTPUT_BLOCK_SIZE = 10'000;

// Return the executor of the process-wide default thread pool of the merge
// phase, which is the global thread pool of QLever (see
// `ad_utility::globalExecutor()`, which also documents the size and the
// lifetime of that pool). It is what a `CompressedExternalIdTableSorter` merges
// on if its owner does not supply an executor of its own, see
// `CompressedExternalIdTableSorter::setMergeExecutor`.
//
// NOTE: The parallel merge itself deliberately has no default executor, so that
// every caller stays in control of the threads that its merges run on (see
// `parallelBlockMerge::parallelBlockMergeToSink`). This pool is therefore a
// policy of the *sorter* and not of the merge: the sorter is created in many
// places that have no thread pool of their own, and merging its runs serially
// would make the merge phase the bottleneck of the index build.
//
// Sharing a single pool between concurrent merge phases is safe, because a
// chunk that cannot make progress suspends instead of occupying its thread, so
// the merges cannot starve each other. It is however *not* safe to consume a
// merge from one of the threads of its own executor, see
// `parallelBlockMerge::parallelBlockMergeToRange`.
inline boost::asio::any_io_executor defaultSorterMergeExecutor() {
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
  MemorySize inputBlockSize_{};
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

// The two numbers that the memory limit has to be split between in the merge
// phase, see `computeMergePhaseParameters`.
struct MergePhaseParameters {
  // The number of rows of a single output block.
  size_t outputBlockSize_;
  // The number of chunks that are merged concurrently.
  size_t numChunksInFlight_;
};

// Split the memory limit between the size of the output blocks and the number
// of chunks that are merged concurrently. The
// `MergePhaseConfig::outputBlockSizeOverride_`, if present, pins the former, so
// that only the latter is derived.
//
// The memory of the merge phase consists of two parts. Each chunk that is in
// flight holds one decompressed input block per run, which is by far the
// larger part and the reason why the number of concurrent chunks is bounded
// at all. On top of that come the output blocks: those in the pipeline
// between the merge and the caller (`numBufferedOutputBlocks_`), plus
// `MERGE_PHASE_OUTPUT_BLOCKS_PER_CHUNK` for every chunk that is in flight.
//
// The strategy is to use as much parallelism as the memory limit allows, but
// never at the price of output blocks below `MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE`
// rows: with many small blocks the per-block overhead, the synchronization in
// the sink, and the input blocks at the chunk boundaries (which are
// decompressed by both of the adjacent chunks) start to dominate. That minimum
// is the only free parameter of the formula, and the following measurements are
// the reason for its value. They are for 48 million rows in 16 runs of 4
// columns each with a memory limit of 192 MB, merged by 16 threads on a machine
// with 16 cores (32 hardware threads), relative to the serial merge of the same
// data (which takes 3.2 seconds), see
// `benchmark/ParallelBlockMergeBenchmark.cpp`:
//
//   minimum    output block    chunks in flight   speedup
//     50'000    86538 rows     16                 4.5x
//    100'000   101902 rows     14                 5.7x
//    150'000   166330 rows      9                 4.6x
//    250'000   291118 rows      5                 3.7x
//    500'000   581250 rows      2                 1.7x
//   1'000'000  843750 rows      1                 0.9x (the serial merge)
//
// The optimum is a flat plateau between 90'000 and 110'000 rows (13 to 15
// concurrent chunks), and the chosen value sits in the middle of it. The
// table was measured before `MERGE_PHASE_SPILL_COMPRESSION` was lowered,
// which made every row of it faster (the chosen one reaches 6.6x today), but
// the optimum stayed where it is. Note that letting *all* 16 chunks be in
// flight (which the minimum of 50'000 does) is measurably worse, but not
// because of the memory bandwidth: both variants move the same 16 GB through
// the memory controllers, and the faster one utilizes the data bus by 46 %,
// against the 80 % that a pure streaming kernel reaches on this machine. What
// the 16-chunk variant runs out of is threads: with one chunk per thread, none
// is left to run the handlers of the sink and the compression of the output
// blocks that `makeMergePhaseBlockStorageFactory` spills, so the aggregate busy
// time of all cores drops by 13 %. A pool of 24 threads instead of 16 removes
// most of the difference (0.61 s instead of 0.72 s), so the minimum above
// effectively keeps a chunk slot free for that bookkeeping.
//
// The reason why the size of an output block matters this much is the spill of
// `makeMergePhaseBlockStorageFactory`: a chunk keeps only
// `MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK` of its output blocks in
// memory and compresses the rest, so with eight blocks per chunk 85 % of all
// output bytes are compressed, written, read back and decompressed again.
// Making that spill cheap is therefore worth as much as this whole formula,
// see `MERGE_PHASE_SPILL_COMPRESSION`. With it, realistic data is within 4 %
// of the merge that does not spill at all (0.47 s against 0.45 s), so there
// is nothing left to gain there.
//
// TODO<joka921> Uniformly distributed `Id`s are the case that is still far
// off: they compress much worse, so they spill 1.2 GB instead of 0.23 GB and
// reach 0.66 s against the same 0.47 s. Buffering more than one output block
// per chunk would spill less, at the price of chunk slots (see
// `MERGE_PHASE_OUTPUT_BLOCKS_PER_CHUNK`). That trade was never measured, and
// it only matters for data that a real index build does not produce.
//
// NOTE: Before the output blocks were spilled to disk, a chunk that had run
// ahead of the consumer suspended while holding on to its slot, so the
// effective parallelism was whatever the consumer happened to allow, and the
// accounting also undercounted the output blocks that the sink buffered per
// chunk. The old formula spent almost the whole memory limit on a single output
// block (1476562 rows and 3 concurrent chunks here) and reached only 2.9x. Note
// that the new formula also makes the *serial* merge (a single chunk with
// 843750-row blocks) 13% faster, because its output blocks got smaller.
inline MergePhaseParameters computeMergePhaseParameters(
    const MergePhaseConfig& config) {
  AD_CONTRACT_CHECK(config.parallelism_ > 0);
  // One decompressed input block per run, for a single chunk.
  const MemorySize inputMemoryPerChunk =
      config.numRuns_ * config.numColumns_ * config.inputBlockSize_;

  if (config.ignoreMemoryLimit_) {
    // For unit tests, always yield 5 rows at once, and let all the chunks
    // that the parallelism offers be in flight. Deriving either number from
    // the (deliberately tiny) memory limit of such a test would always
    // collapse the merge to a single chunk, and the parallel code path would
    // never be exercised.
    return {config.outputBlockSizeOverride_.value_or(5), config.parallelism_};
  }

  // Return the largest number of rows per output block that leaves room for
  // `numInFlight` concurrent chunks, or `std::nullopt` if the input blocks of
  // those chunks alone already exceed the memory limit.
  auto largestOutputBlockSize = [&config,
                                 inputMemoryPerChunk](size_t numInFlight) {
    const MemorySize inputMemory = inputMemoryPerChunk * numInFlight;
    if (inputMemory >= config.memoryLimit_) {
      return std::optional<size_t>{};
    }
    const size_t numOutputBlocks =
        config.numBufferedOutputBlocks_ +
        MERGE_PHASE_OUTPUT_BLOCKS_PER_CHUNK * numInFlight;
    const MemorySize perBlock =
        std::min((config.memoryLimit_ - inputMemory) / numOutputBlocks,
                 config.maxOutputBlockSize_);
    return std::optional<size_t>{perBlock.getBytes() /
                                 (sizeof(Id) * config.numColumns_)};
  };

  // Return `true` if `numInFlight` concurrent chunks with output blocks of
  // `numRows` rows each fit into the memory limit.
  auto fits = [&largestOutputBlockSize](size_t numInFlight, size_t numRows) {
    auto largest = largestOutputBlockSize(numInFlight);
    return largest.has_value() && largest.value() >= numRows;
  };

  if (config.outputBlockSizeOverride_.has_value()) {
    // The caller has pinned the size of the output blocks, so only the number
    // of concurrent chunks is left to derive. NOTE: If not even a single
    // chunk fits, then we merge with a single chunk (which is exactly the
    // serial merge) instead of throwing, because the caller has explicitly
    // asked for that block size.
    const size_t numRows = config.outputBlockSizeOverride_.value();
    for (size_t numInFlight = config.parallelism_; numInFlight > 1;
         --numInFlight) {
      if (fits(numInFlight, numRows)) {
        return {numRows, numInFlight};
      }
    }
    return {numRows, 1};
  }

  // Use as much parallelism as the memory limit allows, but never at the
  // price of output blocks that are too small, see above.
  for (size_t numInFlight = config.parallelism_; numInFlight > 1;
       --numInFlight) {
    auto numRows = largestOutputBlockSize(numInFlight);
    if (numRows.has_value() &&
        numRows.value() >= MIN_MERGE_PHASE_OUTPUT_BLOCK_SIZE) {
      return {numRows.value(), numInFlight};
    }
  }
  // Not even two chunks leave room for a reasonably sized output block, so
  // merge with a single chunk and give it everything that is left.
  auto numRows = largestOutputBlockSize(1);
  if (!numRows.has_value() ||
      numRows.value() <= MIN_USABLE_MERGE_PHASE_OUTPUT_BLOCK_SIZE) {
    throw std::runtime_error{
        absl::StrCat("Insufficient memory for merging ", config.numRuns_,
                     " blocks. Please increase the memory settings")};
  }
  return {numRows.value(), 1};
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
  options.firstChunkSizes = mergePhaseFirstChunkSizes();
  // Two of the buffered output blocks are the one that the consumer currently
  // holds and the one that the merge is just finishing, so all the others are
  // read ahead, see `MergePhaseConfig::numBufferedOutputBlocks_`.
  options.numPrefetchedOutputBlocks = config.numBufferedOutputBlocks_ >= 3
                                          ? config.numBufferedOutputBlocks_ - 2
                                          : 1;
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
// ever written, see `CompressedIdTableBlockStorage`.
//
// NOTE: That storage is only ever used by the coroutine-based sink and hence
// does not exist in the C++17 backports mode, where
// `parallelBlockMergeToRange` merges serially and ignores the factory
// altogether (see there). A placeholder therefore suffices in that mode.
template <size_t NumCols>
auto makeMergePhaseBlockStorageFactory(
    [[maybe_unused]] boost::asio::any_io_executor ioExecutor,
    [[maybe_unused]] std::string spillFilenamePrefix,
    [[maybe_unused]] AllocatorWithLimit<Id> allocator,
    [[maybe_unused]] CompressedBlockFile::CompressionLevel compression =
        MERGE_PHASE_SPILL_COMPRESSION) {
#ifdef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
  return std::monostate{};
#else
  return makeCompressedIdTableStorageFactory<NumCols>(
      std::move(ioExecutor), std::move(spillFilenamePrefix),
      std::move(allocator), MERGE_PHASE_BUFFERED_OUTPUT_BLOCKS_PER_CHUNK,
      compression);
#endif
}

}  // namespace ad_utility::compressedExternalIdTable

#endif  // QLEVER_SRC_ENGINE_IDTABLE_EXTERNALIDTABLESORTERMERGECONFIG_H
