// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEOPTIONS_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEOPTIONS_H

#include <cstddef>

#include "util/MemorySize/MemorySize.h"

// The tuning knobs of the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`) together with their defaults.
// These are shared configuration and not a policy, which is why they live in a
// header of their own.
namespace ad_utility::parallelBlockMerge {

// The default number of elements in a single output block of the merge.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE = 100'000;

// The default upper bound for the memory that a single output block of the
// merge may occupy. An output block is finished as soon as either this limit or
// `DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE` is reached.
constexpr inline MemorySize DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY =
    MemorySize::megabytes(16);

// The default number of chunks that are created per available thread. Values
// greater than one lead to a finer granularity, which in turn improves the load
// balancing if the individual chunks require different amounts of work.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD = 4;

// The default number of input elements below which the merge is performed
// serially. For small inputs the overhead of setting up the parallel merge
// dominates the actual merging.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD =
    100'000;

// The tuning knobs of the parallel merge. All of them have sensible defaults,
// so that a caller typically only has to set the values it actually cares
// about.
struct MergeOptions {
  // Emit an output block as soon as it contains that many elements.
  size_t outputBlockSize = DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE;

  // Emit an output block as soon as it occupies that much memory (according to
  // `BlockedRunsInput::memorySizeOfElement`), even if it contains fewer than
  // `outputBlockSize` elements.
  MemorySize maxOutputBlockMemory = DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY;

  // Aim for that many independent chunks per thread. Larger values improve the
  // load balancing at the cost of a larger scheduling overhead.
  size_t targetChunksPerThread = DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD;

  // Never keep more than that many chunks in flight at the same time. The value
  // `0` means "as many as the scheduler offers", that is
  // `MergeScheduler::maxParallelism()`.
  size_t maxInFlightChunks = 0;

  // Merge serially (that is, without involving the scheduler at all) if the
  // input has at most that many runs.
  size_t serialNumRunsThreshold = 2;

  // Merge serially if the input has at most that many elements in total.
  size_t serialNumElementsThreshold =
      DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD;

  // Buffer at most that many finished output blocks per chunk before the
  // producing worker of that chunk is blocked. This is the back-pressure that
  // bounds the memory consumption of the merge.
  size_t bufferedBlocksPerChunk = 2;

  // Break ties (that is, elements that the comparator considers equal) by the
  // index of the run, which makes the tie order identical for every number of
  // chunks, at the cost of up to twice as many calls to the comparator. Leave
  // this off unless you actually need that property, because the result is
  // deterministic for a fixed configuration either way.
  bool stableTieBreaking = false;
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEOPTIONS_H
