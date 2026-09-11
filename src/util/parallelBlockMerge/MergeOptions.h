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

#include <algorithm>
#include <cstddef>
#include <limits>
#include <thread>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

// The tuning knobs of the block merge together with their defaults. These are
// shared configuration and not a policy, which is why they live in a header of
// their own. For the terminology (runs, blocks, and chunks) see
// `util/parallelBlockMerge/ParallelBlockMerge.h`, which is the header to read
// first.
namespace ad_utility::parallelBlockMerge {

// The default number of elements in a single output block of the merge.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE = 100'000;

// The default upper bound for the memory that a single output block of the
// merge may occupy. An output block is finished as soon as either this limit or
// `DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE` is reached.
constexpr inline MemorySize DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY =
    MemorySize::megabytes(1);

// The default number of chunks that are created per available thread. Values
// greater than one lead to a finer granularity, which in turn improves the load
// balancing if the individual chunks require different amounts of work.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD = 4;

// The default number of input elements below which the merge is performed
// serially. For small inputs the overhead of setting up the parallel merge
// dominates the actual merging.
constexpr inline size_t DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD =
    100'000;

// Return the parallelism that a merge assumes if its `MergeOptions` do not
// specify one, which is one thread per hardware thread. NOTE: This is a pure
// tuning default and says nothing about the executor that a merge actually
// runs on; that executor is always supplied (and owned) by the caller, see
// `parallelBlockMergeToSink`.
inline size_t defaultMergeParallelism() {
  return std::max<size_t>(1, std::thread::hardware_concurrency());
}

// The criterion for when a single output block of the merge is complete. A
// block is finished as soon as it either contains a given number of elements or
// occupies a given amount of memory. Use the named constructors below to
// express which of the two criteria actually matter for a given caller; a
// criterion that is not specified is simply never the reason for finishing a
// block.
class OutputBlockSize {
 private:
  size_t maxNumElements_;
  MemorySize maxMemory_;

 public:
  // Finish a block after `numElements` elements, no matter how much memory it
  // occupies (that is, only the number of elements matters).
  static OutputBlockSize numElements(size_t numElements) {
    return {numElements, MemorySize::max()};
  }

  // Finish a block as soon as it occupies `memory`, no matter how many elements
  // it contains (that is, only the memory matters).
  static OutputBlockSize memory(MemorySize memory) {
    return {std::numeric_limits<size_t>::max(), memory};
  }

  // Finish a block as soon as either of the two limits is reached (that is,
  // both criteria matter).
  static OutputBlockSize both(size_t numElements, MemorySize memory) {
    return {numElements, memory};
  }

  // Return `true` if a block that contains `numElements` elements and occupies
  // `memory` is complete and should be emitted. This is never `true` for an
  // empty block, see the constructor below.
  bool isBlockLargeEnough(size_t numElements, MemorySize memory) const {
    return numElements >= maxNumElements_ || memory >= maxMemory_;
  }

  // The two limits, mostly for testing and for logging.
  size_t maxNumElements() const { return maxNumElements_; }
  MemorySize maxMemory() const { return maxMemory_; }

 private:
  // The general constructor, only reachable via the named constructors above,
  // so that a call site always states which criteria it cares about.
  //
  // NOTE: Both limits have to be strictly positive, because consumers of the
  // merge rely on an empty block never being large enough (an output block that
  // is complete while still being empty would never make any progress).
  OutputBlockSize(size_t maxNumElements, MemorySize maxMemory)
      : maxNumElements_{maxNumElements}, maxMemory_{maxMemory} {
    AD_CONTRACT_CHECK(maxNumElements > 0);
    AD_CONTRACT_CHECK(maxMemory > MemorySize::bytes(0));
  }
};

// The tuning knobs of the merge. All of them have sensible defaults, so that a
// caller typically only has to set the values it actually cares about.
struct MergeOptions {
  // When to finish a single output block, see `OutputBlockSize`.
  OutputBlockSize outputBlockSize =
      OutputBlockSize::both(DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE,
                            DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY);

  // The remaining knobs only affect a merge that actually distributes its
  // chunks over several threads, see `parallelBlockMergeToSink`. The serial
  // merge ignores all of them.

  // The number of threads that are expected to run the executor of the merge.
  // The value `0` means "as many as the hardware offers", see
  // `defaultMergeParallelism()`.
  //
  // NOTE: This is only a hint, and never a promise or a requirement: it is used
  // exclusively to derive `targetNumChunks()` and `numChunksInFlight()` below,
  // both of which may safely exceed the parallelism that the executor actually
  // provides. A merge is correct for every value, it is only its scheduling
  // that becomes suboptimal if the value is far off.
  size_t parallelismHint = 0;

  // Aim for that many independent chunks per thread. Larger values improve the
  // load balancing at the cost of a larger scheduling overhead.
  size_t targetChunksPerThread = DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD;

  // Never keep more than that many chunks in flight at the same time. The value
  // `0` means "as many as `parallelism()`".
  size_t maxNumChunksInFlight = 0;

  // Return the number of threads that the merge assumes, that is the
  // `parallelismHint` with the value `0` resolved to its default.
  size_t parallelism() const {
    return parallelismHint == 0 ? defaultMergeParallelism() : parallelismHint;
  }

  // Return the number of chunks that the merge should be split into, see
  // `computeChunkBoundaries`. This is only a target: the actual number of
  // chunks may be smaller, for example because the input has fewer elements
  // than that.
  size_t targetNumChunks() const {
    return parallelism() * targetChunksPerThread;
  }

  // Return the number of chunks that may be merged concurrently, given the
  // `numChunks` that the merge actually consists of. Never zero, and never
  // greater than `numChunks`, because a chunk that is in flight but does not
  // exist would only waste a permit of the semaphore that enforces this bound.
  size_t numChunksInFlight(size_t numChunks) const {
    size_t requestedNumChunksInFlight =
        maxNumChunksInFlight == 0 ? parallelism() : maxNumChunksInFlight;
    // NOTE: The number of in-flight chunks is deliberately *not* bounded by the
    // available parallelism, because a chunk that has to wait suspends instead
    // of blocking a thread. A single in-flight chunk is legal as well.
    return std::min(requestedNumChunksInFlight, numChunks);
  }

  // Merge serially in the calling thread if the input has at most that many
  // elements in total. Only `parallelBlockMergeToRange` looks at this, see
  // there.
  size_t serialNumElementsThreshold =
      DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD;

  // Buffer at most that many finished output blocks per chunk. Only the
  // `InOrderBlockSink` looks at this, and only if its blocks live in memory, in
  // which case it is the back-pressure that bounds the memory consumption of
  // the merge.
  size_t bufferedBlocksPerChunk = 2;
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEOPTIONS_H
