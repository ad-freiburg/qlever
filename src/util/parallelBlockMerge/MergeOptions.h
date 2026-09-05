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
#include <limits>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"

// The tuning knobs of the block merge (see
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
};

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_MERGEOPTIONS_H
