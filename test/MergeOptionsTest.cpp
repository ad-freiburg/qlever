// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <thread>

#include "util/GTestHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"

using namespace ad_utility::parallelBlockMerge;
using ad_utility::MemorySize;

// _____________________________________________________________________________
TEST(MergeOptions, Defaults) {
  MergeOptions options;
  EXPECT_EQ(options.outputBlockSize.maxNumElements(),
            DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE);
  EXPECT_EQ(options.outputBlockSize.maxMemory(),
            DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY);
  EXPECT_EQ(options.parallelismHint, 0u);
  EXPECT_EQ(options.targetChunksPerThread,
            DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD);
  EXPECT_EQ(options.maxNumChunksInFlight, 0u);
}

// _____________________________________________________________________________
TEST(MergeOptions, DefaultParallelismIsAtLeastOne) {
  EXPECT_GE(defaultMergeParallelism(), 1u);
  EXPECT_EQ(defaultMergeParallelism(),
            std::max<size_t>(1, std::thread::hardware_concurrency()));
}

// _____________________________________________________________________________
TEST(MergeOptions, ParallelismHintOfZeroMeansTheDefault) {
  MergeOptions options;
  EXPECT_EQ(options.parallelism(), defaultMergeParallelism());
  // Every other value is used as it is, also one that far exceeds the hardware,
  // because the hint only affects the scheduling and never the correctness.
  options.parallelismHint = 1;
  EXPECT_EQ(options.parallelism(), 1u);
  options.parallelismHint = 4;
  EXPECT_EQ(options.parallelism(), 4u);
  options.parallelismHint = 1000;
  EXPECT_EQ(options.parallelism(), 1000u);
}

// _____________________________________________________________________________
TEST(MergeOptions, TargetNumChunks) {
  MergeOptions options;
  options.parallelismHint = 4;
  EXPECT_EQ(options.targetNumChunks(),
            4 * DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD);
  // One chunk per thread is the coarsest sensible granularity.
  options.targetChunksPerThread = 1;
  EXPECT_EQ(options.targetNumChunks(), 4u);
  options.targetChunksPerThread = 7;
  EXPECT_EQ(options.targetNumChunks(), 28u);
  // The `parallelismHint` of zero is resolved first, see `parallelism()`.
  options.parallelismHint = 0;
  EXPECT_EQ(options.targetNumChunks(), 7 * defaultMergeParallelism());
}

// _____________________________________________________________________________
TEST(MergeOptions, NumChunksInFlight) {
  MergeOptions options;
  options.parallelismHint = 4;
  // The value `0` means "as many as `parallelism()`".
  EXPECT_EQ(options.numChunksInFlight(100), 4u);
  // Every other value is used as it is, also one that far exceeds the
  // parallelism, because a chunk that has to wait suspends instead of blocking
  // a thread.
  options.maxNumChunksInFlight = 1;
  EXPECT_EQ(options.numChunksInFlight(100), 1u);
  options.maxNumChunksInFlight = 6;
  EXPECT_EQ(options.numChunksInFlight(100), 6u);
  // The result is never greater than the number of chunks that actually exist,
  // no matter whether the bound is explicit or derived from the parallelism.
  options.maxNumChunksInFlight = 1000;
  EXPECT_EQ(options.numChunksInFlight(100), 100u);
  EXPECT_EQ(options.numChunksInFlight(1), 1u);
  options.maxNumChunksInFlight = 0;
  EXPECT_EQ(options.numChunksInFlight(2), 2u);
  EXPECT_EQ(options.numChunksInFlight(1), 1u);
}

// _____________________________________________________________________________
TEST(MergeOptions, OutputBlockSizeOnlyNumElementsMatters) {
  auto size = OutputBlockSize::numElements(3);
  EXPECT_EQ(size.maxNumElements(), 3u);
  EXPECT_EQ(size.maxMemory(), MemorySize::max());
  EXPECT_FALSE(size.isBlockLargeEnough(2, MemorySize::terabytes(1)));
  EXPECT_TRUE(size.isBlockLargeEnough(3, MemorySize::bytes(0)));
  EXPECT_TRUE(size.isBlockLargeEnough(4, MemorySize::bytes(0)));
}

// _____________________________________________________________________________
TEST(MergeOptions, OutputBlockSizeOnlyMemoryMatters) {
  auto size = OutputBlockSize::memory(MemorySize::bytes(100));
  EXPECT_EQ(size.maxNumElements(), std::numeric_limits<size_t>::max());
  EXPECT_EQ(size.maxMemory(), MemorySize::bytes(100));
  EXPECT_FALSE(size.isBlockLargeEnough(1'000'000, MemorySize::bytes(99)));
  EXPECT_TRUE(size.isBlockLargeEnough(0, MemorySize::bytes(100)));
  EXPECT_TRUE(size.isBlockLargeEnough(0, MemorySize::bytes(101)));
}

// _____________________________________________________________________________
TEST(MergeOptions, OutputBlockSizeBothMatter) {
  auto size = OutputBlockSize::both(3, MemorySize::bytes(100));
  EXPECT_EQ(size.maxNumElements(), 3u);
  EXPECT_EQ(size.maxMemory(), MemorySize::bytes(100));
  EXPECT_FALSE(size.isBlockLargeEnough(2, MemorySize::bytes(99)));
  // Either of the two criteria is sufficient.
  EXPECT_TRUE(size.isBlockLargeEnough(3, MemorySize::bytes(99)));
  EXPECT_TRUE(size.isBlockLargeEnough(2, MemorySize::bytes(100)));
}

// _____________________________________________________________________________
TEST(MergeOptions, OutputBlockSizeMustNotBeSatisfiedByAnEmptyBlock) {
  // Both limits have to be strictly positive, because the merge relies on an
  // empty output block never being large enough.
  AD_EXPECT_THROW_WITH_MESSAGE(OutputBlockSize::numElements(0),
                               ::testing::HasSubstr("maxNumElements > 0"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      OutputBlockSize::memory(MemorySize::bytes(0)),
      ::testing::HasSubstr("maxMemory > MemorySize::bytes(0)"));
  AD_EXPECT_THROW_WITH_MESSAGE(OutputBlockSize::both(0, MemorySize::bytes(1)),
                               ::testing::HasSubstr("maxNumElements > 0"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      OutputBlockSize::both(1, MemorySize::bytes(0)),
      ::testing::HasSubstr("maxMemory > MemorySize::bytes(0)"));
  // The smallest valid block size is a single element.
  auto size = OutputBlockSize::both(1, MemorySize::bytes(1));
  EXPECT_FALSE(size.isBlockLargeEnough(0, MemorySize::bytes(0)));
  EXPECT_TRUE(size.isBlockLargeEnough(1, MemorySize::bytes(0)));
}
