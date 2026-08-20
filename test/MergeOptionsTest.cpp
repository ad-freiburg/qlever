// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <limits>

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
  EXPECT_EQ(options.targetChunksPerThread,
            DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD);
  EXPECT_EQ(options.maxInFlightChunks, 0u);
  EXPECT_EQ(options.serialNumElementsThreshold,
            DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD);
  EXPECT_EQ(options.bufferedBlocksPerChunk, 2u);
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
