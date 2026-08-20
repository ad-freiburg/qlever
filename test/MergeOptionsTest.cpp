// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "util/parallelBlockMerge/MergeOptions.h"

using namespace ad_utility::parallelBlockMerge;

// _____________________________________________________________________________
TEST(MergeOptions, Defaults) {
  MergeOptions options;
  EXPECT_EQ(options.outputBlockSize, DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_SIZE);
  EXPECT_EQ(options.maxOutputBlockMemory,
            DEFAULT_PARALLEL_MERGE_OUTPUT_BLOCK_MEMORY);
  EXPECT_EQ(options.targetChunksPerThread,
            DEFAULT_PARALLEL_MERGE_CHUNKS_PER_THREAD);
  EXPECT_EQ(options.maxInFlightChunks, 0u);
  EXPECT_EQ(options.serialNumRunsThreshold, 2u);
  EXPECT_EQ(options.serialNumElementsThreshold,
            DEFAULT_PARALLEL_MERGE_SERIAL_ELEMENT_THRESHOLD);
  EXPECT_EQ(options.bufferedBlocksPerChunk, 2u);
  EXPECT_FALSE(options.stableTieBreaking);
}
