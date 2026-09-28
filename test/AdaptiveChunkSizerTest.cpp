// Copyright 2026, The QLever Authors, in particular:
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <limits>

#include "engine/AdaptiveChunkSizer.h"

namespace {

using qlever::AdaptiveChunkConfig;
using qlever::AdaptiveChunkSizer;
using qlever::AdaptiveChunkStats;

// =============================================================================
// Unit Tests for AdaptiveChunkSizer
// =============================================================================

TEST(AdaptiveChunkSizerTest, DefaultConstructionAndInitialState) {
  AdaptiveChunkSizer sizer;

  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 0);
  EXPECT_EQ(sizer.totalBytes(), 0);
  EXPECT_EQ(sizer.totalRows(), 0);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);

  // Initial target rows: ceil(65536 / 120.0) = 547
  EXPECT_EQ(sizer.targetRowCount(), 547);

  const AdaptiveChunkStats stats = sizer.stats();
  EXPECT_EQ(stats.chunksFlushed_, 0);
  EXPECT_EQ(stats.totalBytes_, 0);
  EXPECT_EQ(stats.totalRows_, 0);
  EXPECT_EQ(stats.currentChunkBytes_, 64 * 1024);
  EXPECT_DOUBLE_EQ(stats.averageRowBytes_, 120.0);
  EXPECT_EQ(stats.targetRowsForNextChunk_, 547);
}

TEST(AdaptiveChunkSizerTest, ExponentialRampUpProgression) {
  AdaptiveChunkSizer sizer;

  // Initial: 64 KB
  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);

  // Flush 1: 64 KB -> 128 KB
  sizer.recordChunk(64 * 1024, 500);
  EXPECT_EQ(sizer.currentChunkBytes(), 128 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 1);

  // Flush 2: 128 KB -> 256 KB
  sizer.recordChunk(128 * 1024, 1000);
  EXPECT_EQ(sizer.currentChunkBytes(), 256 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 2);

  // Flush 3: 256 KB -> 512 KB
  sizer.recordChunk(256 * 1024, 2000);
  EXPECT_EQ(sizer.currentChunkBytes(), 512 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 3);

  // Flush 4: 512 KB -> 1 MB
  sizer.recordChunk(512 * 1024, 4000);
  EXPECT_EQ(sizer.currentChunkBytes(), 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 4);

  // Flush 5: 1 MB -> 2 MB
  sizer.recordChunk(1024 * 1024, 8000);
  EXPECT_EQ(sizer.currentChunkBytes(), 2 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 5);

  // Flush 6: 2 MB -> 4 MB (Cap reached)
  sizer.recordChunk(2 * 1024 * 1024, 16000);
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 6);

  // Flush 7: Remains at 4 MB max cap
  sizer.recordChunk(4 * 1024 * 1024, 32000);
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 7);

  // Flush 8: Sustained bulk streaming at 4 MB
  sizer.recordChunk(4 * 1024 * 1024, 32000);
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 8);
}

TEST(AdaptiveChunkSizerTest, DynamicRowEstimation) {
  AdaptiveChunkSizer sizer;

  // Suppose actual row format produces 250 bytes per row
  const size_t rows = 260;
  const size_t bytes = rows * 250;
  sizer.recordChunk(bytes, rows);

  // Estimated row bytes should adjust towards 250
  EXPECT_GT(sizer.averageRowBytes(), 150.0);
  EXPECT_LE(sizer.averageRowBytes(), 250.0);

  // Next chunk target is 128 KB (131072 bytes).
  // Target rows should reflect the updated row size estimate.
  const size_t expectedTargetRows = static_cast<size_t>(
      std::ceil(static_cast<double>(128 * 1024) / sizer.averageRowBytes()));
  EXPECT_EQ(sizer.targetRowCount(), expectedTargetRows);
}

TEST(AdaptiveChunkSizerTest, TargetRowCountWithRemainingRowsClamping) {
  AdaptiveChunkSizer sizer;

  const size_t target = sizer.targetRowCount();
  EXPECT_GT(target, 100);

  // If 50 rows remain, targetRowCount should clamp to 50
  EXPECT_EQ(sizer.targetRowCount(50), 50);

  // If 100,000 rows remain, targetRowCount returns unconstrained target
  EXPECT_EQ(sizer.targetRowCount(100'000), target);
}

TEST(AdaptiveChunkSizerTest, IsChunkFullPredicate) {
  AdaptiveChunkSizer sizer;
  const size_t targetBytes = sizer.currentChunkBytes();

  EXPECT_FALSE(sizer.isChunkFull(0));
  EXPECT_FALSE(sizer.isChunkFull(targetBytes - 1));
  EXPECT_TRUE(sizer.isChunkFull(targetBytes));
  EXPECT_TRUE(sizer.isChunkFull(targetBytes + 1024));

  const size_t targetRows = sizer.targetRowCount();
  EXPECT_FALSE(sizer.isChunkFull(0, 0));
  EXPECT_FALSE(sizer.isChunkFull(100, targetRows - 1));
  EXPECT_TRUE(sizer.isChunkFull(100, targetRows));
}

TEST(AdaptiveChunkSizerTest, ResetRestoresInitialState) {
  AdaptiveChunkSizer sizer;

  for (size_t i = 0; i < 10; ++i) {
    sizer.recordChunk(1024 * 1024, 8000);
  }
  EXPECT_EQ(sizer.currentChunkBytes(), 4 * 1024 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 10);
  EXPECT_GT(sizer.totalBytes(), 0);
  EXPECT_GT(sizer.totalRows(), 0);

  sizer.reset();

  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);
  EXPECT_EQ(sizer.chunksFlushed(), 0);
  EXPECT_EQ(sizer.totalBytes(), 0);
  EXPECT_EQ(sizer.totalRows(), 0);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);
}

TEST(AdaptiveChunkSizerTest, ZeroRowAndZeroByteHandling) {
  AdaptiveChunkSizer sizer;

  // An empty chunk is counted, but it neither changes the row estimate nor
  // advances the ramp.
  sizer.recordChunk(0, 0);
  EXPECT_EQ(sizer.chunksFlushed(), 1);
  EXPECT_EQ(sizer.totalBytes(), 0);
  EXPECT_EQ(sizer.totalRows(), 0);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);
  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);

  sizer.recordChunk(0, 10);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);
  EXPECT_EQ(sizer.currentChunkBytes(), 64 * 1024);

  // A chunk with bytes but an unknown row count (0) advances the ramp but
  // leaves the row estimate unchanged. This is how the chunked HTTP transfer
  // uses the sizer.
  sizer.recordChunk(64 * 1024, 0);
  EXPECT_EQ(sizer.chunksFlushed(), 3);
  EXPECT_EQ(sizer.totalBytes(), 0);
  EXPECT_DOUBLE_EQ(sizer.averageRowBytes(), 120.0);
  EXPECT_EQ(sizer.currentChunkBytes(), 128 * 1024);
}

// A growth factor that would overshoot `maxChunkBytes_` (or even the range of
// `size_t`) lands exactly on `maxChunkBytes_`; non-finite factors are rejected.
TEST(AdaptiveChunkSizerTest, LargeGrowthFactorIsCappedAtMax) {
  AdaptiveChunkConfig config;
  config.initialChunkBytes_ = 1024;
  config.maxChunkBytes_ = 4096;
  config.growthFactor_ = 1e300;
  AdaptiveChunkSizer sizer(config);
  sizer.recordChunk(1024, 0);
  EXPECT_EQ(sizer.currentChunkBytes(), 4096);
  sizer.recordChunk(4096, 0);
  EXPECT_EQ(sizer.currentChunkBytes(), 4096);

  config.growthFactor_ = std::numeric_limits<double>::infinity();
  EXPECT_ANY_THROW(AdaptiveChunkSizer{config});
  config.growthFactor_ = std::numeric_limits<double>::quiet_NaN();
  EXPECT_ANY_THROW(AdaptiveChunkSizer{config});
  config.growthFactor_ = 0.5;
  EXPECT_ANY_THROW(AdaptiveChunkSizer{config});
}

TEST(AdaptiveChunkSizerTest, CustomConfiguration) {
  AdaptiveChunkConfig config;
  config.initialChunkBytes_ = 32 * 1024;
  config.maxChunkBytes_ = 512 * 1024;
  config.growthFactor_ = 4.0;
  config.initialEstimatedRowBytes_ = 64.0;
  config.minChunkRows_ = 5;
  config.maxChunkRows_ = 1000;
  AdaptiveChunkSizer sizer(config);

  EXPECT_EQ(sizer.currentChunkBytes(), 32 * 1024);
  EXPECT_EQ(sizer.targetRowCount(), 512);

  // 32 KB * 4 = 128 KB
  sizer.recordChunk(32 * 1024, 500);
  EXPECT_EQ(sizer.currentChunkBytes(), 128 * 1024);

  // 128 KB * 4 = 512 KB (Max cap reached)
  sizer.recordChunk(128 * 1024, 2000);
  EXPECT_EQ(sizer.currentChunkBytes(), 512 * 1024);

  // Next flush stays at 512 KB
  sizer.recordChunk(512 * 1024, 8000);
  EXPECT_EQ(sizer.currentChunkBytes(), 512 * 1024);
}

}  // namespace
