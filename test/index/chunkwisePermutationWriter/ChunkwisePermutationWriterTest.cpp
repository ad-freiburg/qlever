// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "../../util/GTestHelpers.h"
#include "../../util/PermutationPairTestHelpers.h"

// The tests of the chunkwise permutation writer that are specific to it: many
// chunks, relations that span several chunks, the serial merge, and failing
// callbacks. The tests that apply to both the classical and the chunkwise
// writer are parameterized in `test/index/PermutationPairWriterTest.cpp`.
//
// NOTE: The chunkwise writer needs the parallel merge, which only exists in the
// C++20 mode of the build.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

namespace {
using namespace permutationPairTestHelpers;

// Run `writeAndCheck` with the chunkwise writer.
void writeAndCheckChunkwise(const TestRows& rows, const Config& config,
                            source_location l = AD_CURRENT_SOURCE_LOC()) {
  writeAndCheck(rows, config, Mode::Chunkwise, l);
}
}  // namespace

// Test random rows that are split into many chunks.
TEST(ChunkwisePermutationWriter, randomRowsManyChunks) {
  writeAndCheckChunkwise(randomRows(300'000, 4, 42), Config{});
}

// Test a few small relations, then one relation that is much larger than a
// chunk, then a few small relations again.
TEST(ChunkwisePermutationWriter, relationSpanningManyChunks) {
  TestRows rows;
  for (int64_t i = 0; i < 20; ++i) {
    rows.push_back({i, i, i, 0});
  }
  for (int64_t i = 0; i < 400'000; ++i) {
    rows.push_back({100, i % 1'000, i % 777, i % 3});
  }
  for (int64_t i = 0; i < 20; ++i) {
    rows.push_back({200 + i, i, i, 0});
  }
  writeAndCheckChunkwise(rows, Config{});
}

// Test two relations that span several chunks each, separated by a single
// tiny relation, so that the boundary between the two large ones falls into a
// chunk that contains the end of the first, the tiny one, and the beginning of
// the second.
TEST(ChunkwisePermutationWriter, twoSpanningRelationsAndTinyRelationsBetween) {
  TestRows rows;
  for (int64_t i = 0; i < 150'000; ++i) {
    rows.push_back({1, i % 500, i % 333, i % 2});
  }
  rows.push_back({2, 1, 1, 0});
  for (int64_t i = 0; i < 150'000; ++i) {
    rows.push_back({3, i % 500, i % 333, i % 2});
  }
  writeAndCheckChunkwise(rows, Config{});
}

// Test a merge with a parallelism of one, which merges serially.
TEST(ChunkwisePermutationWriter, serialMergeParallelismOne) {
  Config config;
  config.parallelism_ = 1;
  writeAndCheckChunkwise(randomRows(50'000, 4, 47), config);
}

// Test that the exception of a failing callback is propagated, both if it is
// reported via `done` and if the callback throws.
TEST(ChunkwisePermutationWriter, failingCallbackPropagates) {
  for (bool throws : {false, true}) {
    Config config;
    config.failingCallbackBlock_ = 2;
    config.failingCallbackThrows_ = throws;
    ScopedIgnoreMemoryLimit ignoreMemoryLimit;
    auto [basename, cleanup] = permutationFilesWithCleanup(
        absl::StrCat(gtestCurrentTestName(), throws));
    auto rows = randomRows(100'000, 4, 48);
    AD_EXPECT_THROW_WITH_MESSAGE(
        writeRows(rows, basename, config, Mode::Chunkwise),
        ::testing::HasSubstr("callback failed"));
  }
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
