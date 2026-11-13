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

#include <boost/asio/use_future.hpp>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

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

using chunkwisePermutationWriter::ChunkwisePermutationWriter;
namespace net = boost::asio;

// Run `writeAndCheck` with the chunkwise writer.
void writeAndCheckChunkwise(const TestRows& rows, const Config& config,
                            source_location l = AD_CURRENT_SOURCE_LOC()) {
  writeAndCheck(rows, config, Mode::Chunkwise, l);
}

// The number of columns and the blocksize of the writers that `makeWriter`
// creates.
constexpr size_t NUM_COLUMNS = 4;
constexpr size_t BLOCKSIZE = 20;

// Create a chunkwise writer with `numChunks` chunks for the permutations at
// `basename`, without any callbacks. The tests that use it drive the writer
// directly via the interface of the sink, instead of via a merge.
std::shared_ptr<ChunkwisePermutationWriter> makeWriter(
    size_t numChunks, const std::string& basename,
    chunkwisePermutationWriter::Options options = {},
    ql::any_io_executor executor = ad_utility::globalExecutor()) {
  auto writerAndCallback = [](const std::string& filename) {
    return CompressedRelationWriter::WriterAndCallback{
        std::make_unique<CompressedRelationWriter>(
            NUM_COLUMNS, ad_utility::File{filename, "w"}, BLOCKSIZE,
            ad_utility::testing::multiThreadExecutor()),
        [](ql::span<const CompressedRelationMetadata>) {}};
  };
  return ChunkwisePermutationWriter::create(
      numChunks, basename, writerAndCallback(basename + ".perm1"),
      writerAndCallback(basename + ".perm2"), qlever::KeyOrder{0, 1, 2, 3}, {},
      options, std::move(executor));
}

// Convert the `rows` to a block with `NUM_COLUMNS` columns.
chunkwisePermutationWriter::Block makeBlock(const TestRows& rows) {
  chunkwisePermutationWriter::Block block{
      NUM_COLUMNS, ad_utility::makeUnlimitedAllocator<Id>()};
  block.resize(rows.size());
  for (size_t i = 0; i < rows.size(); ++i) {
    for (size_t col = 0; col < NUM_COLUMNS; ++col) {
      block(i, col) = V(rows[i][col]);
    }
  }
  return block;
}

// Return a future that is already completed, as the `mergeCompletion` for
// `ChunkwisePermutationWriter::finish` if there is no merge.
std::future<void> completedFuture() {
  std::promise<void> promise;
  promise.set_value();
  return promise.get_future();
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

// Test that a writer needs at least one chunk and a (non-empty) executor.
TEST(ChunkwisePermutationWriter, invalidArguments) {
  auto [basename, cleanup] =
      permutationFilesWithCleanup(gtestCurrentTestName());
  AD_EXPECT_THROW_WITH_MESSAGE(makeWriter(0, basename),
                               ::testing::HasSubstr("at least one chunk"));
  AD_EXPECT_THROW_WITH_MESSAGE(
      makeWriter(1, basename, {}, ql::any_io_executor{}),
      ::testing::HasSubstr("must not be empty"));
}

// Test the sink directly: a push after which nothing is left of the block
// (because it only consists of duplicates of the previous one) dispatches
// nothing, but the writing goes on.
TEST(ChunkwisePermutationWriter, pushOfDuplicatesOnly) {
  auto [basename, cleanup] =
      permutationFilesWithCleanup(gtestCurrentTestName());
  chunkwisePermutationWriter::Options options;
  options.removeDuplicates_ = true;
  auto writer = makeWriter(1, basename, options);
  TestRows rows{{1, 2, 3, 0}, {1, 2, 4, 0}};
  EXPECT_TRUE(writer->asyncPush(0, makeBlock(rows), net::use_future).get());
  EXPECT_TRUE(
      writer->asyncPush(0, makeBlock({rows.back()}), net::use_future).get());
  EXPECT_TRUE(writer->asyncFinishChunk(0, net::use_future).get());
  auto result = writer->finish(completedFuture());
  EXPECT_EQ(writer->numInputRows(), 3);
  checkPermutation(basename + ".perm1", result.blockMetadata_,
                   expectedRows(rows, qlever::KeyOrder{0, 1, 2, 3}),
                   NUM_COLUMNS);
}

// Test the sink directly: the first exception that the merge pushes to the
// sink is rethrown by `finish`, later ones are ignored.
TEST(ChunkwisePermutationWriter, exceptionsPushedByTheMerge) {
  auto [basename, cleanup] =
      permutationFilesWithCleanup(gtestCurrentTestName());
  auto writer = makeWriter(2, basename);
  for (std::string_view message : {"first failure", "second failure"}) {
    writer
        ->asyncPushException(
            std::make_exception_ptr(std::runtime_error{std::string{message}}),
            net::use_future)
        .get();
    EXPECT_TRUE(writer->stopRequested());
  }
  AD_EXPECT_THROW_WITH_MESSAGE(writer->finish(completedFuture()),
                               ::testing::HasSubstr("first failure"));
}

// Test the sink directly: if the merge stops the sink before all the chunks
// are done, `finish` throws.
TEST(ChunkwisePermutationWriter, stoppedByTheMerge) {
  auto [basename, cleanup] =
      permutationFilesWithCleanup(gtestCurrentTestName());
  auto writer = makeWriter(2, basename);
  writer->asyncStop(net::use_future).get();
  EXPECT_TRUE(writer->stopRequested());
  // After the stop, the operations of the chunks do nothing.
  EXPECT_FALSE(
      writer->asyncPush(0, makeBlock({{1, 2, 3, 0}}), net::use_future).get());
  EXPECT_FALSE(writer->asyncFinishChunk(0, net::use_future).get());
  EXPECT_THROW(writer->finish(completedFuture()),
               ad_utility::ThreadSafeReorderBufferStopped);
}

// Test the sink directly: an exception of the operation of a chunk stops the
// writer and is rethrown by `finish`. The operations fail because of an input
// block with the wrong number of columns, and because of an invalid chunk
// index.
TEST(ChunkwisePermutationWriter, failingOperationOfChunk) {
  auto [basename, cleanup] =
      permutationFilesWithCleanup(gtestCurrentTestName());
  {
    auto writer = makeWriter(2, basename);
    chunkwisePermutationWriter::Block block{
        NUM_COLUMNS - 1, ad_utility::makeUnlimitedAllocator<Id>()};
    block.resize(1);
    EXPECT_FALSE(writer->asyncPush(0, std::move(block), net::use_future).get());
    EXPECT_TRUE(writer->stopRequested());
    EXPECT_ANY_THROW(writer->finish(completedFuture()));
  }
  {
    auto writer = makeWriter(2, basename);
    EXPECT_FALSE(writer->asyncFinishChunk(2, net::use_future).get());
    EXPECT_TRUE(writer->stopRequested());
    EXPECT_THROW(writer->finish(completedFuture()), std::out_of_range);
  }
}

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
