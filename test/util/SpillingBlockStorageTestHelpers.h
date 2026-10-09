// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_UTIL_SPILLINGBLOCKSTORAGETESTHELPERS_H
#define QLEVER_TEST_UTIL_SPILLINGBLOCKSTORAGETESTHELPERS_H

// Helpers for the tests of a `parallelBlockMerge::SpillingBlockStorage` with an
// arbitrary codec, see `util/parallelBlockMerge/SpillingBlockStorage.h`. That
// storage is coroutine-based, so this whole header is empty when
// `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/cleanup/cleanup.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <exception>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "./AsioTestHelpers.h"
#include "./GTestHelpers.h"
#include "backports/filesystem.h"
#include "util/Exception.h"
#include "util/File.h"
#include "util/SourceLocation.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace spillingBlockStorageTestHelpers {

// A block that was read back from a storage, together with the information
// whether it was still in memory (and not spilled).
template <typename Block>
struct ReadBlock {
  Block block_;
  bool wasInMemory_;
};

// Return an object that deletes the spill files of the chunks
// `0, ..., numChunks - 1` of the `storage` when it is destroyed. A storage
// deletes the file of a chunk itself as soon as that chunk is done, so this is
// only a safeguard for a test that fails (or that deliberately leaves chunks
// unfinished).
template <typename Storage>
auto makeSpillFileCleanup(const Storage& storage, size_t numChunks) {
  std::vector<std::string> filenames;
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    filenames.push_back(storage.spillFilename(chunkIndex));
  }
  return absl::Cleanup{[filenames = std::move(filenames)] {
    for (const auto& filename : filenames) {
      ad_utility::deleteFile(filename, false);
    }
  }};
}

// Return a completion handler for `Storage::getBlock` that expects no
// exception and stores the `GetResult` in `result`, which therefore has to
// outlive the operation.
template <typename Storage>
auto makeGetHandler(std::optional<typename Storage::GetResult>& result) {
  return [&result](std::exception_ptr exception,
                   typename Storage::GetResult getResult) {
    EXPECT_EQ(exception, nullptr);
    result = std::move(getResult);
  };
}

// Store the `blocks` followed by the end-of-chunk sentinel as the chunk with
// the given `chunkIndex` of the `storage`, one after the other, and run the
// `ioContext` (on which the storage runs) after each of them, so that at most
// one `storeBlock` of the chunk is in flight at any time. Expect that every
// value is stored.
template <typename Storage>
void storeChunk(boost::asio::io_context& ioContext, Storage& storage,
                size_t chunkIndex,
                std::vector<typename Storage::Block> blocks) {
  std::vector<typename Storage::OptionalBlock> values;
  for (auto& block : blocks) {
    values.emplace_back(std::move(block));
  }
  values.emplace_back(std::nullopt);
  for (auto& value : values) {
    std::optional<bool> wasStored;
    storage.storeBlock(chunkIndex, std::move(value),
                       [&wasStored](std::exception_ptr exception, bool stored) {
                         EXPECT_EQ(exception, nullptr);
                         wasStored = stored;
                       });
    ad_utility::testing::pollUntilQuiescent(ioContext);
    ASSERT_TRUE(wasStored.has_value());
    EXPECT_TRUE(wasStored.value());
  }
}

// Initiate a single `getBlock` of the chunk with the given `chunkIndex` of the
// `storage`, run the `ioContext`, and return the result.
//
// PRECONDITION: The chunk already holds a value, such that the operation
// completes right away. Otherwise its handler would outlive the result that it
// writes to.
template <typename Storage>
std::optional<typename Storage::GetResult> getOnce(
    boost::asio::io_context& ioContext, Storage& storage, size_t chunkIndex) {
  std::optional<typename Storage::GetResult> result;
  storage.getBlock(chunkIndex, makeGetHandler<Storage>(result));
  ad_utility::testing::pollUntilQuiescent(ioContext);
  return result;
}

// Read all the blocks of the chunk with the given `chunkIndex` of the
// `storage` until its end-of-chunk sentinel, see `storeChunk`, and
// materialize them.
template <typename Storage>
std::vector<ReadBlock<typename Storage::Block>> readChunk(
    boost::asio::io_context& ioContext, Storage& storage, size_t chunkIndex) {
  std::vector<ReadBlock<typename Storage::Block>> blocks;
  while (true) {
    auto result = getOnce(ioContext, storage, chunkIndex);
    AD_CORRECTNESS_CHECK(result.has_value());
    EXPECT_FALSE(result->wasCancelled());
    if (!result->hasValue()) {
      return blocks;
    }
    auto deferredBlock = std::move(result).value().get();
    bool wasInMemory = deferredBlock.isInMemory();
    blocks.push_back({std::move(deferredBlock).materialize(), wasInMemory});
  }
}

// Store the `blocks` as chunk `0` and as chunk `1` of the `storage` (a fresh
// storage that runs on the `ioContext` and keeps `numBufferedBlocks` blocks
// in memory), read both chunks back, and check that they arrive unchanged.
// The blocks are compared after applying the `projection` to them, which has
// to turn a block into something that `::testing::ElementsAreArray` can
// compare. The consumer starts with chunk `0`, so that chunk keeps its first
// `numBufferedBlocks` blocks in memory and spills the others, whereas chunk
// `1` spills every block. Also check that every spill file is deleted once its
// chunk is done.
template <typename Storage, typename Projection>
void expectRoundTripOfSpilledAndBufferedBlocks(
    boost::asio::io_context& ioContext, Storage& storage,
    const std::vector<typename Storage::Block>& blocks,
    size_t numBufferedBlocks, const Projection& projection,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  storeChunk(ioContext, storage, 0, blocks);
  storeChunk(ioContext, storage, 1, blocks);
  EXPECT_TRUE(ql::filesystem::exists(storage.spillFilename(1)));
  for (size_t chunkIndex : {0u, 1u}) {
    auto readBlocks = readChunk(ioContext, storage, chunkIndex);
    ASSERT_EQ(readBlocks.size(), blocks.size());
    for (size_t i = 0; i < blocks.size(); ++i) {
      EXPECT_THAT(projection(readBlocks.at(i).block_),
                  ::testing::ElementsAreArray(projection(blocks.at(i))));
      EXPECT_EQ(readBlocks.at(i).wasInMemory_,
                chunkIndex == 0 && i < numBufferedBlocks);
    }
  }
  // The deletion of a spill file is posted to the executor of the storage.
  ad_utility::testing::pollUntilQuiescent(ioContext);
  EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(0)));
  EXPECT_FALSE(ql::filesystem::exists(storage.spillFilename(1)));
}

// Check that a `getBlock` of a chunk that has no block yet suspends, and that
// `cancelAll` completes it as cancelled.
template <typename Storage>
void expectCancelAllWakesUpAWaitingConsumer(
    boost::asio::io_context& ioContext, Storage& storage,
    ad_utility::source_location loc = AD_CURRENT_SOURCE_LOC()) {
  auto trace = generateLocationTrace(loc);
  // NOTE: The `result` has to outlive the `cancelAll` below, which is what
  // completes the operation, so `getOnce` cannot be used here.
  std::optional<typename Storage::GetResult> result;
  storage.getBlock(0, makeGetHandler<Storage>(result));
  ad_utility::testing::pollUntilQuiescent(ioContext);
  EXPECT_FALSE(result.has_value());
  storage.cancelAll();
  ad_utility::testing::pollUntilQuiescent(ioContext);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->wasCancelled());
}

}  // namespace spillingBlockStorageTestHelpers

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_TEST_UTIL_SPILLINGBLOCKSTORAGETESTHELPERS_H
