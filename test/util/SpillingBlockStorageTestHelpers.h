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

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <cstddef>
#include <exception>
#include <optional>
#include <utility>
#include <vector>

#include "./AsioTestHelpers.h"
#include "util/Exception.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace spillingBlockStorageTestHelpers {

// A block that was read back from a storage, together with the information
// whether it was still in memory (and not spilled).
template <typename Block>
struct ReadBlock {
  Block block_;
  bool wasInMemory_;
};

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
  storage.getBlock(chunkIndex,
                   [&result](std::exception_ptr exception,
                             typename Storage::GetResult getResult) {
                     EXPECT_EQ(exception, nullptr);
                     result = std::move(getResult);
                   });
  ad_utility::testing::pollUntilQuiescent(ioContext);
  return std::move(result);
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

// Check that a `getBlock` of a chunk that has no block yet suspends, and that
// `cancelAll` completes it as cancelled.
template <typename Storage>
void expectCancelAllWakesUpAWaitingConsumer(boost::asio::io_context& ioContext,
                                            Storage& storage) {
  // NOTE: The `result` has to outlive the `cancelAll` below, which is what
  // completes the operation, so `getOnce` cannot be used here.
  std::optional<typename Storage::GetResult> result;
  storage.getBlock(0, [&result](std::exception_ptr exception,
                                typename Storage::GetResult getResult) {
    EXPECT_EQ(exception, nullptr);
    result = std::move(getResult);
  });
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
