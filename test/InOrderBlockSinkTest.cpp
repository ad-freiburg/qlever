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

#include <atomic>
#include <cstddef>
#include <exception>
#include <stdexcept>
#include <utility>
#include <vector>

#include "util/parallelBlockMerge/InOrderBlockSink.h"

// The tests in this file drive the sink from coroutines and are therefore not
// available in the C++17 backports mode. The sink itself is coroutine-free and
// available in that mode, see `InOrderBlockSink.h`.
#ifndef QLEVER_CPP_17
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <functional>

#include "util/AsyncTestHelpers.h"
#endif

using namespace ad_utility::parallelBlockMerge;

#ifndef QLEVER_CPP_17
namespace {
using Block = std::vector<int>;
using Sink = InOrderBlockSink<Block>;
// A counted latch via which a producer coroutine signals that it is done.
// NOTE: In contrast to the channels inside the sink this one is *not* confined
// to the sink's strand, so it has to be a concurrent channel.
using Latch =
    net::experimental::concurrent_channel<void(boost::system::error_code)>;

// Push all `blocks` to the `sink` as the chunk with the given `chunkIndex`,
// then finish that chunk and open the `latch`. Stop early if the merge was
// stopped, and count the blocks that were actually pushed in `numPushed`.
net::awaitable<void> pushBlocks(Sink& sink, size_t chunkIndex,
                                std::vector<Block> blocks, Latch& latch,
                                std::atomic<size_t>* numPushed = nullptr) {
  for (auto& block : blocks) {
    if (!co_await sink.asyncPush(chunkIndex, std::move(block),
                                 net::use_awaitable)) {
      break;
    }
    if (numPushed != nullptr) {
      ++(*numPushed);
    }
  }
  co_await sink.asyncFinishChunk(chunkIndex, net::use_awaitable);
  latch.try_send(boost::system::error_code{});
}

// Push a single block to the `sink`, then finish the chunk and open the
// `latch`. Record in `pushedBlock` that the block was pushed, such that the
// caller can wait until this coroutine is suspended inside
// `asyncFinishChunk`.
net::awaitable<void> pushOneBlockAndFinish(Sink& sink, size_t chunkIndex,
                                           Block block, Latch& latch,
                                           std::atomic<bool>& pushedBlock) {
  if (co_await sink.asyncPush(chunkIndex, std::move(block),
                              net::use_awaitable)) {
    pushedBlock.store(true);
  }
  co_await sink.asyncFinishChunk(chunkIndex, net::use_awaitable);
  latch.try_send(boost::system::error_code{});
}

// Consume all the blocks of the `sink` and return them.
net::awaitable<std::vector<Block>> collectAsync(Sink& sink) {
  std::vector<Block> result;
  while (auto block = co_await sink.asyncGetNextBlock(net::use_awaitable)) {
    result.push_back(std::move(block.value()));
  }
  co_return result;
}

// Wait until the `latch` was opened `numTimes` times, so that the producers are
// guaranteed to not touch the sink anymore.
net::awaitable<void> waitForLatch(Latch& latch, size_t numTimes = 1) {
  for (size_t i = 0; i < numTimes; ++i) {
    co_await latch.async_receive(net::as_tuple(net::use_awaitable));
  }
}

// Yield to the other coroutines until `condition` holds.
net::awaitable<void> yieldUntil(net::io_context& ioContext,
                                std::function<bool()> condition) {
  while (!condition()) {
    co_await net::post(ioContext, net::use_awaitable);
  }
}
}  // namespace

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, inOrderAcrossChunks) {
  Sink sink{ioContext.get_executor(), 3, 2};
  Latch latch{ioContext.get_executor(), 3};
  // Spawn the producers in reverse order, so that the blocks of the later
  // chunks are produced first.
  net::co_spawn(ioContext, pushBlocks(sink, 2, {{4, 5}}, latch), net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 1, {{2}, {3}}, latch),
                net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 0, {{0, 1}}, latch), net::detached);
  auto blocks = co_await collectAsync(sink);
  EXPECT_THAT(blocks, ::testing::ElementsAre(Block{0, 1}, Block{2}, Block{3},
                                             Block{4, 5}));
  co_await waitForLatch(latch, 3);
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, empty) {
  Sink sink{ioContext.get_executor(), 0, 2};
  auto block = co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, chunksWithoutAnyBlock) {
  Sink sink{ioContext.get_executor(), 3, 2};
  Latch latch{ioContext.get_executor(), 3};
  net::co_spawn(ioContext, pushBlocks(sink, 0, {}, latch), net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 1, {{7}}, latch), net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 2, {}, latch), net::detached);
  auto blocks = co_await collectAsync(sink);
  EXPECT_THAT(blocks, ::testing::ElementsAre(Block{7}));
  co_await waitForLatch(latch, 3);
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, backPressure) {
  // With a single buffered block per chunk, the producer of the second chunk
  // cannot run ahead while the consumer still drains the first one.
  Sink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(ioContext,
                pushBlocks(sink, 1, {{10}, {11}, {12}}, latch, &numPushed),
                net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 0, {{0}}, latch), net::detached);
  auto firstBlock = co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_THAT(firstBlock, ::testing::Optional(Block{0}));
  // The producer of chunk `1` may have filled its single buffer slot, but it
  // cannot have pushed more than that, because the consumer has not consumed
  // any of its blocks yet.
  EXPECT_LE(numPushed.load(), 1u);
  auto rest = co_await collectAsync(sink);
  EXPECT_THAT(rest, ::testing::ElementsAre(Block{10}, Block{11}, Block{12}));
  EXPECT_EQ(numPushed.load(), 3u);
  co_await waitForLatch(latch, 2);
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, pushExceptionSurfaces) {
  Sink sink{ioContext.get_executor(), 2, 2};
  co_await sink.asyncPushException(
      std::make_exception_ptr(std::runtime_error{"kaboom"}),
      net::use_awaitable);
  EXPECT_TRUE(sink.stopRequested());
  bool didThrow = false;
  try {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  } catch (const std::runtime_error& exception) {
    didThrow = true;
    EXPECT_STREQ(exception.what(), "kaboom");
  }
  EXPECT_TRUE(didThrow);
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, exceptionUnblocksProducers) {
  Sink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(ioContext,
                pushBlocks(sink, 1, {{10}, {11}, {12}}, latch, &numPushed),
                net::detached);
  // Let the producer of chunk `1` fill its single buffer slot, such that it is
  // suspended while it waits for the consumer to catch up.
  co_await yieldUntil(ioContext,
                      [&numPushed] { return numPushed.load() == 1; });
  co_await sink.asyncPushException(
      std::make_exception_ptr(std::runtime_error{"kaboom"}),
      net::use_awaitable);
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
  EXPECT_EQ(numPushed.load(), 1u);
  bool didThrow = false;
  try {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  } catch (const std::runtime_error& exception) {
    didThrow = true;
    EXPECT_STREQ(exception.what(), "kaboom");
  }
  EXPECT_TRUE(didThrow);
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, abortUnblocksProducers) {
  Sink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(ioContext,
                pushBlocks(sink, 1, {{10}, {11}, {12}}, latch, &numPushed),
                net::detached);
  co_await yieldUntil(ioContext,
                      [&numPushed] { return numPushed.load() == 1; });
  co_await sink.asyncAbort(net::use_awaitable);
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
  EXPECT_EQ(numPushed.load(), 1u);
  // An aborted sink yields nothing anymore, not even the block that is still
  // buffered.
  auto block = co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, abortUnblocksFinishChunk) {
  // The end-of-chunk sentinel travels through the same bounded channel as the
  // blocks, so a producer may also be suspended inside `asyncFinishChunk`.
  // Aborting has to wake that one up, too.
  Sink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<bool> pushedBlock{false};
  net::co_spawn(ioContext,
                pushOneBlockAndFinish(sink, 1, Block{10}, latch, pushedBlock),
                net::detached);
  // The single buffer slot of chunk `1` is taken by the block, so the producer
  // is now suspended while it sends the sentinel.
  co_await yieldUntil(ioContext, [&pushedBlock] { return pushedBlock.load(); });
  co_await sink.asyncAbort(net::use_awaitable);
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
}

// _____________________________________________________________________________
ASYNC_TEST_N(InOrderBlockSink, multiThreaded, 4) {
  // The same as `inOrderAcrossChunks`, but with several threads and many more
  // blocks, so that the producers and the consumer really run concurrently.
  constexpr size_t numChunks = 8;
  constexpr size_t numBlocksPerChunk = 20;
  Sink sink{ioContext.get_executor(), numChunks, 2};
  Latch latch{ioContext.get_executor(), numChunks};
  std::vector<Block> expected;
  for (size_t chunk = 0; chunk < numChunks; ++chunk) {
    std::vector<Block> blocks;
    for (size_t i = 0; i < numBlocksPerChunk; ++i) {
      blocks.push_back(Block{static_cast<int>(chunk * numBlocksPerChunk + i)});
      expected.push_back(blocks.back());
    }
    net::co_spawn(ioContext, pushBlocks(sink, chunk, std::move(blocks), latch),
                  net::detached);
  }
  auto blocks = co_await collectAsync(sink);
  EXPECT_THAT(blocks, ::testing::ElementsAreArray(expected));
  co_await waitForLatch(latch, numChunks);
}

// _____________________________________________________________________________
ASYNC_TEST_N(InOrderBlockSink, abortRacesWithProducers, 4) {
  // Abort while many producers are in flight on several threads. Every single
  // producer has to arrive at its `finishChunk`, no matter whether it is
  // currently suspended on a full channel, about to initiate a `push`, or about
  // to send its end-of-chunk sentinel. The last case is the interesting one,
  // because the channel of a chunk that never pushed a block only comes into
  // existence in that `finishChunk`, i.e. possibly after the abort has already
  // swept over all the channels that existed at its time.
  constexpr size_t numChunks = 32;
  constexpr size_t numBlocksPerChunk = 20;
  Sink sink{ioContext.get_executor(), numChunks, 1};
  Latch latch{ioContext.get_executor(), numChunks};
  for (size_t chunk = 0; chunk < numChunks; ++chunk) {
    std::vector<Block> blocks;
    // Every third chunk pushes nothing at all, see the comment above.
    if (chunk % 3 != 0) {
      for (size_t i = 0; i < numBlocksPerChunk; ++i) {
        blocks.push_back(Block{static_cast<int>(i)});
      }
    }
    net::co_spawn(ioContext, pushBlocks(sink, chunk, std::move(blocks), latch),
                  net::detached);
  }
  // Consume a little, such that the producers really are in flight, and then
  // abort in the middle of everything.
  for (size_t i = 0; i < 5; ++i) {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  }
  co_await sink.asyncAbort(net::use_awaitable);
  EXPECT_TRUE(sink.stopRequested());
  // This hangs if a single producer was left suspended.
  co_await waitForLatch(latch, numChunks);
  auto block = co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
}
#endif  // QLEVER_CPP_17
