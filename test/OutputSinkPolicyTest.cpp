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

#include "util/GTestHelpers.h"
#include "util/jthread.h"
#include "util/parallelBlockMerge/OutputSinkPolicy.h"

// The tests of the Boost.Asio based `AsioInOrderBlockSink` at the bottom of
// this file require coroutines and are therefore not available in the C++17
// backports mode.
#ifndef QLEVER_CPP_17
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <functional>
#include <memory>
#include <optional>

#include "util/AsyncTestHelpers.h"
#endif

using namespace ad_utility::parallelBlockMerge;

namespace {
using Block = std::vector<int>;
using Sink = InOrderBlockSink<Block>;

// A type that does not fulfill any of the requirements of the `BlockSink`
// concept.
struct NotASink {};

static_assert(BlockSink<Sink, Block>);
static_assert(!BlockSink<NotASink, Block>);

std::vector<Block> collect(Sink& sink) {
  std::vector<Block> result;
  for (auto& block : sink.blocks()) {
    result.push_back(std::move(block));
  }
  return result;
}
}  // namespace

// _____________________________________________________________________________
TEST(OutputSinkPolicy, SingleThreaded) {
  Sink sink{2};
  sink.setNumChunks(2);
  sink(0, Block{1, 2});
  sink.finishChunk(0);
  sink(1, Block{3});
  sink(1, Block{4});
  sink.finishChunk(1);
  EXPECT_THAT(collect(sink),
              ::testing::ElementsAre(Block{1, 2}, Block{3}, Block{4}));
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, Empty) {
  Sink sink;
  sink.setNumChunks(0);
  EXPECT_THAT(collect(sink), ::testing::IsEmpty());
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, MultiThreadedOutOfOrder) {
  static constexpr size_t numChunks = 8;
  static constexpr size_t numBlocksPerChunk = 20;
  Sink sink{3};
  sink.setNumChunks(numChunks);

  // The producers deliberately start with the *highest* chunk index, so that
  // the consumer has to reorder them.
  std::vector<ad_utility::JThread> producers;
  for (size_t i = 0; i < numChunks; ++i) {
    size_t chunkIndex = numChunks - 1 - i;
    producers.emplace_back([&sink, chunkIndex] {
      for (size_t j = 0; j < numBlocksPerChunk; ++j) {
        sink(chunkIndex,
             Block{static_cast<int>(chunkIndex), static_cast<int>(j)});
      }
      sink.finishChunk(chunkIndex);
    });
  }

  auto blocks = collect(sink);
  producers.clear();

  ASSERT_EQ(blocks.size(), numChunks * numBlocksPerChunk);
  // The blocks appear in strict chunk order, and in the order of their
  // creation within a chunk.
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    for (size_t j = 0; j < numBlocksPerChunk; ++j) {
      const auto& block = blocks.at(chunkIndex * numBlocksPerChunk + j);
      EXPECT_THAT(block, ::testing::ElementsAre(static_cast<int>(chunkIndex),
                                                static_cast<int>(j)));
    }
  }
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, PushExceptionSurfaces) {
  Sink sink{2};
  sink.setNumChunks(2);
  sink(0, Block{1});
  ad_utility::JThread producer{[&sink] {
    try {
      throw std::runtime_error{"expected error"};
    } catch (...) {
      sink.pushException(std::current_exception());
    }
  }};
  producer.join();
  auto blocks = sink.blocks();
  AD_EXPECT_THROW_WITH_MESSAGE(([&blocks] {
                                 for ([[maybe_unused]] auto& block : blocks) {
                                 }
                               })(),
                               ::testing::HasSubstr("expected error"));
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, ExceptionUnblocksProducers) {
  // A producer that is blocked on a full buffer is unblocked by an exception
  // that is pushed by another producer.
  Sink sink{1};
  sink.setNumChunks(2);
  ad_utility::JThread blockedProducer{[&sink] {
    for (size_t i = 0; i < 100; ++i) {
      sink(1, Block{static_cast<int>(i)});
    }
  }};
  try {
    throw std::runtime_error{"other error"};
  } catch (...) {
    sink.pushException(std::current_exception());
  }
  // This must terminate.
  blockedProducer.join();
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, BackPressure) {
  // With a buffer of a single block per chunk, the producers of the higher
  // chunks are blocked most of the time. The test must still terminate.
  static constexpr size_t numChunks = 4;
  static constexpr size_t numBlocksPerChunk = 50;
  Sink sink{1};
  sink.setNumChunks(numChunks);

  std::vector<ad_utility::JThread> producers;
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    producers.emplace_back([&sink, chunkIndex] {
      for (size_t j = 0; j < numBlocksPerChunk; ++j) {
        sink(chunkIndex, Block{static_cast<int>(chunkIndex)});
      }
      sink.finishChunk(chunkIndex);
    });
  }

  auto blocks = collect(sink);
  producers.clear();
  ASSERT_EQ(blocks.size(), numChunks * numBlocksPerChunk);
  for (size_t chunkIndex = 0; chunkIndex < numChunks; ++chunkIndex) {
    for (size_t j = 0; j < numBlocksPerChunk; ++j) {
      EXPECT_THAT(blocks.at(chunkIndex * numBlocksPerChunk + j),
                  ::testing::ElementsAre(static_cast<int>(chunkIndex)));
    }
  }
}

// _____________________________________________________________________________
TEST(OutputSinkPolicy, AbortUnblocksProducers) {
  // A consumer that stops early must not leave a blocked producer behind. The
  // destructor of the sink aborts, so the producer terminates.
  std::atomic<bool> producerIsDone = false;
  {
    Sink sink{1};
    sink.setNumChunks(1);
    ad_utility::JThread producer{[&sink, &producerIsDone] {
      for (size_t i = 0; i < 1000; ++i) {
        sink(0, Block{static_cast<int>(i)});
      }
      producerIsDone = true;
    }};
    // Consume a single block, then abort.
    auto blocks = sink.blocks();
    auto it = blocks.begin();
    ASSERT_NE(it, blocks.end());
    sink.abort();
    producer.join();
  }
  EXPECT_TRUE(producerIsDone);
}

#ifndef QLEVER_CPP_17
namespace {
using AsioSink = AsioInOrderBlockSink<Block>;
// A counted latch via which a producer coroutine signals that it is done.
// NOTE: In contrast to the channels inside the sink this one is *not* confined
// to the sink's strand, so it has to be a concurrent channel.
using Latch =
    net::experimental::concurrent_channel<void(boost::system::error_code)>;

// Push all `blocks` to the `sink` as the chunk with the given `chunkIndex`,
// then finish that chunk and open the `latch`. Stop early if the merge was
// stopped, and count the blocks that were actually pushed in `numPushed`.
net::awaitable<void> pushBlocks(AsioSink& sink, size_t chunkIndex,
                                std::vector<Block> blocks, Latch& latch,
                                std::atomic<size_t>* numPushed = nullptr) {
  for (auto& block : blocks) {
    if (!co_await sink.push(chunkIndex, std::move(block))) {
      break;
    }
    if (numPushed != nullptr) {
      ++(*numPushed);
    }
  }
  co_await sink.finishChunk(chunkIndex);
  latch.try_send(boost::system::error_code{});
}

// Push a single block to the `sink`, then finish the chunk and open the
// `latch`. Record in `pushedBlock` that the block was pushed, such that the
// caller can wait until this coroutine is suspended inside `finishChunk`.
net::awaitable<void> pushOneBlockAndFinish(AsioSink& sink, size_t chunkIndex,
                                           Block block, Latch& latch,
                                           std::atomic<bool>& pushedBlock) {
  if (co_await sink.push(chunkIndex, std::move(block))) {
    pushedBlock.store(true);
  }
  co_await sink.finishChunk(chunkIndex);
  latch.try_send(boost::system::error_code{});
}

// Consume all the blocks of the `sink` and return them.
net::awaitable<std::vector<Block>> collectAsync(AsioSink& sink) {
  std::vector<Block> result;
  while (auto block = co_await sink.nextBlock()) {
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
ASYNC_TEST(AsioOutputSinkPolicy, inOrderAcrossChunks) {
  AsioSink sink{ioContext.get_executor(), 3, 2};
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
ASYNC_TEST(AsioOutputSinkPolicy, empty) {
  AsioSink sink{ioContext.get_executor(), 0, 2};
  auto block = co_await sink.nextBlock();
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
ASYNC_TEST(AsioOutputSinkPolicy, chunksWithoutAnyBlock) {
  AsioSink sink{ioContext.get_executor(), 3, 2};
  Latch latch{ioContext.get_executor(), 3};
  net::co_spawn(ioContext, pushBlocks(sink, 0, {}, latch), net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 1, {{7}}, latch), net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 2, {}, latch), net::detached);
  auto blocks = co_await collectAsync(sink);
  EXPECT_THAT(blocks, ::testing::ElementsAre(Block{7}));
  co_await waitForLatch(latch, 3);
}

// _____________________________________________________________________________
ASYNC_TEST(AsioOutputSinkPolicy, backPressure) {
  // With a single buffered block per chunk, the producer of the second chunk
  // cannot run ahead while the consumer still drains the first one.
  AsioSink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(ioContext,
                pushBlocks(sink, 1, {{10}, {11}, {12}}, latch, &numPushed),
                net::detached);
  net::co_spawn(ioContext, pushBlocks(sink, 0, {{0}}, latch), net::detached);
  auto firstBlock = co_await sink.nextBlock();
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
ASYNC_TEST(AsioOutputSinkPolicy, pushExceptionSurfaces) {
  AsioSink sink{ioContext.get_executor(), 2, 2};
  co_await sink.pushException(
      std::make_exception_ptr(std::runtime_error{"kaboom"}));
  EXPECT_TRUE(sink.stopRequested());
  bool didThrow = false;
  try {
    co_await sink.nextBlock();
  } catch (const std::runtime_error& exception) {
    didThrow = true;
    EXPECT_STREQ(exception.what(), "kaboom");
  }
  EXPECT_TRUE(didThrow);
}

// _____________________________________________________________________________
ASYNC_TEST(AsioOutputSinkPolicy, exceptionUnblocksProducers) {
  AsioSink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(ioContext,
                pushBlocks(sink, 1, {{10}, {11}, {12}}, latch, &numPushed),
                net::detached);
  // Let the producer of chunk `1` fill its single buffer slot, such that it is
  // suspended while it waits for the consumer to catch up.
  co_await yieldUntil(ioContext,
                      [&numPushed] { return numPushed.load() == 1; });
  co_await sink.pushException(
      std::make_exception_ptr(std::runtime_error{"kaboom"}));
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
  EXPECT_EQ(numPushed.load(), 1u);
  bool didThrow = false;
  try {
    co_await sink.nextBlock();
  } catch (const std::runtime_error& exception) {
    didThrow = true;
    EXPECT_STREQ(exception.what(), "kaboom");
  }
  EXPECT_TRUE(didThrow);
}

// _____________________________________________________________________________
ASYNC_TEST(AsioOutputSinkPolicy, abortUnblocksProducers) {
  AsioSink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(ioContext,
                pushBlocks(sink, 1, {{10}, {11}, {12}}, latch, &numPushed),
                net::detached);
  co_await yieldUntil(ioContext,
                      [&numPushed] { return numPushed.load() == 1; });
  co_await sink.abort();
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
  EXPECT_EQ(numPushed.load(), 1u);
  // An aborted sink yields nothing anymore, not even the block that is still
  // buffered.
  auto block = co_await sink.nextBlock();
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
ASYNC_TEST(AsioOutputSinkPolicy, abortUnblocksFinishChunk) {
  // The end-of-chunk sentinel travels through the same bounded channel as the
  // blocks, so a producer may also be suspended inside `finishChunk`. Aborting
  // has to wake that one up, too.
  AsioSink sink{ioContext.get_executor(), 2, 1};
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<bool> pushedBlock{false};
  net::co_spawn(ioContext,
                pushOneBlockAndFinish(sink, 1, Block{10}, latch, pushedBlock),
                net::detached);
  // The single buffer slot of chunk `1` is taken by the block, so the producer
  // is now suspended while it sends the sentinel.
  co_await yieldUntil(ioContext, [&pushedBlock] { return pushedBlock.load(); });
  co_await sink.abort();
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
}

// _____________________________________________________________________________
ASYNC_TEST_N(AsioOutputSinkPolicy, multiThreaded, 4) {
  // The same as `inOrderAcrossChunks`, but with several threads and many more
  // blocks, so that the producers and the consumer really run concurrently.
  constexpr size_t numChunks = 8;
  constexpr size_t numBlocksPerChunk = 20;
  AsioSink sink{ioContext.get_executor(), numChunks, 2};
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
ASYNC_TEST_N(AsioOutputSinkPolicy, abortRacesWithProducers, 4) {
  // Abort while many producers are in flight on several threads. Every single
  // producer has to arrive at its `finishChunk`, no matter whether it is
  // currently suspended on a full channel, about to initiate a `push`, or about
  // to send its end-of-chunk sentinel. The last case is the interesting one,
  // because the channel of a chunk that never pushed a block only comes into
  // existence in that `finishChunk`, i.e. possibly after the abort has already
  // swept over all the channels that existed at its time.
  constexpr size_t numChunks = 32;
  constexpr size_t numBlocksPerChunk = 20;
  AsioSink sink{ioContext.get_executor(), numChunks, 1};
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
    co_await sink.nextBlock();
  }
  co_await sink.abort();
  EXPECT_TRUE(sink.stopRequested());
  // This hangs if a single producer was left suspended.
  co_await waitForLatch(latch, numChunks);
  auto block = co_await sink.nextBlock();
  EXPECT_FALSE(block.has_value());
}
#endif  // QLEVER_CPP_17
