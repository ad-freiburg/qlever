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

// The sink is implemented with coroutines and therefore does not exist in the
// C++17 backports mode, see `util/parallelBlockMerge/InOrderBlockSink.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
#include <absl/functional/any_invocable.h>

#include <atomic>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../util/AsyncTestHelpers.h"
#include "./InMemoryBlockStorage.h"
#include "util/Exception.h"
#include "util/parallelBlockMerge/BlockSinkPolicy.h"
#include "util/parallelBlockMerge/InOrderBlockSink.h"

using namespace ad_utility::parallelBlockMerge;

namespace {
using Block = std::vector<int>;
// The blocks of these tests are small, so they simply live in memory. Any other
// model of the `BlockStorageConcept` would do just as well.
using Sink = InOrderBlockSink<Block, InMemoryBlockStorage<Block>>;
// The sink is the output policy that the parallel merge pushes to, so it has to
// model the `SinkConcept`.
static_assert(SinkConcept<Sink, Block>);

// Construct a sink for `numChunks` chunks that buffers at most
// `maxBufferedBlocksPerChunk` blocks per chunk.
Sink makeSink(net::any_io_executor executor, size_t numChunks,
              size_t maxBufferedBlocksPerChunk) {
  return Sink{std::move(executor), numChunks,
              makeInMemoryStorageFactory<Block>(maxBufferedBlocksPerChunk)};
}

// A counted latch via which a producer coroutine signals that it is done.
// NOTE: In contrast to the channels inside the storage this one is *not*
// confined to the sink's strand, so it has to be a concurrent channel.
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

// The state that a `ControlledBlockStorage` (see below) shares with the test
// that drives it: the completion handlers of the at most one `storeBlock` and
// the at most one `getBlock` that may be in flight (see the PRECONDITIONS of
// the `BlockStorageConcept`), plus the result with which a pending `storeBlock`
// completes when the storage is cancelled.
struct StorageControl {
  absl::AnyInvocable<void(bool)> pendingStore_;
  absl::AnyInvocable<void(GetResult<Block>)> pendingGet_;
  // A storage whose blocking work has already begun may well complete a
  // `storeBlock` successfully although it was cancelled in the meantime, see
  // `BlockStorageConcept::cancelAll`. Set this to `true` to exercise that case.
  bool storeSucceedsOnCancel_ = false;

  bool hasPendingStore() const { return pendingStore_ != nullptr; }
  bool hasPendingGet() const { return pendingGet_ != nullptr; }
};
using SharedStorageControl = std::shared_ptr<StorageControl>;

// A model of the `BlockStorageConcept` that never completes an operation on its
// own: a `storeBlock` and a `getBlock` stay in flight until the storage is
// cancelled. It thereby makes those interactions of the sink with the storage
// deterministic that the `InMemoryBlockStorage` only ever produces in a race,
// namely a `getBlock` that is cancelled while it is in flight, and a
// `storeBlock` that succeeds although the merge was stopped in the meantime.
//
// NOTE: This is a handle to the `StorageControl` that the test holds, because
// the sink owns its storage by value and the test therefore cannot refer to the
// storage itself.
class ControlledBlockStorage {
 public:
  using OptionalBlock = ad_utility::parallelBlockMerge::OptionalBlock<Block>;

 private:
  Strand strand_;
  SharedStorageControl control_;

 public:
  ControlledBlockStorage(Strand strand, SharedStorageControl control)
      : strand_{std::move(strand)}, control_{std::move(control)} {}

  // Keep the completion handler until the storage is cancelled.
  template <typename CompletionToken>
  auto storeBlock([[maybe_unused]] size_t chunkIndex,
                  [[maybe_unused]] OptionalBlock block,
                  CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken, void(std::exception_ptr, bool)>(
        [this](auto handler) mutable {
          AD_CORRECTNESS_CHECK(!control_->hasPendingStore());
          control_->pendingStore_ =
              [handler = std::move(handler)](bool wasStored) mutable {
                std::move(handler)(std::exception_ptr{}, wasStored);
              };
        },
        completionToken);
  }

  // Keep the completion handler until the storage is cancelled.
  template <typename CompletionToken>
  auto getBlock([[maybe_unused]] size_t chunkIndex,
                CompletionToken&& completionToken) {
    return net::async_initiate<CompletionToken,
                               void(std::exception_ptr, GetResult<Block>)>(
        [this](auto handler) mutable {
          AD_CORRECTNESS_CHECK(!control_->hasPendingGet());
          control_->pendingGet_ =
              [handler = std::move(handler)](GetResult<Block> result) mutable {
                std::move(handler)(std::exception_ptr{}, std::move(result));
              };
        },
        completionToken);
  }

  // Complete the pending operations. NOTE: They are completed via `net::post`
  // and not inline, exactly as the channels of the `InMemoryBlockStorage` do
  // it, so that nothing is resumed while `cancelAll` still runs.
  void cancelAll() noexcept {
    net::post(strand_, [control = control_] {
      if (control->hasPendingStore()) {
        std::exchange(control->pendingStore_,
                      nullptr)(control->storeSucceedsOnCancel_);
      }
      if (control->hasPendingGet()) {
        std::exchange(control->pendingGet_, nullptr)(GetResult<Block>{});
      }
    });
  }
};
static_assert(BlockStorageConcept<ControlledBlockStorage, Block>);
using ControlledSink = InOrderBlockSink<Block, ControlledBlockStorage>;

// Construct a sink for `numChunks` chunks whose storage is driven by `control`.
ControlledSink makeControlledSink(net::any_io_executor executor,
                                  size_t numChunks,
                                  SharedStorageControl control) {
  return ControlledSink{std::move(executor), numChunks,
                        [control = std::move(control)](const Strand& strand) {
                          return ControlledBlockStorage{strand, control};
                        }};
}

// Push a single `block` to the `sink`, record in `wasPushed` whether it was
// stored, and open the `latch`.
net::awaitable<void> pushOneBlock(ControlledSink& sink, size_t chunkIndex,
                                  Block block, std::optional<bool>& wasPushed,
                                  Latch& latch) {
  wasPushed =
      co_await sink.asyncPush(chunkIndex, std::move(block), net::use_awaitable);
  latch.try_send(boost::system::error_code{});
}

// Retrieve a single value from the `sink`, record it in `received`, and open
// the `latch`.
net::awaitable<void> getOneBlock(
    ControlledSink& sink,
    std::optional<ControlledSink::OptionalBlock>& received, Latch& latch) {
  received = co_await sink.asyncGetNextBlock(net::use_awaitable);
  latch.try_send(boost::system::error_code{});
}

// Retrieve a single value from the `sink`, expecting that it rethrows a pushed
// `std::runtime_error` with the message `expectedMessage`. Record in `didThrow`
// that it did, and open the `latch`.
net::awaitable<void> getOneBlockExpectingThrow(ControlledSink& sink,
                                               std::string expectedMessage,
                                               bool& didThrow, Latch& latch) {
  try {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  } catch (const std::runtime_error& exception) {
    didThrow = true;
    EXPECT_EQ(exception.what(), expectedMessage);
  }
  latch.try_send(boost::system::error_code{});
}
}  // namespace

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, inOrderAcrossChunks) {
  auto sink = makeSink(ioContext.get_executor(), 3, 2);
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
  auto sink = makeSink(ioContext.get_executor(), 0, 2);
  auto block = co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, chunksWithoutAnyBlock) {
  auto sink = makeSink(ioContext.get_executor(), 3, 2);
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
  auto sink = makeSink(ioContext.get_executor(), 2, 1);
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
  auto sink = makeSink(ioContext.get_executor(), 2, 2);
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
  auto sink = makeSink(ioContext.get_executor(), 2, 1);
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
ASYNC_TEST(InOrderBlockSink, stopUnblocksProducers) {
  auto sink = makeSink(ioContext.get_executor(), 2, 1);
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<size_t> numPushed{0};
  net::co_spawn(ioContext,
                pushBlocks(sink, 1, {{10}, {11}, {12}}, latch, &numPushed),
                net::detached);
  co_await yieldUntil(ioContext,
                      [&numPushed] { return numPushed.load() == 1; });
  co_await sink.asyncStop(net::use_awaitable);
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
  EXPECT_EQ(numPushed.load(), 1u);
  // A stopped sink yields nothing anymore, not even the block that is still
  // buffered.
  auto block = co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, stopUnblocksFinishChunk) {
  // The end-of-chunk sentinel travels through the same bounded channel as the
  // blocks, so a producer may also be suspended inside `asyncFinishChunk`.
  // Aborting has to wake that one up, too.
  auto sink = makeSink(ioContext.get_executor(), 2, 1);
  Latch latch{ioContext.get_executor(), 2};
  std::atomic<bool> pushedBlock{false};
  net::co_spawn(ioContext,
                pushOneBlockAndFinish(sink, 1, Block{10}, latch, pushedBlock),
                net::detached);
  // The single buffer slot of chunk `1` is taken by the block, so the producer
  // is now suspended while it sends the sentinel.
  co_await yieldUntil(ioContext, [&pushedBlock] { return pushedBlock.load(); });
  co_await sink.asyncStop(net::use_awaitable);
  // The suspended producer has to wake up, otherwise this hangs.
  co_await waitForLatch(latch);
}

// _____________________________________________________________________________
ASYNC_TEST_N(InOrderBlockSink, multiThreaded, 4) {
  // The same as `inOrderAcrossChunks`, but with several threads and many more
  // blocks, so that the producers and the consumer really run concurrently.
  constexpr size_t numChunks = 8;
  constexpr size_t numBlocksPerChunk = 20;
  auto sink = makeSink(ioContext.get_executor(), numChunks, 2);
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
ASYNC_TEST_N(InOrderBlockSink, stopRacesWithProducers, 4) {
  // Abort while many producers are in flight on several threads. Every single
  // producer has to arrive at its `finishChunk`, no matter whether it is
  // currently suspended on a full channel, about to initiate a `push`, or about
  // to send its end-of-chunk sentinel. The last case is the interesting one,
  // because the channel of a chunk that never pushed a block only comes into
  // existence in that `finishChunk`, i.e. possibly after the stop has already
  // swept over all the channels that existed at its time.
  constexpr size_t numChunks = 32;
  constexpr size_t numBlocksPerChunk = 20;
  auto sink = makeSink(ioContext.get_executor(), numChunks, 1);
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
  // stop in the middle of everything.
  for (size_t i = 0; i < 5; ++i) {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  }
  co_await sink.asyncStop(net::use_awaitable);
  EXPECT_TRUE(sink.stopRequested());
  // This hangs if a single producer was left suspended.
  co_await waitForLatch(latch, numChunks);
  auto block = co_await sink.asyncGetNextBlock(net::use_awaitable);
  EXPECT_FALSE(block.has_value());
}

// _____________________________________________________________________________
TEST(InOrderBlockSink, theChunkIndexIsChecked) {
  net::io_context ioContext;
  auto sink = makeSink(ioContext.get_executor(), 2, 1);
  EXPECT_ANY_THROW(sink.asyncPush(2, Block{1}, net::detached));
  EXPECT_ANY_THROW(sink.asyncFinishChunk(2, net::detached));
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, onlyTheFirstExceptionIsKept) {
  auto sink = makeSink(ioContext.get_executor(), 2, 2);
  co_await sink.asyncPushException(
      std::make_exception_ptr(std::runtime_error{"first"}), net::use_awaitable);
  // The second exception is silently ignored, and in particular it does not
  // sweep over the storage a second time.
  co_await sink.asyncPushException(
      std::make_exception_ptr(std::runtime_error{"second"}),
      net::use_awaitable);
  bool didThrow = false;
  try {
    co_await sink.asyncGetNextBlock(net::use_awaitable);
  } catch (const std::runtime_error& exception) {
    didThrow = true;
    EXPECT_STREQ(exception.what(), "first");
  }
  EXPECT_TRUE(didThrow);
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, aCancelledGetEndsTheRange) {
  // A `getBlock` that was in flight when the merge was stopped completes as
  // cancelled. The consumer then has to report the end of the range instead of
  // mistaking that cancellation for an end-of-chunk sentinel.
  auto control = std::make_shared<StorageControl>();
  auto sink = makeControlledSink(ioContext.get_executor(), 1, control);
  Latch latch{ioContext.get_executor(), 1};
  std::optional<ControlledSink::OptionalBlock> received;
  net::co_spawn(ioContext, getOneBlock(sink, received, latch), net::detached);
  co_await yieldUntil(ioContext,
                      [&control] { return control->hasPendingGet(); });
  co_await sink.asyncStop(net::use_awaitable);
  co_await waitForLatch(latch);
  EXPECT_FALSE(received.value().has_value());
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, aCancelledGetRethrowsThePushedException) {
  // The same as `aCancelledGetEndsTheRange`, but the stop comes from a pushed
  // exception, which the consumer has to rethrow after the cancellation.
  auto control = std::make_shared<StorageControl>();
  auto sink = makeControlledSink(ioContext.get_executor(), 1, control);
  Latch latch{ioContext.get_executor(), 1};
  bool didThrow = false;
  net::co_spawn(ioContext,
                getOneBlockExpectingThrow(sink, "kaboom", didThrow, latch),
                net::detached);
  co_await yieldUntil(ioContext,
                      [&control] { return control->hasPendingGet(); });
  co_await sink.asyncPushException(
      std::make_exception_ptr(std::runtime_error{"kaboom"}),
      net::use_awaitable);
  co_await waitForLatch(latch);
  EXPECT_TRUE(didThrow);
}

// _____________________________________________________________________________
ASYNC_TEST(InOrderBlockSink, aBlockThatIsStoredAfterTheStopIsDropped) {
  // A storage may complete a `storeBlock` successfully although it was
  // cancelled in the meantime. The block is then still lost, because no
  // consumer will ever read it, so the push has to report `false` such that its
  // producer stops producing.
  auto control = std::make_shared<StorageControl>();
  control->storeSucceedsOnCancel_ = true;
  auto sink = makeControlledSink(ioContext.get_executor(), 1, control);
  Latch latch{ioContext.get_executor(), 1};
  std::optional<bool> wasPushed;
  net::co_spawn(ioContext, pushOneBlock(sink, 0, Block{1}, wasPushed, latch),
                net::detached);
  co_await yieldUntil(ioContext,
                      [&control] { return control->hasPendingStore(); });
  co_await sink.asyncStop(net::use_awaitable);
  co_await waitForLatch(latch);
  EXPECT_FALSE(wasPushed.value());
}
#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
