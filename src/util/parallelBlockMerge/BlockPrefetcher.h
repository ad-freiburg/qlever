// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKPREFETCHER_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKPREFETCHER_H

// The read-ahead on the consumer side of a parallel merge that pushes to an
// `InOrderBlockSink`. It is driven by a coroutine, so this whole header is only
// available in C++20 mode and empty when `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17`
// is set, see `util/parallelBlockMerge/ParallelMergeState.h`.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <absl/cleanup/cleanup.h>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <utility>

#include "util/AsioHelpers.h"
#include "util/Exception.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockStorage.h"

namespace ad_utility::parallelBlockMerge::detail {

namespace net = boost::asio;

// The consuming side of a sink that a `BlockPrefetcher` reads from, see
// `InOrderBlockSink`: `asyncGetNextBlock` completes with the next (possibly
// deferred) block, or with `std::nullopt` at the end of the merge, or with an
// exception, and `asyncStop` stops the merge, such that a pending and every
// later `asyncGetNextBlock` completes with `std::nullopt` promptly. (The
// producing side is the `SinkConcept`, which the `BlockPrefetcher` does not
// need.)
template <typename T, typename Block>
concept PrefetchableSinkConcept = requires(T& sink) {
  sink.asyncGetNextBlock(
      [](std::exception_ptr, std::optional<DeferredBlock<Block>>) {});
  sink.asyncStop([](std::exception_ptr) {});
};

// Keep up to `numPrefetchedBlocks` blocks of a `Sink` ready for a consumer that
// reads them synchronously, such that a consumer that asks for the next block
// typically gets one that is already there, instead of having to wait for the
// merge. See the `PrefetchableSinkConcept` above for what the `Sink` has to
// provide.
//
// The read-ahead is a single coroutine (the "filler", see `fill()`) that runs
// on the given executor, reads one block after the other from the sink, and
// sends each of them into a `concurrent_channel` whose capacity is
// `numPrefetchedBlocks`. A full channel suspends the filler, which is what
// bounds the read-ahead; the consumer receives from that channel.
//
// CONCURRENT READS: The sink hands out `DeferredBlock`s, whose expensive part
// (e.g. reading a spilled block back from disk and decompressing it) has not
// happened yet. The filler does not wait for that part, but starts it on the
// executor right away and sends a `std::future` of the block into the channel
// instead. So the reads of all the blocks in the channel run concurrently,
// although the sink itself is read strictly sequentially, and the consumer
// still gets the blocks in their order, as it waits for the futures one after
// the other. An exception of such a read is rethrown when the consumer reaches
// the respective block.
//
// SEQUENTIAL READS: The filler never initiates an `asyncGetNextBlock` before
// the previous one has completed. That is not an implementation detail but a
// hard requirement of `InOrderBlockSink::asyncGetNextBlock`, which must never
// run concurrently with itself, see there.
//
// CONCURRENCY: This class is not thread-safe. All its member functions
// (including the destructor) have to be called by a single consumer, or at
// least never concurrently with each other. Only the communication between the
// consumer and the filler (which runs on the executor) is synchronized, namely
// via the channel.
//
// MEMORY: While the channel is full, the filler holds one more block in the
// `async_send` that it is suspended in, so the read-ahead holds up to
// `numPrefetchedBlocks + 1` blocks besides the one that the consumer holds,
// no matter whether they are already read or are still being read.
//
// LIFETIME: The filler holds the sink and the channel alive by itself, and
// `shutDown()` (which the destructor calls) waits until the filler and every
// read that it has started have finished, so no operation of the filler is
// left afterwards.
template <typename Block, typename Sink>
requires PrefetchableSinkConcept<Sink, Block>
class BlockPrefetcher : public ad_utility::NoCopyNoMove {
 private:
  // The future of a single value that the filler sends: either a block, or the
  // end of the merge (`std::nullopt`), or an exception.
  using FutureBlock = std::future<std::optional<Block>>;

  // The value that the filler sends for every completed `asyncGetNextBlock`:
  // the `FutureBlock` and whether that is the last value that is ever sent,
  // which is the case for the end of the merge and for an exception of the
  // sink (but not for an exception of a read, which the filler does not wait
  // for, see the CONCURRENT READS note above). The `error_code` is required by
  // the channel and always empty.
  using Channel = net::experimental::concurrent_channel<void(
      boost::system::error_code, bool, FutureBlock)>;

  std::shared_ptr<Sink> sink_;
  std::shared_ptr<Channel> channel_;
  // The future of the `co_spawn` of the filler, which is ready as soon as the
  // filler has finished, see `shutDown()`.
  std::future<void> fillerIsDone_;
  // The first exception that the merge has pushed (or that a read has thrown).
  // It is deliberately kept, such that every further call to `getNextBlock()`
  // rethrows it, exactly as the sink itself does.
  std::exception_ptr exception_;
  // True once the last value that the filler ever sends was received, see
  // `Channel`.
  bool lastValueWasReceived_ = false;
  bool wasShutDown_ = false;

 public:
  // Create a read-ahead of up to `numPrefetchedBlocks` (which have to be
  // positive) blocks from the `sink`, and start it right away on the
  // `executor`, which typically is the executor of the merge itself.
  BlockPrefetcher(net::any_io_executor executor, std::shared_ptr<Sink> sink,
                  size_t numPrefetchedBlocks)
      : sink_{std::move(sink)} {
    AD_CONTRACT_CHECK(sink_ != nullptr);
    AD_CONTRACT_CHECK(numPrefetchedBlocks > 0);
    channel_ = std::make_shared<Channel>(executor, numPrefetchedBlocks);
    fillerIsDone_ = net::co_spawn(executor, fill(executor, sink_, channel_),
                                  net::use_future);
  }

  // Shut the read-ahead down, see `shutDown()`.
  ~BlockPrefetcher() { shutDown(); }

  // Return the next block of the merge, or `std::nullopt` at its end (or after
  // `shutDown()`), and block the calling thread until one of the two is
  // available. Rethrow an exception that the merge has pushed, once all the
  // blocks that were already buffered have been returned, and likewise an
  // exception of the read of a block exactly in that block's place. See the
  // CONCURRENCY note at the class comment above.
  //
  // NOTE: This blocks the calling thread, which therefore must not be one of
  // the threads that run the `executor`, because that thread is then no longer
  // available to deliver the very block that this waits for.
  std::optional<Block> getNextBlock() {
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    if (lastValueWasReceived_ || wasShutDown_) {
      return std::nullopt;
    }
    auto [isLastValue, futureBlock] = receive();
    lastValueWasReceived_ = isLastValue;
    try {
      return futureBlock.get();
    } catch (...) {
      exception_ = std::current_exception();
      throw;
    }
  }

  // Stop the sink (and thereby the merge), wait until the filler has finished,
  // and release everything that the read-ahead holds, including the blocks that
  // are still buffered. This can be called at any time, and calling it more
  // than once is fine. See the CONCURRENCY note at the class comment above.
  //
  // NOTE: This blocks the calling thread, which therefore must not be one of
  // the threads that run the `executor`, see `getNextBlock()`.
  void shutDown() {
    if (std::exchange(wasShutDown_, true)) {
      return;
    }
    // Once the sink is stopped, a pending and every later `asyncGetNextBlock`
    // of the filler completes with `std::nullopt` promptly. Wait for the stop,
    // such that the sink is no longer referenced once this function returns.
    sink_->asyncStop(net::use_future).get();
    // Receive (and drop) the values until the last one, unless the consumer
    // has already received it. This also wakes up a filler that is suspended
    // in `async_send` because the channel is full. Every read that the filler
    // has started is waited for, such that none of them is left once this
    // function returns.
    //
    // NOTE: The channel is deliberately never closed or cancelled, the filler
    // always ends by sending a last value (the end of the merge or an
    // exception) which is received either here or by `getNextBlock()`. Closing
    // and cancelling a channel with a suspended sender is broken in some
    // versions of Boost (in Boost 1.83, `cancel` after `close` completes the
    // suspended send as if it were a receive).
    while (!lastValueWasReceived_) {
      auto [isLastValue, futureBlock] = receive();
      lastValueWasReceived_ = isLastValue;
      futureBlock.wait();
    }
    fillerIsDone_.wait();
    sink_.reset();
  }

 private:
  // Receive the next value from the channel, and block the calling thread
  // until it is available.
  std::pair<bool, FutureBlock> receive() {
    auto [errorCode, isLastValue, futureBlock] =
        channel_->async_receive(net::as_tuple(net::use_future)).get();
    AD_CORRECTNESS_CHECK(!errorCode,
                         "The channel of a `BlockPrefetcher` is never closed "
                         "or cancelled.");
    return {isLastValue, std::move(futureBlock)};
  }

  // Turn a single result of `asyncGetNextBlock` of the sink into the future of
  // the block. A `block` that still has to be read is read on the `executor`,
  // all others are returned as a ready future, see the CONCURRENT READS note at
  // the class comment above.
  //
  // NOTE: The reader of the `block` is also destroyed on the `executor`, which
  // matters, because that may be expensive (e.g. it may delete a file, see
  // `DeferredBlock::materialize`). It is destroyed before the future becomes
  // ready, such that `shutDown()`, which waits for these futures, returns only
  // once every reader (and everything that it holds) has been released. The
  // function that `runFunctionOnExecutor` runs is only destroyed after the
  // future has become ready, so the reader must not merely be owned by it.
  static FutureBlock startRead(const net::any_io_executor& executor,
                               std::exception_ptr exception,
                               std::optional<DeferredBlock<Block>> block) {
    if (exception == nullptr && block.has_value() && !block->isInMemory()) {
      return ad_utility::runFunctionOnExecutor(
          executor,
          [block = std::move(block)]() mutable {
            // Destroy the reader also if it throws.
            absl::Cleanup destroyReader = [&block] { block.reset(); };
            return std::optional<Block>{std::move(block).value().materialize()};
          },
          net::use_future);
    }
    // NOTE: This is not strictly necessary (the `materialize` call above would
    // also work for all the remaining cases), but it saves the trip to the
    // `executor` for values that are already available.
    std::promise<std::optional<Block>> promise;
    if (exception != nullptr) {
      promise.set_exception(std::move(exception));
    } else if (!block.has_value()) {
      promise.set_value(std::nullopt);
    } else {
      promise.set_value(std::move(block).value().materialize());
    }
    return promise.get_future();
  }

  // The filler, see the class comment above: read the blocks from the `sink`
  // one after the other, start their reads on the `executor`, and send their
  // futures into the `channel`, until the end of the merge or an exception of
  // the sink, which is the last value that is sent.
  static net::awaitable<void> fill(net::any_io_executor executor,
                                   std::shared_ptr<Sink> sink,
                                   std::shared_ptr<Channel> channel) {
    // NOTE: None of the operations below throws, the sink reports its
    // exceptions as values, so the `catch` is merely a safety net that makes
    // sure that the consumer still receives a last value. (It cannot contain
    // the `co_await` itself, which C++20 does not allow in a handler.)
    std::exception_ptr failure;
    try {
      for (;;) {
        auto [exception, block] =
            co_await sink->asyncGetNextBlock(net::as_tuple(net::use_awaitable));
        bool isLastValue = exception != nullptr || !block.has_value();
        auto [errorCode] = co_await channel->async_send(
            boost::system::error_code{}, isLastValue,
            startRead(executor, std::move(exception), std::move(block)),
            net::as_tuple(net::use_awaitable));
        AD_CORRECTNESS_CHECK(!errorCode,
                             "The channel of a `BlockPrefetcher` is never "
                             "closed or cancelled.");
        if (isLastValue) {
          break;
        }
      }
    } catch (...) {
      failure = std::current_exception();
    }
    if (failure != nullptr) {
      // The channel only carries futures, so the `failure` is sent as a future
      // that rethrows it on `get()`, which delivers it to the consumer exactly
      // like an exception of a read, see `getNextBlock()`. The `true` marks it
      // as the last value, such that neither `getNextBlock()` nor `shutDown()`
      // waits for another one.
      std::promise<std::optional<Block>> promise;
      promise.set_exception(std::move(failure));
      co_await channel->async_send(boost::system::error_code{}, true,
                                   promise.get_future(),
                                   net::as_tuple(net::use_awaitable));
    }
    // Release the sink before the filler finishes (and thereby makes
    // `fillerIsDone_` ready), such that the sink is no longer referenced by the
    // filler once `shutDown()` returns.
    sink.reset();
  }
};

}  // namespace ad_utility::parallelBlockMerge::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKPREFETCHER_H
