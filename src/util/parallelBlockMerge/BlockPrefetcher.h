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

#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility::parallelBlockMerge::detail {

namespace net = boost::asio;

// The consuming side of a sink that a `BlockPrefetcher` reads from, see
// `InOrderBlockSink`: `asyncGetNextBlock` completes with the next block, or
// with `std::nullopt` at the end of the merge, or with an exception, and
// `asyncStop` stops the merge, such that a pending and every later
// `asyncGetNextBlock` completes with `std::nullopt` promptly. (The producing
// side is the `SinkConcept`, which the `BlockPrefetcher` does not need.)
template <typename T, typename Block>
concept PrefetchableSinkConcept = requires(T& sink) {
  sink.asyncGetNextBlock([](std::exception_ptr, std::optional<Block>) {});
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
// `numPrefetchedBlocks + 1` blocks besides the one that the consumer holds.
//
// LIFETIME: The filler holds the sink and the channel alive by itself, and
// `shutDown()` (which the destructor calls) waits until the filler has
// finished, so no operation of the filler is left afterwards.
template <typename Block, typename Sink>
requires PrefetchableSinkConcept<Sink, Block>
class BlockPrefetcher : public ad_utility::NoCopyNoMove {
 private:
  // The value that the filler sends for every completed `asyncGetNextBlock`:
  // either a block, or the end of the merge (`std::nullopt`), or an exception.
  // The latter two are the last value that is ever sent. The `error_code` is
  // required by the channel and always empty.
  using Channel = net::experimental::concurrent_channel<void(
      boost::system::error_code, std::exception_ptr, std::optional<Block>)>;

  std::shared_ptr<Sink> sink_;
  std::shared_ptr<Channel> channel_;
  // The future of the `co_spawn` of the filler, which is ready as soon as the
  // filler has finished, see `shutDown()`.
  std::future<void> fillerIsDone_;
  // The first exception that the merge has pushed. It is deliberately kept,
  // such that every further call to `getNextBlock()` rethrows it, exactly as
  // the sink itself does.
  std::exception_ptr exception_;
  bool endOfMergeWasReached_ = false;
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
    fillerIsDone_ =
        net::co_spawn(executor, fill(sink_, channel_), net::use_future);
  }

  // Shut the read-ahead down, see `shutDown()`.
  ~BlockPrefetcher() { shutDown(); }

  // Return the next block of the merge, or `std::nullopt` at its end (or after
  // `shutDown()`), and block the calling thread until one of the two is
  // available. Rethrow an exception that the merge has pushed, once all the
  // blocks that were already buffered have been returned. See the CONCURRENCY
  // note at the class comment above.
  //
  // NOTE: This blocks the calling thread, which therefore must not be one of
  // the threads that run the `executor`, because that thread is then no longer
  // available to deliver the very block that this waits for.
  std::optional<Block> getNextBlock() {
    if (exception_ != nullptr) {
      std::rethrow_exception(exception_);
    }
    if (endOfMergeWasReached_ || wasShutDown_) {
      return std::nullopt;
    }
    auto [exception, block] = receive();
    if (exception != nullptr) {
      exception_ = std::move(exception);
      std::rethrow_exception(exception_);
    }
    if (!block.has_value()) {
      endOfMergeWasReached_ = true;
    }
    return std::move(block);
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
    // in `async_send` because the channel is full.
    //
    // NOTE: The channel is deliberately never closed or cancelled, the filler
    // always ends by sending a last value (the end of the merge or an
    // exception) which is received either here or by `getNextBlock()`. Closing
    // and cancelling a channel with a suspended sender is broken in some
    // versions of Boost (in Boost 1.83, `cancel` after `close` completes the
    // suspended send as if it were a receive).
    bool lastValueWasReceived = endOfMergeWasReached_ || exception_ != nullptr;
    while (!lastValueWasReceived) {
      auto [exception, block] = receive();
      lastValueWasReceived = exception != nullptr || !block.has_value();
    }
    fillerIsDone_.wait();
    sink_.reset();
  }

 private:
  // Receive the next value from the channel, and block the calling thread
  // until it is available.
  std::pair<std::exception_ptr, std::optional<Block>> receive() {
    auto [errorCode, exception, block] =
        channel_->async_receive(net::as_tuple(net::use_future)).get();
    AD_CORRECTNESS_CHECK(!errorCode,
                         "The channel of a `BlockPrefetcher` is never closed "
                         "or cancelled.");
    return {std::move(exception), std::move(block)};
  }

  // The filler, see the class comment above: read the blocks from the `sink`
  // one after the other and send them into the `channel`, until the end of the
  // merge or an exception, which is the last value that is sent.
  static net::awaitable<void> fill(std::shared_ptr<Sink> sink,
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
            boost::system::error_code{}, std::move(exception), std::move(block),
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
      co_await channel->async_send(boost::system::error_code{},
                                   std::move(failure), std::nullopt,
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
