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
#include <stdexcept>
#include <utility>

#include "util/Exception.h"
#include "util/NoCopyNoMove.h"

namespace ad_utility::parallelBlockMerge::detail {

namespace net = boost::asio;

// Keep up to `numPrefetchedBlocks` blocks of a `Sink` ready for a consumer that
// reads them synchronously, such that a consumer that asks for the next block
// typically gets one that is already there, instead of having to wait for the
// merge. The `Sink` only has to provide the operation
// `asyncGetNextBlock(token)` with the completion signature
// `void(std::exception_ptr, std::optional<Block>)`, where `std::nullopt` is the
// end of the merge, see `InOrderBlockSink`.
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
// MEMORY: While the channel is full, the filler holds one more block in the
// `async_send` that it is suspended in, so the read-ahead holds up to
// `numPrefetchedBlocks + 1` blocks besides the one that the consumer holds.
//
// LIFETIME: The filler holds the sink and the channel alive by itself, and
// `shutDown()` (which the destructor calls) waits until the filler has
// finished, so no operation of the filler is left afterwards.
template <typename Block, typename Sink>
class BlockPrefetcher : public ad_utility::NoCopyNoMove {
 private:
  // The value that the filler sends for every completed `asyncGetNextBlock`:
  // either a block, or the end of the merge (`std::nullopt`), or an exception.
  // The latter two are the last value that is ever sent. The `error_code` is
  // required by the channel and always empty.
  using Channel = net::experimental::concurrent_channel<void(
      boost::system::error_code, std::exception_ptr, std::optional<Block>)>;

  std::shared_ptr<Channel> channel_;
  // Ready as soon as the filler has finished, see `shutDown()`. It holds the
  // exception that has left the filler (if any), see `onFillerDone`.
  std::shared_future<void> fillerIsDone_;
  // The following members are only ever touched by the consumer.
  //
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
                  size_t numPrefetchedBlocks) {
    AD_CONTRACT_CHECK(sink != nullptr);
    AD_CONTRACT_CHECK(numPrefetchedBlocks > 0);
    channel_ = std::make_shared<Channel>(executor, numPrefetchedBlocks);
    auto fillerIsDone = std::make_shared<std::promise<void>>();
    fillerIsDone_ = fillerIsDone->get_future().share();
    net::co_spawn(executor, fill(std::move(sink), channel_),
                  [channel = channel_, fillerIsDone = std::move(fillerIsDone)](
                      std::exception_ptr exception) {
                    onFillerDone(std::move(channel), std::move(fillerIsDone),
                                 std::move(exception));
                  });
  }

  // Shut the read-ahead down, see `shutDown()`, including its PRECONDITION.
  ~BlockPrefetcher() { shutDown(); }

  // Return the next block of the merge, or `std::nullopt` at its end, and block
  // the calling thread until one of the two is available. Rethrow an exception
  // that the merge has pushed, once all the blocks that were already buffered
  // have been returned.
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
    auto [errorCode, exception, block] =
        channel_->async_receive(net::as_tuple(net::use_future)).get();
    if (errorCode) {
      // The filler has failed and closed the channel, see `onFillerDone`.
      exception = failureOfFiller();
    }
    if (exception != nullptr) {
      exception_ = std::move(exception);
      std::rethrow_exception(exception_);
    }
    if (!block.has_value()) {
      endOfMergeWasReached_ = true;
    }
    return std::move(block);
  }

  // Stop the read-ahead, wait until the filler has finished, and release
  // everything that the read-ahead holds. Calling this more than once is fine.
  //
  // PRECONDITION: The merge has already been stopped, because only then does
  // the sink complete a pending `asyncGetNextBlock` promptly (with
  // `std::nullopt`) and this wait terminates. See the destructor of
  // `ParallelMergeRange` for the exact ordering.
  //
  // NOTE: This blocks the calling thread, which therefore must not be one of
  // the threads that run the `executor`, see `getNextBlock()`.
  void shutDown() {
    if (std::exchange(wasShutDown_, true)) {
      return;
    }
    // A filler that is suspended in `async_send` is woken up, and every later
    // `async_send` fails right away, so the filler finishes as soon as its
    // current `asyncGetNextBlock` (if any) has completed.
    channel_->close();
    channel_->cancel();
    fillerIsDone_.wait();
    // Drop the blocks that are still buffered.
    channel_->reset();
  }

 private:
  // The filler, see the class comment above: read the blocks from the `sink`
  // one after the other and send them into the `channel`, until the end of the
  // merge, an exception, or the channel being closed by `shutDown()`.
  static net::awaitable<void> fill(std::shared_ptr<Sink> sink,
                                   std::shared_ptr<Channel> channel) {
    for (;;) {
      auto [exception, block] =
          co_await sink->asyncGetNextBlock(net::as_tuple(net::use_awaitable));
      bool isLastValue = exception != nullptr || !block.has_value();
      auto [errorCode] = co_await channel->async_send(
          boost::system::error_code{}, std::move(exception), std::move(block),
          net::as_tuple(net::use_awaitable));
      if (errorCode || isLastValue) {
        break;
      }
    }
    // Release the sink before the filler reports that it is done, such that
    // the sink is no longer referenced by the read-ahead once `shutDown()`
    // returns.
    sink.reset();
  }

  // The completion handler of the filler: fulfill `fillerIsDone`. An
  // `exception` that has left the filler is stored there, and the channel is
  // closed, such that the consumer does not wait for a block that never comes
  // but rethrows that exception, see `getNextBlock()`.
  //
  // NOTE: The filler itself does not throw, the sink reports its exceptions as
  // values (see `fill()`), so this is merely a safety net. It may lose the
  // blocks that are still buffered, which does not matter on this path.
  static void onFillerDone(std::shared_ptr<Channel> channel,
                           std::shared_ptr<std::promise<void>> fillerIsDone,
                           std::exception_ptr exception) {
    if (exception == nullptr) {
      fillerIsDone->set_value();
      return;
    }
    fillerIsDone->set_exception(std::move(exception));
    channel->close();
  }

  // Return the exception that has left the filler, see `onFillerDone`.
  std::exception_ptr failureOfFiller() const {
    try {
      fillerIsDone_.get();
    } catch (...) {
      return std::current_exception();
    }
    return std::make_exception_ptr(
        std::runtime_error{"The read-ahead of the parallel merge has closed "
                           "its channel without an exception."});
  }
};

}  // namespace ad_utility::parallelBlockMerge::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_BLOCKPREFETCHER_H
