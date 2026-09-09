// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGESTATE_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGESTATE_H

// The parallel driver of the block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`). Its asynchrony is expressed
// with Boost.Asio coroutines, so this whole header is only available in C++20
// mode and empty when `QLEVER_REDUCED_FEATURE_SET_FOR_CPP17` is set. The
// *serial* merge is unaffected and available in both modes.
#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include "backports/asio.h"
#include "backports/concepts.h"
#include "util/AsyncResourcePool.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/BlockSinkPolicy.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {

namespace net = boost::asio;

namespace detail {

// A fresh cancellation handle, which is the default for a caller of the
// parallel merge that does not want to cancel it. NOTE: The merge requires a
// handle that is not `nullptr`, see `MergeState`.
inline ad_utility::SharedCancellationHandle freshCancellationHandle() {
  return std::make_shared<ad_utility::CancellationHandle<>>();
}

// Await the asynchronous `operation` and return the exception that it threw, or
// `nullptr` if it ran through. The result of a successful `operation` is
// deliberately discarded, so this is for the operations whose outcome does not
// change what the caller does next.
//
// NOTE: This is a coroutine of its own (and not simply a `try` block around the
// `co_await`) because an `await-expression` must not appear inside an exception
// handler, see [expr.await]. A coroutine that has to *continue* after a failed
// operation therefore cannot deal with that failure in place, and has to
// capture the exception first.
template <typename Awaitable>
net::awaitable<std::exception_ptr> exceptionOf(Awaitable operation) {
  try {
    co_await std::move(operation);
  } catch (...) {
    co_return std::current_exception();
  }
  co_return nullptr;
}

// Log the `exception` (if there is one) together with the `note` instead of
// propagating it, see `ad_utility::ignoreExceptionIfThrows`. This is what the
// teardown of a merge does with an exception that it cannot report any more.
inline void logIgnoredException(std::exception_ptr exception,
                                std::string_view note) noexcept {
  if (exception == nullptr) {
    return;
  }
  ad_utility::ignoreExceptionIfThrows(
      [&exception] { std::rethrow_exception(exception); }, note);
}

// The state of a parallel merge that schedules *all* of its work on a
// Boost.Asio executor. It runs one coroutine per chunk, and a chunk which
// currently cannot make progress (because the sink has not caught up yet)
// suspends and releases its thread instead of blocking it. The output blocks
// are pushed to the `Sink`, which is where the back-pressure and the order of
// the blocks live, see `SinkConcept`.
//
// The number of chunks that are merged concurrently is bounded by
// `maxNumChunksInFlight`, which is enforced by the `semaphore_`. This bound is
// a pure *memory* bound (every live chunk holds one input block per run plus
// its heap) and not a correctness requirement: a value of `1` and a value that
// far exceeds the available parallelism are both perfectly fine.
//
// EXECUTOR: Every coroutine of this class — the dispatch loop
// (`dispatchChunks`) as well as the chunks (`runChunk`) — runs on `executor_`
// and on no strand at all. There deliberately is no strand of this class: the
// dispatch loop is a single coroutine and hence already serialized with itself,
// everything else that this class owns is immutable after construction, and the
// `sink_` and the `semaphore_` synchronize their own state themselves — the
// `sink_` by a strand or a mutex of its own, the `semaphore_` by its internal
// `concurrent_channel`, see `ad_utility::AsyncResourcePool`. The teardown in
// `stop()` in turn runs on no executor of this class at all, see there.
//
// IMPORTANT: The merging itself must not run on the strand of the sink either,
// because everything that runs on a strand is serialized. This is guaranteed
// because every operation of the sink is awaited with a completion token whose
// associated executor is `executor_` (namely the executor of the awaiting
// coroutine), and because a sink never completes an operation inline, see the
// IMPORTANT note at `SinkConcept`.
//
// LIFETIME: There is deliberately no destructor that waits for the coroutines
// that are still in flight. Instead, every one of them holds a `shared_ptr` to
// this object (which `net::consign` attaches to it), so that this object simply
// outlives all of them; this is also why it can only be created via `create()`.
// This object in turn holds the `shared_ptr` to the `sink_`, which therefore
// outlives them as well. A consumer that abandons the merge has to call
// `stop()`, so that those coroutines actually finish instead of waiting for a
// consumer that is gone.
CPP_template(bool moveElements, typename Input, typename Comparator,
             typename Sink)(
    requires InputConcept<Input> CPP_and
        SinkConcept<Sink, typename Input::Block>) class ParallelMergeState
    : public std::enable_shared_from_this<
          ParallelMergeState<moveElements, Input, Comparator, Sink>>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  // The merger of a single chunk, and the state that the mergers of all chunks
  // share (see `ChunkMerger`). The latter is the single owner of the input, the
  // comparator, the options, the cancellation handle, and the chunk boundaries
  // of the merge.
  using Merger = ChunkMerger<moveElements, Input, Comparator>;
  using SharedMergeState = typename Merger::State;
  // The counting semaphore that bounds the number of chunks that are merged
  // concurrently, and one of its permits. A resource pool without resources is
  // exactly a counting semaphore, see `ad_utility::AsyncResourcePool`.
  using Semaphore = ad_utility::AsyncResourcePool<void>;
  using Permit = Semaphore::Handle;

 private:
  // A tag that makes the constructor unusable from the outside, such that a
  // `ParallelMergeState` can only be created via `create()` and hence only ever
  // exists inside a `shared_ptr`, see the LIFETIME note above.
  struct PrivateTag {};

  ql::any_io_executor executor_;
  // Both are never `nullptr`, see `create()` below.
  std::shared_ptr<const SharedMergeState> mergeState_;
  std::shared_ptr<Sink> sink_;
  size_t maxNumChunksInFlight_;
  // The semaphore that bounds the number of chunks that are merged
  // concurrently. A chunk coroutine is only spawned once a permit could be
  // taken out, and holds that permit until it is done.
  //
  // NOTE: The order of the members matters, this one is initialized from
  // `executor_` above.
  Semaphore semaphore_;

 public:
  // The constructor is effectively private, use `create()` instead.
  ParallelMergeState(PrivateTag, ql::any_io_executor executor,
                     std::shared_ptr<const SharedMergeState> mergeState,
                     std::shared_ptr<Sink> sink, size_t maxNumChunksInFlight)
      : executor_{std::move(executor)},
        mergeState_{std::move(mergeState)},
        sink_{std::move(sink)},
        maxNumChunksInFlight_{maxNumChunksInFlight},
        semaphore_{executor_, maxNumChunksInFlight} {
    AD_CORRECTNESS_CHECK(maxNumChunksInFlight_ > 0);
    AD_CORRECTNESS_CHECK(maxNumChunksInFlight_ <= numChunks());
  }

  // Create the state of a merge and start dispatching its chunks. All the work
  // is scheduled on the `executor`, which somebody else has to run. The
  // `mergeState` and the `sink` must not be `nullptr`, and the `sink` has to
  // expect exactly `mergeState->chunkBoundaries_.size()` chunks.
  static std::shared_ptr<ParallelMergeState> create(
      ql::any_io_executor executor,
      std::shared_ptr<const SharedMergeState> mergeState,
      std::shared_ptr<Sink> sink, size_t maxNumChunksInFlight) {
    AD_CORRECTNESS_CHECK(mergeState != nullptr);
    AD_CORRECTNESS_CHECK(sink != nullptr);
    auto self = std::make_shared<ParallelMergeState>(
        PrivateTag{}, std::move(executor), std::move(mergeState),
        std::move(sink), maxNumChunksInFlight);
    // NOTE: The dispatching can only be started once the `shared_ptr` exists,
    // because the `consign` is what keeps this object alive for as long as the
    // dispatch loop runs, see the LIFETIME note above.
    net::co_spawn(self->executor_, self->dispatchChunks(),
                  net::consign(net::detached, self));
    return self;
  }

  // The number of chunks that this merge consists of, see
  // `computeChunkBoundaries`. Always at least one.
  size_t numChunks() const { return mergeState_->chunkBoundaries_.size(); }

  // Stop the merge, so that no coroutine is left waiting for a consumer that is
  // gone. NOTE: This returns immediately, it does *not* wait for the coroutines
  // that are still in flight, see the LIFETIME note above.
  void stop() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          // NOTE: This function may be called synchronously from a thread that
          // runs none of the executors involved, and it must not block. Neither
          // call below does: `asyncStop` only *initiates* the teardown on the
          // strand of the sink and returns immediately, and
          // `AsyncResourcePool::cancel` cancels the channel of the `semaphore_`
          // right in the calling thread, without a hop onto any executor. The
          // `shared_ptr` that is consigned to the first one is required because
          // the caller may drop its own `shared_ptr` right after this call; the
          // `semaphore_` in contrast keeps its state alive itself.
          sink_->asyncStop(
              net::consign(net::detached, this->shared_from_this()));
          // Wake up the dispatch loop if it currently waits for a free permit.
          // It sees the stop afterwards and never waits again, so the
          // cancellation does not have to be sticky, see
          // `ad_utility::AsyncResourcePool::cancel`.
          //
          // NOTE: Neither half of this teardown is synchronized with the
          // dispatch loop, which may therefore still dispatch a few chunks in
          // between. That is benign: such a chunk sees the stop in its very
          // first `stopRequested()` and returns its permit right away, so the
          // loop runs through the remaining chunk indices and terminates.
          semaphore_.cancel();
        },
        "Stopping a `ParallelMergeState` failed.");
  }

 private:
  // The dispatch loop: spawn one coroutine per chunk, in strictly increasing
  // order of the chunk index and never more than `maxNumChunksInFlight_` of
  // them at a time. Forward an exception of the loop itself to the sink, which
  // also stops the chunks that are already running.
  //
  // NOTE: The forwarding typically allocates a completion handler of its own,
  // so the only way it can fail is that memory is exhausted, in which case the
  // consumer sees the end of the output instead of the exception.
  net::awaitable<void> dispatchChunks() {
    std::exception_ptr exception = co_await exceptionOf(dispatchChunksImpl());
    co_await forwardExceptionToSink(
        std::move(exception),
        "Forwarding an exception of the chunk dispatcher to the sink of a "
        "`ParallelMergeState` failed.");
  }

  // Forward the `exception` to the sink, or do nothing if there is none.
  // Pushing an exception to the sink can itself fail, in which case that
  // failure is logged together with the `note` instead of being propagated,
  // because there is nobody left to report it to.
  //
  // NOTE: Pushing the exception also stops the merge, so that the chunks that
  // are still running do not keep producing blocks that nobody wants any more.
  net::awaitable<void> forwardExceptionToSink(std::exception_ptr exception,
                                              std::string_view note) {
    if (exception == nullptr) {
      co_return;
    }
    logIgnoredException(co_await exceptionOf(sink_->asyncPushException(
                            std::move(exception), net::use_awaitable)),
                        note);
  }

  // The actual dispatch loop, see `dispatchChunks` above. Stop dispatching as
  // soon as the merge was stopped, either directly (`stopRequested()`) or via
  // the cancellation of the wait for a permit (`errorCode`).
  //
  // The chunks have to be dispatched in strictly increasing order of their
  // index: a chunk holds its permit while it is suspended on a sink that has no
  // room, so if a higher chunk could start before a lower one, all permits
  // could be held by chunks that the consumer does not read yet, and the merge
  // would deadlock.
  net::awaitable<void> dispatchChunksImpl() {
    for (size_t chunkIndex = 0; chunkIndex < numChunks(); ++chunkIndex) {
      if (sink_->stopRequested()) {
        co_return;
      }
      // NOTE: This deliberately uses `Semaphore::asyncAcquire` and not
      // `ad_utility::asyncWithResource`, because the loop has to continue as
      // soon as the permit was *acquired* and not when the chunk is done. It
      // therefore needs the permit as an explicit RAII handle that it can hand
      // to the chunk.
      auto [errorCode, permit] =
          co_await semaphore_.asyncAcquire(net::as_tuple(net::use_awaitable));
      if (errorCode || sink_->stopRequested()) {
        co_return;
      }
      // NOTE: The `permit` is owned by the chunk coroutine and is hence
      // returned to the `semaphore_` as soon as that chunk is done, so that the
      // next chunk may take its place.
      net::co_spawn(executor_, runChunk(chunkIndex, std::move(permit)),
                    net::consign(net::detached, this->shared_from_this()));
    }
  }

  // Merge the chunk with the given `chunkIndex` and push all of its output
  // blocks to the sink, then forward the exception of the chunk (if there is
  // one) and announce the end of the chunk. This coroutine never propagates an
  // exception, everything is reported to the sink or logged.
  //
  // The `permit` is deliberately unused: this coroutine holds it for its whole
  // lifetime and hence returns it to the `semaphore_` exactly when the chunk is
  // done, see `dispatchChunksImpl` above.
  net::awaitable<void> runChunk(size_t chunkIndex,
                                [[maybe_unused]] Permit permit) {
    // Yield the thread before any actual work is done. This hop is essential
    // and easy to overlook: `co_spawn` starts a coroutine *inline* via
    // `dispatch()` on the spawning thread, so without it the dispatch loop
    // would be blocked while this chunk merges its very first output block.
    co_await net::post(executor_, net::use_awaitable);
    std::exception_ptr exception =
        co_await exceptionOf(mergeAndPushChunk(chunkIndex));
    co_await forwardExceptionToSink(
        std::move(exception),
        "Forwarding the exception of a chunk to the sink of a "
        "`ParallelMergeState` failed.");
    // NOTE: The end-of-chunk sentinel has to be sent on every path, because a
    // sink that waits for this chunk would wait forever otherwise.
    logIgnoredException(co_await exceptionOf(sink_->asyncFinishChunk(
                            chunkIndex, net::use_awaitable)),
                        "Finishing a chunk of a `ParallelMergeState` failed.");
  }

  // Merge the output blocks of the chunk with the given `chunkIndex` and push
  // them to the sink, one after the other, until the chunk is exhausted or the
  // merge is stopped. Propagate the first exception, be it one of the merging
  // itself or one that the sink reported.
  net::awaitable<void> mergeAndPushChunk(size_t chunkIndex) {
    Merger merger{mergeState_, chunkIndex};
    while (!sink_->stopRequested()) {
      // NOTE: Merging a single output block is ordinary blocking work that may
      // even do I/O, so a chunk occupies its thread for the duration of one
      // output block. It runs on `executor_` and never on a strand, see the
      // IMPORTANT note at the class comment above.
      std::optional<Block> block = merger.get();
      if (!block.has_value()) {
        co_return;
      }
      bool keepGoing = co_await sink_->asyncPush(
          chunkIndex, std::move(block).value(), net::use_awaitable);
      if (!keepGoing) {
        co_return;
      }
    }
  }
};

// The `ParallelMergeState` of a merge that pushes its output blocks to the sink
// which a `SinkFactory` creates. This alias exists only to keep the return type
// of `parallelBlockMergeToSink` readable.
template <bool moveElements, typename Input, typename Comparator,
          typename SinkFactory>
using ParallelMergeStateFor =
    ParallelMergeState<moveElements, Input, Comparator,
                       SinkFromFactoryT<SinkFactory>>;

}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGESTATE_H
