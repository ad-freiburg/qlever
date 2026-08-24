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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/OutputSinkPolicy.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"
#include "util/parallelBlockMerge/SchedulerPolicy.h"

// The Boost.Asio based `AsioParallelMergeState` at the bottom of this file
// requires coroutines and is therefore not available in the C++17 backports
// mode.
#ifndef QLEVER_CPP_17
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/error_code.hpp>

#include "util/AsyncSemaphore.h"
#endif

namespace ad_utility::parallelBlockMerge {
namespace detail {

// The state of a parallel merge, exposed as a lazy range of blocks. It owns
// *everything* that the concurrently running chunk tasks refer to (the input,
// the comparator, the sink, and the bookkeeping of the tasks), because
// `InOrderBlockSink::blocks()` only holds a raw pointer to the sink. Its
// destructor aborts the sink and then waits for all in-flight tasks, so that a
// consumer that abandons the range early cannot cause a use-after-free.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class ParallelMergeState
    : public ad_utility::InputRangeFromGet<typename Input::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  // See the `BlockSink` concept, which cannot express this requirement itself.
  static_assert(
      noexcept(std::declval<InOrderBlockSink<Block>&>().finishChunk(size_t{})),
      "`BlockSink::finishChunk` must be `noexcept`, because it is also called "
      "while cleaning up after a failed chunk");

  Input input_;
  Comparator comparator_;
  MergeOptions options_;
  SharedMergeScheduler scheduler_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  Splitters<Key> splitters_;
  size_t maxInFlight_;
  // NOTE: `blocks_` holds a raw pointer to `sink_`, so `sink_` has to be
  // declared (and therefore constructed) first.
  InOrderBlockSink<Block> sink_;
  ad_utility::InputRangeTypeErased<Block> blocks_;
  // Set by the destructor and by a failing chunk, so that the running tasks
  // stop as soon as possible.
  std::atomic<bool> stopRequested_{false};

  // The bookkeeping of the chunk tasks. NOTE: Two distinct counters are
  // required. `numChunksInFlight_` drives the dispatch policy and is decreased
  // as soon as a chunk is complete, such that the completing worker can
  // immediately dispatch the next chunk. `numActiveTasks_` in contrast counts
  // the tasks that may still touch this object at all (a task stays active
  // while it dispatches its successor) and is what the destructor waits for.
  std::mutex mutex_;
  // Notified whenever a single task becomes inactive, which is what the
  // destructor waits for.
  std::condition_variable taskFinished_;
  size_t nextChunkToDispatch_ = 0;
  size_t numChunksInFlight_ = 0;
  size_t numActiveTasks_ = 0;

 public:
  // Construct all the state of the merge. NOTE: The running tasks refer to this
  // object, which is why it is a `NoCopyNoMove`.
  ParallelMergeState(Input input, Comparator comparator, MergeOptions options,
                     SharedMergeScheduler scheduler,
                     ad_utility::SharedCancellationHandle cancellationHandle,
                     Splitters<Key> splitters, size_t maxInFlight)
      : input_{std::move(input)},
        comparator_{std::move(comparator)},
        options_{std::move(options)},
        scheduler_{std::move(scheduler)},
        cancellationHandle_{std::move(cancellationHandle)},
        splitters_{std::move(splitters)},
        maxInFlight_{maxInFlight},
        sink_{options_.bufferedBlocksPerChunk},
        blocks_{sink_.blocks()} {
    // The dispatch policy is what makes the merge deadlock-free, see
    // `InOrderBlockSink`. In particular, more than one chunk has to be in
    // flight, because otherwise a single chunk could fill its buffer and then
    // block forever.
    AD_CORRECTNESS_CHECK(maxInFlight_ > 1);
    AD_CORRECTNESS_CHECK(maxInFlight_ <= scheduler_->maxParallelism());
    AD_CORRECTNESS_CHECK(maxInFlight_ <= splitters_.numChunks());
    sink_.setNumChunks(splitters_.numChunks());
  }

  // Abort the sink (which unblocks all producers) and then wait for all
  // in-flight tasks, before any of the members they refer to is destroyed.
  ~ParallelMergeState() override {
    stopRequested_.store(true);
    sink_.abort();
    std::unique_lock lock{mutex_};
    taskFinished_.wait(lock, [this] { return numActiveTasks_ == 0; });
  }

  // Return the next block in the global order, or `std::nullopt` if the merge
  // is exhausted. Rethrow an exception from one of the chunk tasks.
  std::optional<Block> get() override {
    dispatchChunks();
    return blocks_.get();
  }

 private:
  // Dispatch chunks in strictly increasing order of their index, until
  // `maxInFlight_` of them are in flight. This is called from the consuming
  // thread (at the beginning of every `get()`) as well as from a worker that
  // has just completed a chunk. The strictly increasing order is preserved in
  // either case, because `nextChunkToDispatch_` is guarded by `mutex_`.
  void dispatchChunks() {
    while (true) {
      std::unique_lock lock{mutex_};
      if (stopRequested_.load() ||
          nextChunkToDispatch_ >= splitters_.numChunks() ||
          numChunksInFlight_ >= maxInFlight_) {
        return;
      }
      size_t chunkIndex = nextChunkToDispatch_;
      ++nextChunkToDispatch_;
      ++numChunksInFlight_;
      ++numActiveTasks_;
      // NOTE: The lock must not be held while scheduling, because a scheduler
      // may run the task inline.
      lock.unlock();
      try {
        scheduler_->schedule([this, chunkIndex] { runChunk(chunkIndex); });
      } catch (...) {
        // The task was never started, so undo the bookkeeping. Otherwise the
        // destructor would wait for a task that does not exist.
        markChunkAsFinished();
        markTaskAsFinished();
        throw;
      }
    }
  }

  // Account for a chunk that is complete, such that the next one may be
  // dispatched in its place.
  void markChunkAsFinished() {
    std::unique_lock lock{mutex_};
    AD_CORRECTNESS_CHECK(numChunksInFlight_ > 0);
    --numChunksInFlight_;
  }

  // Account for a task that will not touch this object anymore, and wake up the
  // destructor if it was the last one.
  void markTaskAsFinished() {
    std::unique_lock lock{mutex_};
    AD_CORRECTNESS_CHECK(numActiveTasks_ > 0);
    --numActiveTasks_;
    // NOTE: The notification has to happen while the lock is still held.
    // Otherwise the waiting destructor could return and destroy `taskFinished_`
    // while this thread is still inside `notify_all`, which is a
    // use-after-free.
    taskFinished_.notify_all();
  }

  // Merge a single chunk and push its blocks to the sink. This never throws
  // (`TaskQueue::push` would call `std::terminate`), all exceptions are
  // forwarded to the consumer via `InOrderBlockSink::pushException`.
  void runChunk(size_t chunkIndex) noexcept {
    ad_utility::terminateIfThrows(
        [this, chunkIndex] { runChunkImpl(chunkIndex); },
        "Merging a single chunk in `parallelBlockMergeToRange` failed.");
  }

  // The implementation of `runChunk`, see there. This must not throw.
  void runChunkImpl(size_t chunkIndex) {
    try {
      ChunkMerger<moveElements, Input, Comparator> merger{
          &input_, &comparator_, options_,
          splitters_.getSplittersAt(chunkIndex), cancellationHandle_};
      while (!stopRequested_.load()) {
        auto block = merger.get();
        if (!block.has_value()) {
          break;
        }
        sink_(chunkIndex, std::move(block.value()));
      }
    } catch (...) {
      stopRequested_.store(true);
      sink_.pushException(std::current_exception());
    }
    // NOTE: `finishChunk` has to be called on every path, because the consumer
    // would hang otherwise.
    sink_.finishChunk(chunkIndex);
    markChunkAsFinished();
    // Top up the pipeline from the completing worker. This is required for
    // correctness and not only for throughput: a chunk that yields no output
    // block at all leaves the consumer parked inside
    // `InOrderBlockSink::popNextBlock`, where it cannot dispatch anything, so
    // the successors of that chunk would never be dispatched and the merge
    // would hang.
    //
    // NOTE: This relies on `MergeScheduler::schedule` not running the task in
    // the calling thread, because otherwise the dispatching would recurse once
    // per chunk. The constructor checks that the scheduler offers a parallelism
    // greater than one, which excludes the only inline scheduler that exists
    // (`InlineMergeScheduler`).
    try {
      dispatchChunks();
    } catch (...) {
      stopRequested_.store(true);
      sink_.pushException(std::current_exception());
    }
    markTaskAsFinished();
  }
};
#ifndef QLEVER_CPP_17
// ___________________________________________________________________________
// The Boost.Asio based alternative to `ParallelMergeState`.
// ___________________________________________________________________________

// The state of a parallel merge that schedules *all* of its work on a
// Boost.Asio executor. It plays the same role as `ParallelMergeState` above,
// but instead of occupying one thread per in-flight chunk it runs one coroutine
// per chunk, so that a chunk which currently cannot make progress (because the
// consumer has not caught up yet) releases its thread. The corresponding
// back-pressure and the reordering of the blocks live in
// `AsioInOrderBlockSink`, see there.
//
// The number of chunks that are merged concurrently is bounded by
// `maxInFlight`, which is enforced by the `semaphore_`. This bound is a pure
// *memory* bound (every live chunk holds one input block per run plus its heap)
// and, in contrast to `ParallelMergeState`, no longer a correctness
// requirement: a value of `1` and a value that far exceeds the available
// parallelism are both perfectly fine.
//
// STRAND CONFINEMENT: The dispatch loop (`dispatchNextChunk` and its
// continuation) as well as the teardown in `abort()` run on `strand_`, which
// this class owns. The `sink_` and the `semaphore_` each confine their own
// state to a strand of their own, so no state is ever shared between the three
// and none of them has to know about the strands of the others.
//
// IMPORTANT: The merging itself must *not* run on any of those strands, because
// everything that runs on a strand is serialized. `runChunk` is therefore
// spawned on `executor_` and only hops to the strand of the `sink_` for the
// short bookkeeping of an `asyncPush`, see the note there.
//
// LIFETIME: There is deliberately no destructor that waits for the coroutines
// and handlers that are still in flight. Instead, every one of them holds a
// `shared_ptr` to this object, so that this object simply outlives all of them;
// this is also why it can only be created via `create()`. A consumer that
// abandons the merge has to call `abort()`, so that those coroutines actually
// finish instead of waiting for a consumer that is gone.
// `AsioParallelMergeRange` below does this in its destructor.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class AsioParallelMergeState
    : public std::enable_shared_from_this<
          AsioParallelMergeState<moveElements, Input, Comparator>>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;
  using Sink = AsioInOrderBlockSink<Block>;
  // The strand to which the dispatch loop and the teardown are confined, see
  // the STRAND CONFINEMENT note above.
  using Strand = net::strand<net::any_io_executor>;
  // The completion token that the asynchronous operations below use if the
  // caller does not specify one.
  using DefaultToken = typename Sink::DefaultToken;

 private:
  // A tag that makes the constructor unusable from the outside, such that a
  // `AsioParallelMergeState` can only be created via `create()` and hence only
  // ever exists inside a `shared_ptr`, see the LIFETIME note above.
  struct PrivateTag {};

  net::any_io_executor executor_;
  Input input_;
  Comparator comparator_;
  MergeOptions options_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  Splitters<Key> splitters_;
  size_t maxInFlight_;
  // NOTE: The order of these members matters, all three of them are initialized
  // from the members above.
  Strand strand_;
  Sink sink_;
  // The counting semaphore that bounds the number of chunks that are merged
  // concurrently. A chunk coroutine is only spawned once a permit could be
  // taken out, and holds that permit until it is done.
  ad_utility::AsyncSemaphore semaphore_;

 public:
  // Create the state of a merge and start dispatching its chunks. All the work
  // is scheduled on the `executor`, which somebody else has to run.
  static std::shared_ptr<AsioParallelMergeState> create(
      net::any_io_executor executor, Input input, Comparator comparator,
      MergeOptions options,
      ad_utility::SharedCancellationHandle cancellationHandle,
      Splitters<Key> splitters, size_t maxInFlight) {
    auto self = std::make_shared<AsioParallelMergeState>(
        PrivateTag{}, std::move(executor), std::move(input),
        std::move(comparator), std::move(options),
        std::move(cancellationHandle), std::move(splitters), maxInFlight);
    // NOTE: The dispatching can only be started once the `shared_ptr` exists,
    // because the coroutines and handlers keep this object alive via
    // `shared_from_this`. It runs on `strand_`, see `dispatchNextChunk`.
    net::post(self->strand_, [self] { self->dispatchNextChunk(0); });
    return self;
  }

  // The constructor is effectively private, use `create()` instead.
  AsioParallelMergeState(
      PrivateTag, net::any_io_executor executor, Input input,
      Comparator comparator, MergeOptions options,
      ad_utility::SharedCancellationHandle cancellationHandle,
      Splitters<Key> splitters, size_t maxInFlight)
      : executor_{std::move(executor)},
        input_{std::move(input)},
        comparator_{std::move(comparator)},
        options_{std::move(options)},
        cancellationHandle_{std::move(cancellationHandle)},
        splitters_{std::move(splitters)},
        maxInFlight_{maxInFlight},
        strand_{net::make_strand(executor_)},
        sink_{executor_, splitters_.numChunks(),
              options_.bufferedBlocksPerChunk},
        semaphore_{executor_, maxInFlight} {
    AD_CORRECTNESS_CHECK(maxInFlight_ > 0);
    AD_CORRECTNESS_CHECK(maxInFlight_ <= splitters_.numChunks());
  }

  // Complete with the next block in the global order, or with `std::nullopt` if
  // the merge is exhausted or was aborted. Rethrow an exception of one of the
  // chunks. Suspend until the next block is available.
  //
  // NOTE: Run this from a single consumer only, and never concurrently with
  // itself.
  template <typename CompletionToken = DefaultToken>
  auto asyncNext(CompletionToken&& completionToken = {}) {
    // NOTE: This is nothing but a forwarding of the completion token to the
    // sink, plus a `shared_ptr` that is consigned to the completion handler and
    // hence keeps this object alive for as long as the consumer is inside the
    // sink.
    return sink_.asyncGetNextBlock(
        net::consign(AD_FWD(completionToken), this->shared_from_this()));
  }

  // Stop the merge, so that no coroutine is left waiting for a consumer that is
  // gone. The blocks that are still buffered are dropped and `asyncNext()`
  // yields `std::nullopt` from now on. NOTE: This returns immediately, it does
  // *not* wait for the coroutines that are still in flight, see the LIFETIME
  // note above.
  void abort() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          // NOTE: This function is called synchronously from
          // `~AsioParallelMergeRange`, i.e. from a thread that typically runs
          // none of the strands involved, and it must not block. Both calls
          // below therefore only *initiate* the teardown on the respective
          // strand and return immediately. The `shared_ptr` that is consigned
          // to the first one is required because the caller drops its own
          // `shared_ptr` right after this call; the `semaphore_` in contrast
          // keeps its state alive itself.
          sink_.asyncAbort(
              net::consign(net::detached, this->shared_from_this()));
          // Wake up the dispatch loop if it currently waits for a free permit.
          // It sees the stop afterwards and never waits again, so the
          // cancellation does not have to be sticky, see
          // `ad_utility::AsyncSemaphore::cancel`.
          //
          // NOTE: The two halves of this teardown run on different strands and
          // are hence not atomic with respect to the dispatch loop, which may
          // therefore still dispatch a few chunks in between. That is benign:
          // such a chunk sees the stop in its very first `stopRequested()` and
          // returns its permit right away, so the loop runs through the
          // remaining chunk indices and terminates.
          semaphore_.cancel();
        },
        "Aborting an `AsioParallelMergeState` failed.");
  }

 private:
  // Dispatch the chunk with the given `chunkIndex`, and recursively all the
  // following ones: wait for a free permit of the `semaphore_`, and continue in
  // `spawnChunkAndContinue` as soon as one was taken out. Do nothing if all
  // chunks were dispatched or the merge was stopped.
  //
  // NOTE: This deliberately uses `AsyncSemaphore::asyncAcquire` and not
  // `ad_utility::asyncWithPermit`, because the loop has to continue as soon as
  // the permit was *acquired* and not when the chunk is done. It therefore
  // needs the permit as an explicit RAII handle that it can hand to the chunk.
  //
  // NOTE: This recursion does not accumulate stack space. Boost.Asio never
  // invokes a completion handler from within the initiating function, so
  // `spawnChunkAndContinue`, and with it the next iteration, always runs on a
  // fresh stack.
  //
  // PRECONDITION: This runs on `strand_`, and so does its continuation.
  void dispatchNextChunk(size_t chunkIndex) noexcept {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (chunkIndex >= splitters_.numChunks() || sink_.stopRequested()) {
      return;
    }
    // NOTE: The only thing that can throw here is the allocation of the
    // completion handler, in which case the merge cannot continue at all.
    try {
      semaphore_.asyncAcquire(net::bind_executor(
          strand_, [self = this->shared_from_this(), chunkIndex](
                       const boost::system::error_code& errorCode,
                       ad_utility::AsyncSemaphore::Permit permit) {
            self->spawnChunkAndContinue(errorCode, std::move(permit),
                                        chunkIndex);
          }));
    } catch (...) {
      forwardExceptionToConsumer(std::current_exception());
    }
  }

  // The continuation of `dispatchNextChunk`: spawn the coroutine that merges
  // the chunk with the given `chunkIndex` and immediately continue the loop
  // with the next chunk. Stop dispatching if the wait for the permit was
  // cancelled (`errorCode`) or the merge was stopped in the meantime.
  //
  // The chunks have to be dispatched in strictly increasing order of their
  // index: a chunk holds its permit while its producer is suspended on a full
  // channel, so if a higher chunk could start before a lower one, all permits
  // could be held by producers of chunks that the consumer does not read yet,
  // and the merge would deadlock.
  //
  // PRECONDITION: This runs on `strand_`.
  void spawnChunkAndContinue(const boost::system::error_code& errorCode,
                             ad_utility::AsyncSemaphore::Permit permit,
                             size_t chunkIndex) noexcept {
    AD_CORRECTNESS_CHECK(strand_.running_in_this_thread());
    if (errorCode || sink_.stopRequested()) {
      return;
    }
    // NOTE: The only thing that can throw here is the allocation of the
    // coroutine frame of the chunk, in which case the merge cannot continue.
    try {
      // NOTE: The chunk is deliberately spawned on `executor_` and never on a
      // strand, see `runChunk`. The `permit` is moved into the completion
      // handler of that coroutine and is hence returned to the `semaphore_` as
      // soon as the chunk is done, so that the next chunk may take its place.
      net::co_spawn(executor_, runChunk(chunkIndex),
                    [permit = std::move(permit)](std::exception_ptr) {});
    } catch (...) {
      forwardExceptionToConsumer(std::current_exception());
      return;
    }
    dispatchNextChunk(chunkIndex + 1);
  }

  // Forward an `exception` that was thrown while dispatching to the consumer,
  // which also stops the chunks that are already running.
  //
  // NOTE: The forwarding allocates a coroutine frame of its own, so the only
  // way it can fail is that memory is exhausted, in which case the consumer
  // sees the end of the range instead of the exception.
  void forwardExceptionToConsumer(std::exception_ptr exception) noexcept {
    ad_utility::ignoreExceptionIfThrows(
        [this, &exception] {
          sink_.asyncPushException(
              std::move(exception),
              net::consign(net::detached, this->shared_from_this()));
        },
        "Forwarding an exception of the chunk dispatcher to the consumer of "
        "an `AsioParallelMergeState` failed.");
  }

  // Merge a single chunk and push its blocks to the sink. This never throws,
  // all exceptions are forwarded to the consumer via
  // `AsioInOrderBlockSink::asyncPushException`.
  net::awaitable<void> runChunk(size_t chunkIndex) {
    auto self = this->shared_from_this();
    std::exception_ptr chunkException;
    try {
      ChunkMerger<moveElements, Input, Comparator> merger{
          &input_, &comparator_, options_,
          splitters_.getSplittersAt(chunkIndex), cancellationHandle_};
      while (!sink_.stopRequested()) {
        // NOTE: Merging a single output block is ordinary blocking work that
        // may even do I/O, so a chunk occupies its thread for the duration of
        // one output block. This is exactly why this coroutine runs on
        // `executor_` and *not* on a strand: everything that runs on a strand
        // is serialized, so merging there would silently destroy all the
        // parallelism. Only the short bookkeeping of the `asyncPush` below hops
        // to the strand of the sink, and that is also the only suspension
        // point.
        auto block = merger.get();
        if (!block.has_value()) {
          break;
        }
        if (!co_await sink_.asyncPush(chunkIndex, std::move(block.value()))) {
          break;
        }
      }
    } catch (...) {
      // NOTE: The exception is only stashed here and forwarded below, because
      // `co_await` must not appear in an exception handler.
      chunkException = std::current_exception();
    }
    if (chunkException != nullptr) {
      try {
        co_await sink_.asyncPushException(std::move(chunkException));
      } catch (...) {
        // `asyncPushException` allocates a coroutine frame, so the only way it
        // can fail is that memory is exhausted. Ignore that, because the
        // `asyncFinishChunk` below has to run in any case.
      }
    }
    // NOTE: The end-of-chunk sentinel has to be sent on every path, because the
    // consumer would wait for this chunk forever otherwise.
    try {
      co_await sink_.asyncFinishChunk(chunkIndex);
    } catch (...) {
      // `asyncFinishChunk` reports a stopped merge via its result and not via
      // an exception, so this can only be an allocation failure, and the merge
      // is being torn down anyway.
    }
  }
};

// A synchronous adapter for an `AsioParallelMergeState`: a lazy range of blocks
// whose `get()` blocks the calling thread until the next block is available.
// Use this to plug the Boost.Asio based merge into a consumer that is not
// itself asynchronous.
//
// IMPORTANT: The executor of the merge has to be run by *other* threads (for
// example by a `boost::asio::thread_pool`), because the thread that iterates
// over this range is blocked while it waits for the next block and can
// therefore not run any of the merge's coroutines itself.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class AsioParallelMergeRange
    : public ad_utility::InputRangeFromGet<typename Input::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using State = AsioParallelMergeState<moveElements, Input, Comparator>;

 private:
  std::shared_ptr<State> state_;

 public:
  // Construct from the `state` of a merge that was already started, see
  // `AsioParallelMergeState::create`.
  explicit AsioParallelMergeRange(std::shared_ptr<State> state)
      : state_{std::move(state)} {
    AD_CONTRACT_CHECK(state_ != nullptr);
  }

  // Stop the merge, such that the coroutines that are still in flight finish
  // instead of waiting for a consumer that is gone.
  ~AsioParallelMergeRange() override { state_->abort(); }

  // Return the next block of the merge, or `std::nullopt` at its end.
  //
  // IMPORTANT: This is *synchronous and blocking*: it waits on a `future` until
  // the next block is available and hence occupies its thread for that whole
  // time. It therefore deadlocks if it is called from one of the threads that
  // run the executor of the merge, because that thread is then no longer
  // available to run the coroutines that produce the very block it waits for.
  // In the extreme case of a single-threaded executor the *first* call already
  // deadlocks. See also the IMPORTANT note in the class comment above.
  std::optional<Block> get() override {
    return state_->asyncNext(net::use_future).get();
  }
};
#endif  // QLEVER_CPP_17

}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGESTATE_H
