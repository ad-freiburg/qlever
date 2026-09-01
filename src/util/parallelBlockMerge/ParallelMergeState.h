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

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/consign.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/error_code.hpp>
#include <cstddef>
#include <exception>
#include <memory>
#include <optional>
#include <utility>

#include "backports/concepts.h"
#include "util/AsyncSemaphore.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Forward.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/InOrderBlockSink.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"

namespace ad_utility::parallelBlockMerge {
namespace detail {

// The state of a parallel merge that schedules *all* of its work on a
// Boost.Asio executor. It runs one task per chunk, and a chunk which currently
// cannot make progress (because the consumer has not caught up yet) releases
// its thread instead of blocking it. The corresponding back-pressure and the
// reordering of the blocks live in `InOrderBlockSink`, see there.
//
// The number of chunks that are merged concurrently is bounded by
// `maxInFlight`, which is enforced by the `semaphore_`. This bound is a pure
// *memory* bound (every live chunk holds one input block per run plus its heap)
// and not a correctness requirement: a value of `1` and a value that far
// exceeds the available parallelism are both perfectly fine.
//
// STRAND CONFINEMENT: The dispatch loop (`dispatchNextChunk` and its
// continuation) as well as the teardown in `abort()` run on `strand_`, which
// this class owns. The `sink_` and the `semaphore_` each confine their own
// state to a strand of their own, so no state is ever shared between the three
// and none of them has to know about the strands of the others.
//
// IMPORTANT: The merging itself must *not* run on any of those strands, because
// everything that runs on a strand is serialized. A `ChunkTask` therefore runs
// on `executor_` throughout and only hops to the strand of the `sink_` for the
// short bookkeeping of an `asyncPush`, see the note there.
//
// LIFETIME: There is deliberately no destructor that waits for the tasks and
// handlers that are still in flight. Instead, every one of them holds a
// `shared_ptr` to this object, so that this object simply outlives all of them;
// this is also why it can only be created via `create()`. A consumer that
// abandons the merge has to call `abort()`, so that those tasks actually finish
// instead of waiting for a consumer that is gone. `ParallelMergeRange` below
// does this in its destructor.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class ParallelMergeState
    : public std::enable_shared_from_this<
          ParallelMergeState<moveElements, Input, Comparator>>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;
  using Sink = InOrderBlockSink<Block>;
  // The strand to which the dispatch loop and the teardown are confined, see
  // the STRAND CONFINEMENT note above.
  using Strand = net::strand<net::any_io_executor>;

 private:
  // A tag that makes the constructor unusable from the outside, such that a
  // `ParallelMergeState` can only be created via `create()` and hence only ever
  // exists inside a `shared_ptr`, see the LIFETIME note above.
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
  // concurrently. A chunk task is only posted once a permit could be taken out,
  // and holds that permit until it is done.
  ad_utility::AsyncSemaphore semaphore_;

 public:
  // Create the state of a merge and start dispatching its chunks. All the work
  // is scheduled on the `executor`, which somebody else has to run.
  static std::shared_ptr<ParallelMergeState> create(
      net::any_io_executor executor, Input input, Comparator comparator,
      MergeOptions options,
      ad_utility::SharedCancellationHandle cancellationHandle,
      Splitters<Key> splitters, size_t maxInFlight,
      BlockStorageFactory<Block> blockStorageFactory = {}) {
    auto self = std::make_shared<ParallelMergeState>(
        PrivateTag{}, std::move(executor), std::move(input),
        std::move(comparator), std::move(options),
        std::move(cancellationHandle), std::move(splitters), maxInFlight,
        std::move(blockStorageFactory));
    // NOTE: The dispatching can only be started once the `shared_ptr` exists,
    // because the tasks and handlers keep this object alive via
    // `shared_from_this`. It runs on `strand_`, see `dispatchNextChunk`.
    net::post(self->strand_, [self] { self->dispatchNextChunk(0); });
    return self;
  }

  // The constructor is effectively private, use `create()` instead.
  ParallelMergeState(PrivateTag, net::any_io_executor executor, Input input,
                     Comparator comparator, MergeOptions options,
                     ad_utility::SharedCancellationHandle cancellationHandle,
                     Splitters<Key> splitters, size_t maxInFlight,
                     BlockStorageFactory<Block> blockStorageFactory)
      : executor_{std::move(executor)},
        input_{std::move(input)},
        comparator_{std::move(comparator)},
        options_{std::move(options)},
        cancellationHandle_{std::move(cancellationHandle)},
        splitters_{std::move(splitters)},
        maxInFlight_{maxInFlight},
        strand_{net::make_strand(executor_)},
        // NOTE: An empty `blockStorageFactory` means "keep the blocks in
        // memory", which is what bounds the memory consumption of the merge via
        // back-pressure, see `InMemoryBlockStorage`.
        sink_{executor_, splitters_.numChunks(),
              blockStorageFactory ? std::move(blockStorageFactory)
                                  : Sink::makeInMemoryStorageFactory(
                                        options_.bufferedBlocksPerChunk)},
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
  template <typename CompletionToken>
  auto asyncNext(CompletionToken&& completionToken) {
    // NOTE: This is nothing but a forwarding of the completion token to the
    // sink, plus a `shared_ptr` that is consigned to the completion handler and
    // hence keeps this object alive for as long as the consumer is inside the
    // sink.
    return sink_.asyncGetNextBlock(
        net::consign(AD_FWD(completionToken), this->shared_from_this()));
  }

  // Stop the merge, so that no task is left waiting for a consumer that is
  // gone. The blocks that are still buffered are dropped and `asyncNext()`
  // yields `std::nullopt` from now on. NOTE: This returns immediately, it does
  // *not* wait for the tasks that are still in flight, see the LIFETIME note
  // above.
  void abort() noexcept {
    ad_utility::terminateIfThrows(
        [this] {
          // NOTE: This function is called synchronously from
          // `~ParallelMergeRange`, i.e. from a thread that typically runs none
          // of the strands involved, and it must not block. Both calls below
          // therefore only *initiate* the teardown on the respective strand and
          // return immediately. The `shared_ptr` that is consigned to the first
          // one is required because the caller drops its own `shared_ptr` right
          // after this call; the `semaphore_` in contrast keeps its state alive
          // itself.
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
        "Aborting a `ParallelMergeState` failed.");
  }

 private:
  // The merging of a single chunk, as a handler-based loop instead of a
  // coroutine (which is not available in the C++17 backports mode): `step()`
  // merges a single output block and pushes it to the sink, and the completion
  // handler of that push calls `step()` again.
  //
  // The task keeps both the `ParallelMergeState` and its permit of the
  // `semaphore_` alive: it holds a `shared_ptr` to the former, and the latter
  // is returned as soon as the last `shared_ptr` to the task itself is gone, so
  // that the next chunk may take its place.
  //
  // IMPORTANT: Every step runs on `ParallelMergeState::executor_` and never on
  // one of the strands, because merging is ordinary blocking work that may even
  // do I/O, see the IMPORTANT note at the class comment above. This is achieved
  // by binding every completion handler of this task to `executor_`; the sink
  // then posts (and never dispatches) the handler there, see
  // `InOrderBlockSink::completeOn`.
  class ChunkTask : public std::enable_shared_from_this<ChunkTask> {
   private:
    std::shared_ptr<ParallelMergeState> state_;
    size_t chunkIndex_;
    ad_utility::AsyncSemaphore::Permit permit_;
    ChunkMerger<moveElements, Input, Comparator> merger_;

   public:
    // Construct from the `state` of the merge, the index of the chunk to merge,
    // and the `permit` that this task holds for its whole lifetime.
    ChunkTask(std::shared_ptr<ParallelMergeState> state, size_t chunkIndex,
              ad_utility::AsyncSemaphore::Permit permit)
        : state_{std::move(state)},
          chunkIndex_{chunkIndex},
          permit_{std::move(permit)},
          merger_{&state_->input_, &state_->comparator_, state_->options_,
                  state_->splitters_.getSplittersAt(chunkIndex),
                  state_->cancellationHandle_} {}

    // Merge the next output block of this chunk and push it to the sink, or
    // finish the chunk if it is exhausted or the merge was stopped. This never
    // throws; all exceptions are forwarded to the consumer via
    // `InOrderBlockSink::asyncPushException`.
    //
    // PRECONDITION: This runs on `ParallelMergeState::executor_` and not on one
    // of the strands, see the IMPORTANT note at the class comment above.
    void step() noexcept {
      std::optional<Block> block;
      try {
        if (state_->sink_.stopRequested()) {
          finish();
          return;
        }
        // NOTE: Merging a single output block is ordinary blocking work that
        // may even do I/O, so a chunk occupies its thread for the duration of
        // one output block.
        block = merger_.get();
      } catch (...) {
        fail(std::current_exception());
        return;
      }
      if (!block.has_value()) {
        finish();
        return;
      }
      executeAndHandleUnlikelyMemoryError([this, &block] {
        state_->sink_.asyncPush(
            chunkIndex_, std::move(block).value(),
            net::bind_executor(
                state_->executor_,
                [self = this->shared_from_this()](std::exception_ptr exception,
                                                  bool keepGoing) {
                  if (exception != nullptr) {
                    self->fail(std::move(exception));
                  } else if (keepGoing) {
                    self->step();
                  } else {
                    self->finish();
                  }
                }));
      });
    }

   private:
    // Run the `function`, whose only way of throwing is that an allocation (of
    // a completion handler) fails, in which case this chunk cannot continue at
    // all. Return `true` if the `function` ran through, and forward the
    // exception to the consumer otherwise.
    template <typename Function>
    bool executeAndHandleUnlikelyMemoryError(Function function) noexcept {
      try {
        function();
        return true;
      } catch (...) {
        fail(std::current_exception());
        return false;
      }
    }

    // Forward the `exception` of this chunk to the consumer and then finish the
    // chunk.
    void fail(std::exception_ptr exception) noexcept {
      bool wasForwarded = false;
      try {
        state_->sink_.asyncPushException(
            std::move(exception),
            net::bind_executor(state_->executor_,
                               [self = this->shared_from_this()](
                                   std::exception_ptr) { self->finish(); }));
        wasForwarded = true;
      } catch (...) {
        // `asyncPushException` allocates the handler that it posts onto the
        // strand of the sink, so the only way it can fail is that memory is
        // exhausted. Ignore that, because the chunk has to be finished in any
        // case, see below.
      }
      if (!wasForwarded) {
        finish();
      }
    }

    // Send the end-of-chunk sentinel, which is the last thing that this task
    // does. NOTE: This has to happen on every path, because the consumer would
    // wait for this chunk forever otherwise.
    void finish() noexcept {
      ad_utility::ignoreExceptionIfThrows(
          [this] {
            // NOTE: Nothing is left to do in the completion handler, but the
            // `self` keeps this task (and with it the permit) alive until the
            // sentinel was really sent.
            state_->sink_.asyncFinishChunk(
                chunkIndex_,
                net::bind_executor(state_->executor_,
                                   [self = this->shared_from_this()](
                                       std::exception_ptr, bool) {}));
          },
          "Finishing a chunk of a `ParallelMergeState` failed.");
    }
  };

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
    executeAndHandleUnlikelyMemoryError([this, chunkIndex] {
      semaphore_.asyncAcquire(net::bind_executor(
          strand_, [self = this->shared_from_this(), chunkIndex](
                       const boost::system::error_code& errorCode,
                       ad_utility::AsyncSemaphore::Permit permit) {
            self->spawnChunkAndContinue(errorCode, std::move(permit),
                                        chunkIndex);
          }));
    });
  }

  // The continuation of `dispatchNextChunk`: post the task that merges the
  // chunk with the given `chunkIndex` and immediately continue the loop with
  // the next chunk. Stop dispatching if the wait for the permit was cancelled
  // (`errorCode`) or the merge was stopped in the meantime.
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
    bool wasSpawned =
        executeAndHandleUnlikelyMemoryError([this, chunkIndex, &permit] {
          // NOTE: The task is deliberately posted onto `executor_` and never
          // onto a strand, see `ChunkTask`. The `permit` is owned by the task
          // and is hence returned to the `semaphore_` as soon as the chunk is
          // done, so that the next chunk may take its place.
          auto task = std::make_shared<ChunkTask>(
              this->shared_from_this(), chunkIndex, std::move(permit));
          net::post(executor_, [task = std::move(task)] { task->step(); });
        });
    if (!wasSpawned) {
      return;
    }
    dispatchNextChunk(chunkIndex + 1);
  }

  // Run the `function`, whose only way of throwing is that an allocation (of a
  // chunk task or of a completion handler) fails, in which case the merge
  // cannot continue at all. Return `true` if the `function` ran through, and
  // forward the exception to the consumer otherwise.
  template <typename Function>
  bool executeAndHandleUnlikelyMemoryError(Function function) noexcept {
    try {
      function();
      return true;
    } catch (...) {
      forwardExceptionToConsumer(std::current_exception());
      return false;
    }
  }

  // Forward an `exception` that was thrown while dispatching to the consumer,
  // which also stops the chunks that are already running.
  //
  // NOTE: The forwarding allocates a handler of its own, so the only way it can
  // fail is that memory is exhausted, in which case the consumer sees the end
  // of the range instead of the exception.
  void forwardExceptionToConsumer(std::exception_ptr exception) noexcept {
    ad_utility::ignoreExceptionIfThrows(
        [this, &exception] {
          sink_.asyncPushException(
              std::move(exception),
              net::consign(net::detached, this->shared_from_this()));
        },
        "Forwarding an exception of the chunk dispatcher to the consumer of "
        "a `ParallelMergeState` failed.");
  }
};

// A synchronous adapter for a `ParallelMergeState`: a lazy range of blocks
// whose `get()` blocks the calling thread until the next block is available.
// Use this to plug the merge into a consumer that is not itself asynchronous.
//
// IMPORTANT: The executor of the merge has to be run by *other* threads (for
// example by a `boost::asio::thread_pool`), because the thread that iterates
// over this range is blocked while it waits for the next block and can
// therefore not run any of the merge's tasks itself.
CPP_template(bool moveElements, typename Input, typename Comparator)(
    requires BlockedRunsInput<Input>) class ParallelMergeRange
    : public ad_utility::InputRangeFromGet<typename Input::Block>,
      public ad_utility::NoCopyNoMove {
 public:
  using Block = typename Input::Block;
  using State = ParallelMergeState<moveElements, Input, Comparator>;

 private:
  std::shared_ptr<State> state_;

 public:
  // Construct from the `state` of a merge that was already started, see
  // `ParallelMergeState::create`.
  explicit ParallelMergeRange(std::shared_ptr<State> state)
      : state_{std::move(state)} {
    AD_CONTRACT_CHECK(state_ != nullptr);
  }

  // Stop the merge, such that the tasks that are still in flight finish instead
  // of waiting for a consumer that is gone.
  ~ParallelMergeRange() override { state_->abort(); }

  // Return the next block of the merge, or `std::nullopt` at its end.
  //
  // IMPORTANT: This is *synchronous and blocking*: it waits on a `future` until
  // the next block is available and hence occupies its thread for that whole
  // time. It therefore deadlocks if it is called from one of the threads that
  // run the executor of the merge, because that thread is then no longer
  // available to run the tasks that produce the very block it waits for. In the
  // extreme case of a single-threaded executor the *first* call already
  // deadlocks. See also the IMPORTANT note in the class comment above.
  std::optional<Block> get() override {
    return state_->asyncNext(net::use_future).get();
  }
};

}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGESTATE_H
