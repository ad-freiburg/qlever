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
#include <mutex>
#include <optional>
#include <utility>

#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/NoCopyNoMove.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
#include "util/parallelBlockMerge/MergeHelpers.h"
#include "util/parallelBlockMerge/MergeOptions.h"
#include "util/parallelBlockMerge/OutputSinkPolicy.h"
#include "util/parallelBlockMerge/RunsInputPolicy.h"
#include "util/parallelBlockMerge/SchedulerPolicy.h"

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
          input_, comparator_, options_, splitters_.getSplittersAt(chunkIndex),
          cancellationHandle_};
      while (!stopRequested_.load()) {
        auto block = merger.nextBlock();
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
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGESTATE_H
