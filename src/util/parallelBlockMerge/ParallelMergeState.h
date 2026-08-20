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
#include <vector>

#include "backports/concepts.h"
#include "util/CancellationHandle.h"
#include "util/Exception.h"
#include "util/ExceptionHandling.h"
#include "util/Iterators.h"
#include "util/parallelBlockMerge/ChunkMerger.h"
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
    : public ad_utility::InputRangeFromGet<typename Input::Block> {
 public:
  using Block = typename Input::Block;
  using Key = typename Input::Key;

 private:
  Input input_;
  Comparator comparator_;
  MergeOptions options_;
  SharedMergeScheduler scheduler_;
  ad_utility::SharedCancellationHandle cancellationHandle_;
  // The splitters, `splitters_.size() + 1` is the number of chunks.
  std::vector<Key> splitters_;
  size_t numChunks_;
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
  std::condition_variable allTasksFinished_;
  size_t nextChunkToDispatch_ = 0;
  size_t numChunksInFlight_ = 0;
  size_t numActiveTasks_ = 0;

 public:
  // ________________________________________________________________________
  ParallelMergeState(Input input, Comparator comparator, MergeOptions options,
                     SharedMergeScheduler scheduler,
                     ad_utility::SharedCancellationHandle cancellationHandle,
                     std::vector<Key> splitters, size_t maxInFlight)
      : input_{std::move(input)},
        comparator_{std::move(comparator)},
        options_{std::move(options)},
        scheduler_{std::move(scheduler)},
        cancellationHandle_{std::move(cancellationHandle)},
        splitters_{std::move(splitters)},
        numChunks_{splitters_.size() + 1},
        maxInFlight_{maxInFlight},
        sink_{options_.bufferedBlocksPerChunk},
        blocks_{sink_.blocks()} {
    // The dispatch policy is what makes the merge deadlock-free, see
    // `InOrderBlockSink`. In particular, more than one chunk has to be in
    // flight, because otherwise a single chunk could fill its buffer and then
    // block forever.
    AD_CORRECTNESS_CHECK(maxInFlight_ > 1);
    AD_CORRECTNESS_CHECK(maxInFlight_ <= scheduler_->maxParallelism());
    AD_CORRECTNESS_CHECK(maxInFlight_ <= numChunks_);
    sink_.setNumChunks(numChunks_);
  }

  // The tasks refer to this object, so it must neither be copied nor moved.
  ParallelMergeState(const ParallelMergeState&) = delete;
  ParallelMergeState& operator=(const ParallelMergeState&) = delete;
  ParallelMergeState(ParallelMergeState&&) = delete;
  ParallelMergeState& operator=(ParallelMergeState&&) = delete;

  // Abort the sink (which unblocks all producers) and then wait for all
  // in-flight tasks, before any of the members they refer to is destroyed.
  ~ParallelMergeState() override {
    stopRequested_.store(true);
    sink_.abort();
    std::unique_lock lock{mutex_};
    allTasksFinished_.wait(lock, [this] { return numActiveTasks_ == 0; });
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
      if (stopRequested_.load() || nextChunkToDispatch_ >= numChunks_ ||
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
        // destructor would wait for a task that does not exist. As always, the
        // notification has to happen while the lock is still held.
        std::unique_lock undoLock{mutex_};
        --numChunksInFlight_;
        --numActiveTasks_;
        allTasksFinished_.notify_all();
        undoLock.unlock();
        throw;
      }
    }
  }

  // Merge a single chunk and push its blocks to the sink. This never throws
  // (`TaskQueue::push` would call `std::terminate`), all exceptions are
  // forwarded to the consumer via `InOrderBlockSink::pushException`.
  void runChunk(size_t chunkIndex) noexcept {
    ad_utility::terminateIfThrows(
        [this, chunkIndex] {
          try {
            std::optional<Key> lo;
            std::optional<Key> hi;
            if (chunkIndex > 0) {
              lo = splitters_.at(chunkIndex - 1);
            }
            if (chunkIndex + 1 < numChunks_) {
              hi = splitters_.at(chunkIndex);
            }
            ChunkMerger<moveElements, Input, Comparator> merger{
                input_,        comparator_,   options_,
                std::move(lo), std::move(hi), cancellationHandle_};
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
          // NOTE: `finishChunk` has to be called on every path, because the
          // consumer would hang otherwise.
          sink_.finishChunk(chunkIndex);
          {
            std::unique_lock lock{mutex_};
            AD_CORRECTNESS_CHECK(numChunksInFlight_ > 0);
            --numChunksInFlight_;
          }
          // Top up the pipeline from the completing worker. This is required
          // for correctness and not only for throughput: a chunk that yields no
          // output block at all leaves the consumer parked inside
          // `InOrderBlockSink::popNextBlock`, where it cannot dispatch
          // anything, so the successors of that chunk would never be
          // dispatched and the merge would hang.
          //
          // NOTE: This relies on `MergeScheduler::schedule` not running the
          // task in the calling thread, because otherwise the dispatching would
          // recurse once per chunk. The constructor checks that the scheduler
          // offers a parallelism greater than one, which excludes the only
          // inline scheduler that exists (`InlineMergeScheduler`).
          try {
            dispatchChunks();
          } catch (...) {
            stopRequested_.store(true);
            sink_.pushException(std::current_exception());
          }
          std::unique_lock lock{mutex_};
          AD_CORRECTNESS_CHECK(numActiveTasks_ > 0);
          --numActiveTasks_;
          // NOTE: The notification has to happen while the lock is still
          // held. Otherwise the waiting destructor could return and destroy
          // `allTasksFinished_` while this thread is still inside
          // `notify_all`, which is a use-after-free.
          allTasksFinished_.notify_all();
        },
        "Merging a single chunk in `parallelBlockMergeToRange` failed.");
  }
};
}  // namespace detail
}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_PARALLELMERGESTATE_H
