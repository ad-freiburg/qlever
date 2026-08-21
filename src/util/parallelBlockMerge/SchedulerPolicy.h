// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SCHEDULERPOLICY_H
#define QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SCHEDULERPOLICY_H

#include <absl/functional/any_invocable.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "util/Exception.h"
#include "util/TaskQueue.h"

// The scheduler policy of the parallel block merge (see
// `util/parallelBlockMerge/ParallelBlockMerge.h`): the `MergeScheduler`
// interface via which the merge obtains its parallelism, together with the
// implementations that QLever uses.
namespace ad_utility::parallelBlockMerge {

// The runtime interface via which the parallel merge obtains its parallelism.
// It is deliberately a runtime (and not a compile-time) policy, so that the
// merge does not have to be templated on the way its tasks are executed and so
// that a scheduler can be shared between several concurrent merges.
class MergeScheduler {
 public:
  virtual ~MergeScheduler() = default;

  // Schedule the `task` for execution. The task may run in any thread
  // (including the calling one) and at any time after this call.
  //
  // IMPORTANT: The `task` must NEVER throw an exception. The implementations
  // below forward to `TaskQueue::push`, which calls `std::terminate` if the
  // task throws. It is the responsibility of the *core* of the merge to wrap
  // every task body in a `try`/`catch` and to forward any exception to the sink
  // via `pushException`.
  virtual void schedule(absl::AnyInvocable<void()> task) = 0;

  // Return the number of tasks that can run concurrently. The core uses this to
  // bound the number of chunks that are in flight at the same time, which is
  // essential for the deadlock-freedom of `InOrderBlockSink` (see there).
  virtual size_t maxParallelism() const = 0;
};

// The natural way of passing a scheduler around, because a single scheduler is
// typically shared by several merges.
using SharedMergeScheduler = std::shared_ptr<MergeScheduler>;

// A `MergeScheduler` that owns a `TaskQueue` with a dedicated thread pool.
class TaskQueueMergeScheduler : public MergeScheduler {
 private:
  // NOTE: The order of these members matters, `numThreads_` is used to
  // initialize `queue_`.
  size_t numThreads_;
  TaskQueue<false> queue_;

 public:
  // Construct from the number of threads and the name of the queue (the latter
  // is only used for logging). A value of `0` for `numThreads` means "as many
  // threads as the hardware offers".
  //
  // NOTE: The maximal size of the queue is `2 * numThreads`, such that
  // `schedule` never blocks as long as at most `numThreads` tasks are in
  // flight.
  explicit TaskQueueMergeScheduler(size_t numThreads = 0,
                                   std::string name = "parallelBlockMerge")
      : numThreads_{numThreads == 0
                        ? std::max<size_t>(1,
                                           std::thread::hardware_concurrency())
                        : numThreads},
        queue_{2 * numThreads_, numThreads_, std::move(name)} {}

  // See `MergeScheduler::schedule`. The `task` must never throw.
  void schedule(absl::AnyInvocable<void()> task) override {
    queue_.push(std::move(task));
  }

  // ________________________________________________________________________
  size_t maxParallelism() const override { return numThreads_; }
};

// A `MergeScheduler` that uses an externally owned `TaskQueue`. Use this to
// share a single thread pool between the merge and other tasks.
class BorrowedTaskQueueMergeScheduler : public MergeScheduler {
 private:
  TaskQueue<false>* queue_;
  size_t maxParallelism_;

 public:
  // Construct from the borrowed `queue` (which has to outlive this scheduler
  // and every merge that uses it) and the number of tasks that this scheduler
  // may keep in flight.
  //
  // NOTE: The maximal size of the borrowed queue has to be at least
  // `maxParallelism`, because otherwise `schedule` might block although the
  // scheduled tasks have not yet been started, which in turn could deadlock the
  // merge.
  BorrowedTaskQueueMergeScheduler(TaskQueue<false>& queue,
                                  size_t maxParallelism)
      : queue_{&queue}, maxParallelism_{maxParallelism} {
    AD_CONTRACT_CHECK(maxParallelism > 0);
    AD_CONTRACT_CHECK(queue.maxQueueSize() >= maxParallelism);
  }

  // See `MergeScheduler::schedule`. The `task` must never throw.
  void schedule(absl::AnyInvocable<void()> task) override {
    queue_->push(std::move(task));
  }

  // ________________________________________________________________________
  size_t maxParallelism() const override { return maxParallelism_; }
};

// A `MergeScheduler` that runs every task immediately in the calling thread.
// Use this for serial fast paths and for deterministic tests.
class InlineMergeScheduler : public MergeScheduler {
 public:
  // Run the `task` immediately. The `task` must never throw (see
  // `MergeScheduler::schedule`); note that in contrast to the queue-based
  // schedulers an exception would propagate to the caller here, but the core
  // must not rely on that difference.
  void schedule(absl::AnyInvocable<void()> task) override { task(); }

  // ________________________________________________________________________
  size_t maxParallelism() const override { return 1; }
};

// Return the process-wide default scheduler, which is a
// `TaskQueueMergeScheduler` with one thread per hardware thread. It is created
// lazily on the first call and then shared by all merges that do not specify a
// scheduler of their own.
inline SharedMergeScheduler defaultMergeScheduler() {
  static const SharedMergeScheduler scheduler =
      std::make_shared<TaskQueueMergeScheduler>();
  return scheduler;
}

}  // namespace ad_utility::parallelBlockMerge

#endif  // QLEVER_SRC_UTIL_PARALLELBLOCKMERGE_SCHEDULERPOLICY_H
