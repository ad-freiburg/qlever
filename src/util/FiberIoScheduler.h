// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_FIBERIOSCHEDULER_H
#define QLEVER_SRC_UTIL_FIBERIOSCHEDULER_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace ad_utility {

// Forward declaration; defined in `util/IoUringManager.h` on io_uring builds.
class IoUringPolicy;

// Cooperative fiber scheduler for io_uring batch waits. Design-only
// reference: `docs/io_uring/fibers-design.md` (PR #165, steps 1-4).
//
// One thread runs several lookup batches as stackful fibers (Boost.Fiber over
// Boost.Context). While a fiber's batch is still in flight, the fiber reaps
// available completions and yields to sibling fibers instead of parking the
// OS thread in `io_uring_wait_cqe`. Parking remains only as the last resort
// when no sibling can make progress.
//
// The scheduler is thread-local and single-threaded by construction:
// fibers on one thread cooperate without locks, and each worker thread owns
// its `IoUringPolicy` (see the per-thread manager pool in
// `VocabularyOnDisk::open`). Sharing one scheduler or policy across OS
// threads is not supported.
//
// Every blocking point in this scheduler waits only on already-submitted
// reads, whose completions the kernel posts independently of what any fiber
// does. A parked fiber therefore always makes progress (it reaps and
// attributes completions of any batch while parked), so parking cannot
// deadlock sibling fibers.
//
// When fiber support is not compiled in (`QLEVER_HAS_FIBER_IO` undefined),
// every method degrades gracefully: `runAsFibers` runs the bodies
// sequentially, `isInsideFiber` is always false, and the waits fall back to
// the blocking `IoUringPolicy` primitives.
class FiberIoScheduler {
 public:
  using BatchHandle = uint64_t;

  FiberIoScheduler(const FiberIoScheduler&) = delete;
  FiberIoScheduler& operator=(const FiberIoScheduler&) = delete;

  // The calling thread's scheduler instance.
  static FiberIoScheduler& local();

  // True when called from inside a body running under `runAsFibers` on a
  // fiber-enabled build. Tracked explicitly (rather than asking
  // `boost::fibers::context::active()`, whose nullability on plain threads
  // varies across Boost versions), so plain threads — including the main
  // thread after fibers ran on it — always report false.
  static bool isInsideFiber();

  // Run each body as its own fiber on this thread and return once all have
  // finished. Exceptions thrown by a body propagate to the caller after all
  // fibers have been joined; the first exception is rethrown. Without fiber
  // support the bodies run sequentially in order.
  static void runAsFibers(std::vector<std::function<void()>> bodies);

  // Cooperatively wait until every read of `handle` has completed. To be
  // called from inside a fiber body (this is what `IoUringPolicy::wait`
  // delegates to when it detects an active fiber). Returns only after at
  // least one of this batch's own completions was reaped, so no wakeup can
  // be lost between the last reap and the yield: every resume re-reaps
  // before checking completion.
  void waitForBatch(IoUringPolicy& policy, BatchHandle handle);

  // Cooperatively wait until the ring has a free submission slot. To be
  // called from inside a fiber body when `addBatch` hits a full ring (this
  // is what the ring-full path delegates to when it detects an active
  // fiber).
  void waitForFreeSlot(IoUringPolicy& policy);

  // Number of fibers currently inside `waitForBatch`/`waitForFreeSlot` on
  // this thread. Exposed for the adaptive-batching ratio controller
  // (design step 3): the submission path adapts batch size to the ratio of
  // outstanding I/Os (`IoUringPolicy::numOutstandingReads`) to waiting
  // fibers without new bookkeeping.
  size_t numWaitingFibers() const { return numWaitingFibers_; }

  // Number of fiber bodies currently executing on this thread.
  size_t numActiveFibers() const { return numActiveFibers_; }

 private:
  FiberIoScheduler() = default;

  size_t numWaitingFibers_ = 0;
  size_t numActiveFibers_ = 0;

  // A sibling fiber is doing non-wait work (submitting, formatting) when
  // more fibers are active than waiting; yielding then lets it run.
  bool siblingsMayHaveWork() const {
    return numActiveFibers_ > numWaitingFibers_;
  }

  void enterWait() { ++numWaitingFibers_; }
  void exitWait() { --numWaitingFibers_; }
  void enterActive() { ++numActiveFibers_; }
  void exitActive() { --numActiveFibers_; }

  // Shared cooperative loop behind `waitForBatch` and `waitForFreeSlot`.
  // Only declared meaningfully on fiber-enabled io_uring builds; defined in
  // the implementation file.
  void waitUntil(IoUringPolicy& policy, const std::function<bool()>& isDone);
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_FIBERIOSCHEDULER_H
