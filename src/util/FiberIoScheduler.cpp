// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "util/FiberIoScheduler.h"

#include <exception>

#include "absl/cleanup/cleanup.h"
#include "util/Exception.h"

#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
#include <boost/fiber/algo/round_robin.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>

#include "util/IoUringManager.h"
#endif

namespace ad_utility {

//______________________________________________________________________________
FiberIoScheduler& FiberIoScheduler::local() {
  thread_local FiberIoScheduler scheduler;
  return scheduler;
}

//______________________________________________________________________________
bool FiberIoScheduler::isInsideFiber() {
#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
  return boost::fibers::context::active() != nullptr;
#else
  return false;
#endif
}

#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
// Install the round-robin fiber scheduling algorithm on this thread once.
// Must run before the first fiber is created on the thread.
namespace {
void ensureFiberAlgorithm() {
  thread_local bool installed = []() {
    boost::fibers::use_scheduling_algorithm<boost::fibers::algo::round_robin>();
    return true;
  }();
  (void)installed;
}
}  // namespace
#endif

//______________________________________________________________________________
void FiberIoScheduler::runAsFibers(std::vector<std::function<void()>> bodies) {
#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
  if (bodies.empty()) {
    return;
  }
  ensureFiberAlgorithm();
  FiberIoScheduler& scheduler = FiberIoScheduler::local();
  std::vector<boost::fibers::fiber> fibers;
  fibers.reserve(bodies.size());
  for (auto& body : bodies) {
    fibers.emplace_back([&scheduler, &body]() {
      scheduler.enterActive();
      absl::Cleanup leaveActive{[&scheduler]() { scheduler.exitActive(); }};
      body();
    });
  }
  // Join every fiber even when one throws: a joinable fiber's destructor
  // would call `std::terminate`. Rethrow the first exception afterwards.
  std::exception_ptr firstException;
  for (auto& fiber : fibers) {
    try {
      fiber.join();
    } catch (...) {
      if (!firstException) {
        firstException = std::current_exception();
      }
    }
  }
  if (firstException) {
    std::rethrow_exception(firstException);
  }
#else
  // Without fiber support the bodies run sequentially in order, so callers
  // keep a single code path on every build configuration.
  for (auto& body : bodies) {
    body();
  }
#endif
}

#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
// Wait until `isDone()` holds: reap every available completion first, then
// yield once so siblings run, and only then park blocking. The park
// (`drainOneCqe`) reaps and attributes the next completion of any batch, so
// a parking fiber still serves its siblings and every park terminates: all
// awaited reads were already submitted, and the kernel posts their
// completions independently of fiber activity.
void FiberIoScheduler::waitUntil(IoUringPolicy& policy,
                                 const std::function<bool()>& isDone) {
  enterWait();
  absl::Cleanup leaveWait{[this]() { exitWait(); }};
  bool yieldedSinceProgress = false;
  while (!isDone()) {
    if (policy.reapAvailableCompletions() > 0) {
      yieldedSinceProgress = false;
      continue;
    }
    if (siblingsMayHaveWork() || !yieldedSinceProgress) {
      yieldedSinceProgress = true;
      boost::this_fiber::yield();
    } else {
      policy.drainOneCqe();
      yieldedSinceProgress = false;
    }
  }
}
#endif

//______________________________________________________________________________
void FiberIoScheduler::waitForBatch(IoUringPolicy& policy, BatchHandle handle) {
#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
  waitUntil(policy,
            [&policy, handle]() { return policy.isBatchComplete(handle); });
#else
  (void)policy;
  (void)handle;
  AD_THROW(
      "FiberIoScheduler::waitForBatch requires an io_uring build with fiber "
      "support (QLEVER_HAS_IO_URING and QLEVER_HAS_FIBER_IO)");
#endif
}

//______________________________________________________________________________
void FiberIoScheduler::waitForFreeSlot(IoUringPolicy& policy) {
#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
  waitUntil(policy, [&policy]() { return !policy.isRingFull(); });
#else
  (void)policy;
  AD_THROW(
      "FiberIoScheduler::waitForFreeSlot requires an io_uring build with "
      "fiber support (QLEVER_HAS_IO_URING and QLEVER_HAS_FIBER_IO)");
#endif
}

}  // namespace ad_utility
