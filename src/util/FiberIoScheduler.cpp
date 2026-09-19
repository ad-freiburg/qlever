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
#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>

#include "util/IoUringManager.h"
#endif

namespace ad_utility {

// Whether this thread is currently executing a `runAsFibers` body. Set by
// the fiber wrapper below (save/restore, so nested `runAsFibers` calls are
// safe); never set on plain threads.
thread_local bool t_inSchedulerFiberBody = false;

//______________________________________________________________________________
FiberIoScheduler& FiberIoScheduler::local() {
  thread_local FiberIoScheduler scheduler;
  return scheduler;
}

//______________________________________________________________________________
bool FiberIoScheduler::isInsideFiber() {
#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
  return t_inSchedulerFiberBody;
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
  // One error slot per body. Exceptions are captured inside the wrapper:
  // letting them escape the fiber function risks `std::terminate`
  // (depending on launch and transport semantics), while captured errors let
  // every sibling run to completion before the first one is rethrown.
  std::vector<std::exception_ptr> errors(bodies.size());
  std::vector<boost::fibers::fiber> fibers;
  fibers.reserve(bodies.size());
  try {
    for (size_t i = 0; i < bodies.size(); ++i) {
      // `launch::post`: schedule the body instead of running it in the
      // constructor. The default (`launch::dispatch`) runs the body
      // immediately, so a throwing body would unwind through vector
      // construction and terminate on the still-joinable earlier fibers.
      fibers.emplace_back(boost::fibers::launch::post, [&scheduler, &bodies,
                                                        &errors, i]() {
        scheduler.enterActive();
        absl::Cleanup leaveActive([&scheduler]() { scheduler.exitActive(); });
        const bool wasInBody = t_inSchedulerFiberBody;
        t_inSchedulerFiberBody = true;
        absl::Cleanup restoreFlag(
            [wasInBody]() { t_inSchedulerFiberBody = wasInBody; });
        try {
          bodies[i]();
        } catch (...) {
          errors[i] = std::current_exception();
        }
      });
    }
  } catch (...) {
    // Construction failed partway (e.g. `bad_alloc` from `emplace_back`
    // despite the reserve): join what exists, since destroying a joinable
    // fiber calls `std::terminate`, then rethrow the original error.
    for (auto& fiber : fibers) {
      try {
        fiber.join();
      } catch (...) {
      }
    }
    throw;
  }
  // Join every fiber: a joinable fiber's destructor would call
  // `std::terminate`. Bodies cannot throw past the wrapper anymore, so a
  // throwing join here is a scheduler error and propagates.
  for (auto& fiber : fibers) {
    fiber.join();
  }
  for (const auto& error : errors) {
    if (error) {
      std::rethrow_exception(error);
    }
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
// completions independently of fiber activity. This rests on the same
// assumption as the pre-existing blocking `wait` — completions for
// submitted reads always arrive — so a lost completion would hang exactly
// as `io_uring_wait_cqe` would; no timeout is added because silently
// abandoning in-flight reads would be worse than hanging loudly.
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
