// Copyright 2026, The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <functional>
#include <stdexcept>
#include <vector>

#include "util/FiberIoScheduler.h"

namespace {

// Whether this build runs bodies as real fibers. The cooperative waits need
// a live io_uring ring and are covered through `IoUringPolicy::wait` in
// `IoUringManagerTest`; here only the ring-free scheduling interface is
// tested, which behaves identically in every build configuration except for
// the documented graceful degradation (`isInsideFiber` is always false and
// bodies run sequentially without fiber support).
#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
constexpr bool kHasFiberSupport = true;
#else
constexpr bool kHasFiberSupport = false;
#endif

// The scheduler instance is thread-local and stable: repeated calls on the
// same thread return the same object, and a fresh thread starts with zero
// waiting and active fibers.
TEST(FiberIoScheduler, localIsStableThreadLocalInstance) {
  auto& first = ad_utility::FiberIoScheduler::local();
  auto& second = ad_utility::FiberIoScheduler::local();
  EXPECT_EQ(&first, &second);
  EXPECT_EQ(first.numWaitingFibers(), size_t{0});
  EXPECT_EQ(first.numActiveFibers(), size_t{0});
}

// Outside `runAsFibers` the calling thread never runs inside a fiber body.
TEST(FiberIoScheduler, plainThreadIsNotInsideFiber) {
  EXPECT_FALSE(ad_utility::FiberIoScheduler::isInsideFiber());
}

// Empty input runs nothing and returns: fiber builds take the early return,
// other builds simply skip the sequential loop.
TEST(FiberIoScheduler, emptyBodiesDoNothing) {
  ad_utility::FiberIoScheduler::runAsFibers({});
  EXPECT_FALSE(ad_utility::FiberIoScheduler::isInsideFiber());
  EXPECT_EQ(ad_utility::FiberIoScheduler::local().numActiveFibers(), size_t{0});
}

// Every body runs exactly once, and once `runAsFibers` returns no body is
// still active or waiting.
TEST(FiberIoScheduler, allBodiesRunExactlyOnce) {
  constexpr size_t kNumBodies = 5;
  std::vector<int> runCount(kNumBodies, 0);
  std::vector<std::function<void()>> bodies;
  bodies.reserve(kNumBodies);
  for (size_t i = 0; i < kNumBodies; ++i) {
    bodies.emplace_back([&, i]() { ++runCount[i]; });
  }
  ad_utility::FiberIoScheduler::runAsFibers(std::move(bodies));
  for (const int count : runCount) {
    EXPECT_EQ(count, 1);
  }
  EXPECT_EQ(ad_utility::FiberIoScheduler::local().numActiveFibers(), size_t{0});
  EXPECT_EQ(ad_utility::FiberIoScheduler::local().numWaitingFibers(),
            size_t{0});
}

// Inside a body the fiber flag reports the build mode: true on fiber builds
// (the body runs as a real fiber), false otherwise (bodies run sequentially
// on the plain calling thread).
TEST(FiberIoScheduler, insideBodyFlagMatchesBuildMode) {
  ad_utility::FiberIoScheduler::runAsFibers({[]() {
    EXPECT_EQ(ad_utility::FiberIoScheduler::isInsideFiber(), kHasFiberSupport);
  }});
  EXPECT_FALSE(ad_utility::FiberIoScheduler::isInsideFiber());
}

// A throwing body does not strand its siblings: every other body still runs
// to completion, and the exception reaches the caller.
TEST(FiberIoScheduler, throwingBodyDoesNotStrandSiblings) {
  bool siblingRan = false;
  EXPECT_THROW(
      ad_utility::FiberIoScheduler::runAsFibers(
          {[&]() { siblingRan = true; },
           []() -> void { throw std::runtime_error("fiber body failure"); }}),
      std::runtime_error);
  EXPECT_TRUE(siblingRan);
}

// With several throwing bodies the first body's exception is the one that
// propagates: sequentially the loop aborts at the first throw, as fibers the
// first captured error is rethrown after all bodies were joined.
TEST(FiberIoScheduler, firstExceptionWins) {
  EXPECT_THROW(ad_utility::FiberIoScheduler::runAsFibers(
                   {[]() -> void { throw std::runtime_error("first"); },
                    []() -> void { throw std::logic_error("second"); }}),
               std::runtime_error);
}

// A nested `runAsFibers` call runs its bodies and restores the outer fiber
// flag instead of clearing it.
TEST(FiberIoScheduler, nestedRunAsFibersRestoresOuterFlag) {
  bool innerRan = false;
  ad_utility::FiberIoScheduler::runAsFibers({[&]() {
    EXPECT_EQ(ad_utility::FiberIoScheduler::isInsideFiber(), kHasFiberSupport);
    ad_utility::FiberIoScheduler::runAsFibers({[&]() {
      EXPECT_EQ(ad_utility::FiberIoScheduler::isInsideFiber(),
                kHasFiberSupport);
      innerRan = true;
    }});
    // The inner call saved and restored the flag: the outer body still
    // observes its own mode.
    EXPECT_EQ(ad_utility::FiberIoScheduler::isInsideFiber(), kHasFiberSupport);
  }});
  EXPECT_TRUE(innerRan);
  EXPECT_FALSE(ad_utility::FiberIoScheduler::isInsideFiber());
}

#if defined(QLEVER_HAS_IO_URING) && defined(QLEVER_HAS_FIBER_IO)
// A lone body counts as exactly one active fiber while it runs.
TEST(FiberIoScheduler, singleBodyCountsOneActiveFiber) {
  ad_utility::FiberIoScheduler::runAsFibers({[]() {
    EXPECT_EQ(ad_utility::FiberIoScheduler::local().numActiveFibers(),
              size_t{1});
    EXPECT_EQ(ad_utility::FiberIoScheduler::local().numWaitingFibers(),
              size_t{0});
  }});
}
#endif

}  // namespace
