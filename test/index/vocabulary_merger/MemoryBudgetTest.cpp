// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <utility>

#include "index/vocabulary_merger/MemoryBudget.h"

using ad_utility::MemorySize;
using ad_utility::vocabulary_merger::detail::MemoryBudget;

static MemorySize bytes(size_t n) { return MemorySize::bytes(n); }

// Test that reservations account for their memory until they are destroyed or
// released, that they can grow and be moved, and that the peak is tracked.
TEST(MemoryBudget, reserveGrowAndRelease) {
  MemoryBudget budget{bytes(100)};
  EXPECT_EQ(budget.limit(), bytes(100));
  {
    auto a = budget.reserve(bytes(30));
    EXPECT_EQ(a.numBytes(), 30u);
    EXPECT_EQ(budget.used(), bytes(30));
    // A reservation may exceed the limit.
    auto b = budget.reserve(bytes(80));
    EXPECT_EQ(budget.used(), bytes(110));
    b.grow(5);
    EXPECT_EQ(b.numBytes(), 85u);
    EXPECT_EQ(budget.used(), bytes(115));
    // Moving transfers the bytes, the moved-from reservation is empty.
    MemoryBudget::Reservation c = std::move(b);
    EXPECT_EQ(c.numBytes(), 85u);
    EXPECT_EQ(budget.used(), bytes(115));
    // The moved-from reservation holds nothing and cannot grow.
    // NOLINTNEXTLINE(bugprone-use-after-move)
    EXPECT_EQ(b.numBytes(), 0u);
    EXPECT_ANY_THROW(b.grow(1));
    c.release();
    EXPECT_EQ(c.numBytes(), 0u);
    EXPECT_EQ(budget.used(), bytes(30));
    // A move assignment releases the previous bytes of the target.
    a = budget.reserve(bytes(10));
    EXPECT_EQ(budget.used(), bytes(10));
  }
  EXPECT_EQ(budget.used(), bytes(0));
  EXPECT_EQ(budget.peak(), bytes(115));
}

// Test that `waitUntilFits` returns immediately if the memory fits (always if
// nothing is reserved), and returns `false` as soon as waiting is pointless.
TEST(MemoryBudget, waitUntilFitsWithoutWaiting) {
  MemoryBudget budget{bytes(100)};
  auto neverStop = []() { return false; };
  // Everything fits into an empty budget, even more than the limit.
  EXPECT_TRUE(budget.waitUntilFits(bytes(1000), neverStop));
  auto reservation = budget.reserve(bytes(60));
  EXPECT_TRUE(budget.waitUntilFits(bytes(40), neverStop));
  size_t numCalls = 0;
  EXPECT_FALSE(budget.waitUntilFits(bytes(41), [&numCalls]() {
    ++numCalls;
    return true;
  }));
  EXPECT_EQ(numCalls, 1u);
}

// Test that `waitUntilFits` waits until another thread releases memory.
TEST(MemoryBudget, waitUntilFitsWaitsForRelease) {
  MemoryBudget budget{bytes(100)};
  auto reservation = budget.reserve(bytes(80));
  std::atomic<bool> released = false;
  std::thread releaser{[&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    released = true;
    reservation.release();
  }};
  EXPECT_TRUE(budget.waitUntilFits(bytes(50), []() { return false; }));
  EXPECT_TRUE(released);
  releaser.join();
}

// Test that the `stopWaiting` predicate is polled while waiting, also without
// any release.
TEST(MemoryBudget, waitUntilFitsPollsThePredicate) {
  MemoryBudget budget{bytes(100)};
  auto reservation = budget.reserve(bytes(80));
  std::atomic<bool> stop = false;
  std::thread stopper{[&stop]() {
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
    stop = true;
  }};
  EXPECT_FALSE(budget.waitUntilFits(
      bytes(50), [&stop]() { return stop.load(); },
      std::chrono::milliseconds{1}));
  stopper.join();
}
