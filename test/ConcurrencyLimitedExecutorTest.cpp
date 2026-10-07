// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/synchronization/notification.h>
#include <absl/time/time.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <vector>

#include "./util/GTestHelpers.h"
#include "util/ConcurrencyLimitedExecutor.h"
#include "util/jthread.h"

namespace {
namespace net = boost::asio;

using ad_utility::ConcurrencyLimitedExecutor;
using namespace std::chrono_literals;

// Generous, as it is only waited for in full if something is broken.
constexpr auto timeout = 10s;
// Always waited for in full to check that something does NOT happen.
constexpr auto shortTimeout = 50ms;

// Track the number of concurrently running tasks and its maximum.
struct ConcurrencyTracker {
  std::atomic<size_t> numActive_ = 0;
  std::atomic<size_t> maxActive_ = 0;
  std::atomic<size_t> numFinished_ = 0;

  void enter() {
    size_t active = ++numActive_;
    size_t max = maxActive_.load();
    while (active > max && !maxActive_.compare_exchange_weak(max, active)) {
    }
  }
  void leave() {
    --numActive_;
    ++numFinished_;
  }
};

}  // namespace

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, invalidArguments) {
  net::thread_pool pool{2};
  EXPECT_ANY_THROW(ConcurrencyLimitedExecutor(ql::any_io_executor{}, 2));
  EXPECT_ANY_THROW(ConcurrencyLimitedExecutor(pool.get_executor(), 0));
  EXPECT_NO_THROW(ConcurrencyLimitedExecutor(pool.get_executor(), 3));
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, limitIsRespected) {
  net::thread_pool pool{8};
  ConcurrencyLimitedExecutor ex{pool.get_executor(), 3};
  absl::Notification releaseTasks;
  ConcurrencyTracker tracker;
  constexpr size_t numTasks = 7;
  for (size_t i = 0; i < numTasks; ++i) {
    net::post(ex, [&]() {
      tracker.enter();
      // Bounded, so that a broken executor fails the test instead of hanging.
      releaseTasks.WaitForNotificationWithTimeout(absl::FromChrono(timeout));
      tracker.leave();
    });
  }
  ASSERT_TRUE(
      waitUntil([&]() { return tracker.numActive_.load() == 3u; }, timeout));
  std::this_thread::sleep_for(shortTimeout);
  EXPECT_EQ(tracker.numActive_.load(), 3u);
  releaseTasks.Notify();
  pool.join();
  EXPECT_EQ(tracker.numFinished_.load(), numTasks);
  EXPECT_EQ(tracker.maxActive_.load(), 3u);
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, concurrentSubmitters) {
  net::thread_pool pool{6};
  ConcurrencyLimitedExecutor ex{pool.get_executor(), 2};
  ConcurrencyTracker tracker;
  constexpr size_t numThreads = 4;
  constexpr size_t numTasksPerThread = 500;
  {
    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      // Every thread uses its own copy, as copies share the limit.
      threads.emplace_back([&tracker, ex]() {
        for (size_t j = 0; j < numTasksPerThread; ++j) {
          net::post(ex, [&tracker]() {
            tracker.enter();
            std::this_thread::yield();
            tracker.leave();
          });
        }
      });
    }
  }
  pool.join();
  EXPECT_EQ(tracker.numFinished_.load(), numThreads * numTasksPerThread);
  EXPECT_LE(tracker.maxActive_.load(), 2u);
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, limitOneRunsTasksInOrder) {
  net::thread_pool pool{4};
  ConcurrencyLimitedExecutor ex{pool.get_executor(), 1};
  // No synchronization, the executor has to establish the happens-before.
  std::vector<size_t> order;
  constexpr size_t numTasks = 300;
  for (size_t i = 0; i < numTasks; ++i) {
    net::post(ex, [&order, i]() { order.push_back(i); });
  }
  pool.join();
  std::vector<size_t> expected(numTasks);
  std::iota(expected.begin(), expected.end(), 0);
  EXPECT_EQ(order, expected);
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, neverRunsInline) {
  // A single thread, so that a submitted task can't run before the current one
  // completes.
  net::thread_pool pool{1};
  ConcurrencyLimitedExecutor ex{pool.get_executor(), 1};
  std::atomic<bool> dispatchedHasRun = false;
  net::post(ex, [&]() {
    // The only slot is occupied by this task, so this has to wait for it.
    net::dispatch(ex, [&]() { dispatchedHasRun = true; });
    EXPECT_FALSE(dispatchedHasRun.load());
  });
  pool.join();
  EXPECT_TRUE(dispatchedHasRun.load());
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, copiesAndComparison) {
  net::thread_pool pool{1};
  ConcurrencyLimitedExecutor ex{pool.get_executor(), 2};
  ConcurrencyLimitedExecutor copy = ex;
  ConcurrencyLimitedExecutor other{pool.get_executor(), 2};
  EXPECT_EQ(ex, copy);
  EXPECT_NE(ex, other);
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, throwingTaskReleasesItsSlot) {
  net::io_context ctx;
  ConcurrencyLimitedExecutor ex{ctx.get_executor(), 1};
  bool secondHasRun = false;
  net::post(ex, []() { throw std::runtime_error{"expected"}; });
  net::post(ex, [&secondHasRun]() { secondHasRun = true; });
  EXPECT_THROW(ctx.run(), std::runtime_error);
  EXPECT_FALSE(secondHasRun);
  ctx.run();
  EXPECT_TRUE(secondHasRun);
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, shutdownDestroysQueuedTasks) {
  std::weak_ptr<int> weakToken;
  {
    net::io_context ctx;
    ConcurrencyLimitedExecutor ex{ctx.get_executor(), 1};
    auto token = std::make_shared<int>(42);
    weakToken = token;
    // The tasks hold copies of the executor, which would form a reference
    // cycle with the queued task if it was not discarded on shutdown.
    for (size_t i = 0; i < 3; ++i) {
      net::post(ex, [ex, token]() { ADD_FAILURE() << "Must not be run"; });
    }
  }
  EXPECT_TRUE(weakToken.expired());
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, propertiesAreForwarded) {
  namespace execution = net::execution;
  net::thread_pool pool{1};
  ConcurrencyLimitedExecutor ex{pool.get_executor(), 2};
  EXPECT_EQ(&net::query(ex, execution::context), &pool);

  auto neverBlocking = net::require(ex, execution::blocking.never);
  static_assert(
      std::is_same_v<decltype(neverBlocking), ConcurrencyLimitedExecutor>);
  EXPECT_EQ(net::query(neverBlocking, execution::blocking),
            execution::blocking.never);
  EXPECT_NE(neverBlocking, ex);

  // A tracked executor keeps `pool.join()` from returning, so only check the
  // type.
  static_assert(std::is_same_v<decltype(net::prefer(
                                   ex, execution::outstanding_work.tracked)),
                               ConcurrencyLimitedExecutor>);
}

// _____________________________________________________________________________
TEST(ConcurrencyLimitedExecutor, makeConcurrencyLimitedExecutor) {
  net::thread_pool pool{4};
  ql::any_io_executor ex =
      ad_utility::makeConcurrencyLimitedExecutor(pool.get_executor(), 2);
  ConcurrencyTracker tracker;
  constexpr size_t numTasks = 100;
  for (size_t i = 0; i < numTasks; ++i) {
    net::post(ex, [&tracker]() {
      tracker.enter();
      std::this_thread::yield();
      tracker.leave();
    });
  }
  pool.join();
  EXPECT_EQ(tracker.numFinished_.load(), numTasks);
  EXPECT_LE(tracker.maxActive_.load(), 2u);
}
