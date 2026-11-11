// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <atomic>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/use_future.hpp>
#include <chrono>
#include <future>
#include <thread>

#include "util/FairShareExecutor.h"

using namespace ad_utility;
using namespace std::chrono_literals;

namespace {

// Busy-wait for `duration`, so that the task really occupies its thread.
void spin(std::chrono::microseconds duration) {
  auto end = std::chrono::steady_clock::now() + duration;
  while (std::chrono::steady_clock::now() < end) {
  }
}

// Track the maximal number of tasks that run at the same time.
struct ConcurrencyTracker {
  std::atomic<size_t> current_ = 0;
  std::atomic<size_t> max_ = 0;
  void enter() {
    size_t now = ++current_;
    size_t prev = max_.load();
    while (now > prev && !max_.compare_exchange_weak(prev, now)) {
    }
  }
  void leave() { --current_; }
};

// Post `numTasks` tasks to `executor`, wait for all of them, and return the
// maximal number of them that ran at the same time. Each task waits (for at
// most 10 seconds) until `expectedConcurrency` tasks have run at the same time,
// so that the result doesn't depend on the timing, and then keeps its thread
// busy for a short while, so that exceeding the limit is detected.
size_t runAndWait(const qlever::Executor& executor, size_t numTasks,
                  size_t expectedConcurrency) {
  ConcurrencyTracker tracker;
  auto deadline = std::chrono::steady_clock::now() + 10s;
  std::vector<std::future<void>> futures;
  for (size_t i = 0; i < numTasks; ++i) {
    futures.push_back(boost::asio::post(
        executor,
        boost::asio::use_future([&tracker, expectedConcurrency, deadline]() {
          tracker.enter();
          while (tracker.max_ < expectedConcurrency &&
                 std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
          }
          spin(1000us);
          tracker.leave();
        })));
  }
  for (auto& future : futures) {
    future.get();
  }
  return tracker.max_;
}

// A task that keeps a class of a `FairScheduler` saturated: it counts itself
// and posts a copy of itself again until `stop_` is set.
struct Feeder {
  qlever::Executor executor_;
  std::atomic<size_t>* counter_;
  const std::atomic<bool>* stop_;
  void operator()() const {
    if (*stop_) {
      return;
    }
    spin(500us);
    ++*counter_;
    boost::asio::post(executor_, *this);
  }
};
}  // namespace

// A single client without a limit gets all the slots.
TEST(FairShareExecutor, singleClientIsWorkConserving) {
  boost::asio::thread_pool pool{8};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 4);
  qlever::Executor executor = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Foreground)};
  EXPECT_EQ(runAndWait(executor, 64, 4), 4u);
  pool.join();
}

// The limit of a client is respected.
TEST(FairShareExecutor, maxConcurrencyOfClient) {
  boost::asio::thread_pool pool{8};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 8);
  qlever::Executor root = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Background)};
  auto limited =
      makeFairShareClientExecutor(root, FairShareClass::Foreground, 2);
  EXPECT_EQ(runAndWait(limited, 32, 2), 2u);
  pool.join();
}

// Under contention, the classes get the threads according to their shares,
// no matter how many tasks each of them has queued.
#ifndef _QLEVER_NO_TIMING_TESTS
TEST(FairShareExecutor, sharesOfClasses) {
  boost::asio::thread_pool pool{4};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 4);
  scheduler->setShare(FairShareClass::Background, 0.25);
  scheduler->setShare(FairShareClass::Foreground, 0.75);
  qlever::Executor background = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Background)};
  // Many query clients, each of which floods the scheduler with tasks.
  std::vector<qlever::Executor> queries;
  for (size_t i = 0; i < 8; ++i) {
    queries.push_back(
        makeFairShareClientExecutor(background, FairShareClass::Foreground, 4));
  }
  std::atomic<size_t> backgroundDone = 0;
  std::atomic<size_t> queryDone = 0;
  std::atomic<bool> stop = false;
  // Keep both classes saturated for a while.
  auto feed = [&stop](const qlever::Executor& executor,
                      std::atomic<size_t>& counter) {
    for (size_t i = 0; i < 8; ++i) {
      boost::asio::post(executor, Feeder{executor, &counter, &stop});
    }
  };
  feed(background, backgroundDone);
  for (auto& q : queries) {
    feed(q, queryDone);
  }
  std::this_thread::sleep_for(1s);
  stop = true;
  pool.join();
  double fraction = static_cast<double>(backgroundDone) /
                    static_cast<double>(backgroundDone + queryDone);
  EXPECT_NEAR(fraction, 0.25, 0.07) << backgroundDone << " " << queryDone;
}
#endif

// If only the background class has work, it gets all the threads.
TEST(FairShareExecutor, idleClassLeavesItsShareToOthers) {
  boost::asio::thread_pool pool{4};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 4);
  scheduler->setShare(FairShareClass::Background, 0.1);
  scheduler->setShare(FairShareClass::Foreground, 0.9);
  qlever::Executor background = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Background)};
  EXPECT_EQ(runAndWait(background, 64, 4), 4u);
  pool.join();
}

// `makeFairShareExecutor` wraps a plain executor into a new scheduler, which
// runs at most `numSlots` tasks at the same time, and from which clients of
// other classes can be derived.
TEST(FairShareExecutor, makeFairShareExecutor) {
  boost::asio::thread_pool pool{8};
  auto executor = makeFairShareExecutor(pool.get_executor(), 3);
  EXPECT_NE(executor.target<FairShareExecutor>(), nullptr);
  EXPECT_EQ(runAndWait(executor, 32, 3), 3u);

  auto query = makeFairShareClientExecutor(executor, FairShareClass::Foreground,
                                           std::nullopt);
  ASSERT_NE(query.target<FairShareExecutor>(), nullptr);
  EXPECT_EQ(query.target<FairShareExecutor>()->scheduler(),
            executor.target<FairShareExecutor>()->scheduler());
  // Executors of different clients of the same scheduler are not equal.
  EXPECT_FALSE(*query.target<FairShareExecutor>() ==
               *executor.target<FairShareExecutor>());

  // Two calls create independent schedulers.
  auto other = makeFairShareExecutor(pool.get_executor(), 3);
  EXPECT_NE(other.target<FairShareExecutor>()->scheduler(),
            executor.target<FairShareExecutor>()->scheduler());

  // Copies are equal, executors of different schedulers are not.
  FairShareExecutor copy = *executor.target<FairShareExecutor>();
  EXPECT_TRUE(copy == *executor.target<FairShareExecutor>());
  EXPECT_FALSE(*other.target<FairShareExecutor>() ==
               *executor.target<FairShareExecutor>());

  EXPECT_ANY_THROW(makeFairShareExecutor(pool.get_executor(), 0));
  EXPECT_ANY_THROW(makeFairShareExecutor(qlever::Executor{}, 2));
  pool.join();
}

// Executors that are not `FairShareExecutor`s fall back to a concurrency limit.
TEST(FairShareExecutor, fallbackForOtherExecutors) {
  boost::asio::thread_pool pool{8};
  auto limited = makeFairShareClientExecutor(pool.get_executor(),
                                             FairShareClass::Foreground, 3);
  EXPECT_EQ(runAndWait(limited, 32, 3), 3u);
  auto unlimited = makeFairShareClientExecutor(
      pool.get_executor(), FairShareClass::Foreground, std::nullopt);
  EXPECT_TRUE(unlimited == qlever::Executor{pool.get_executor()});
  pool.join();
}

// A client that is at its limit (with queued tasks) and a class all of whose
// clients are at their limits are skipped in favor of a client that may run.
TEST(FairShareExecutor, clientsAtTheirLimitAreSkipped) {
  boost::asio::thread_pool pool{3};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 3);
  auto makeExecutor = [&scheduler](FairShareClass fairShareClass,
                                   size_t maxConcurrency) {
    return qlever::Executor{FairShareExecutor{
        scheduler, scheduler->makeClient(fairShareClass, maxConcurrency)}};
  };
  auto limitedBackground = makeExecutor(FairShareClass::Background, 1);
  auto limitedForeground = makeExecutor(FairShareClass::Foreground, 1);
  auto otherBackground = makeExecutor(FairShareClass::Background, 1);

  // Start a task of each limited client, which blocks until it is released.
  std::promise<void> release;
  std::shared_future<void> released = release.get_future().share();
  auto startBlockingTask = [&released](const qlever::Executor& executor) {
    std::promise<void> started;
    auto hasStarted = started.get_future();
    auto done = boost::asio::post(
        executor, boost::asio::use_future(
                      [&released, started = std::move(started)]() mutable {
                        started.set_value();
                        released.wait();
                      }));
    hasStarted.wait();
    return done;
  };
  auto blockedBackground = startBlockingTask(limitedBackground);
  auto blockedForeground = startBlockingTask(limitedForeground);

  // The second tasks of the limited clients have to wait, but the task of the
  // other client runs (on the free third slot).
  auto noop = []() {};
  auto queuedBackground =
      boost::asio::post(limitedBackground, boost::asio::use_future(noop));
  auto queuedForeground =
      boost::asio::post(limitedForeground, boost::asio::use_future(noop));
  boost::asio::post(otherBackground, boost::asio::use_future(noop)).get();
  EXPECT_EQ(queuedBackground.wait_for(0s), std::future_status::timeout);
  EXPECT_EQ(queuedForeground.wait_for(0s), std::future_status::timeout);

  // After the release, all the remaining tasks run.
  release.set_value();
  for (auto* future : {&blockedBackground, &blockedForeground,
                       &queuedBackground, &queuedForeground}) {
    future->get();
  }
  pool.join();
}

// A task that throws gives the slot of its worker back, so that the next task
// runs, and the exception propagates out of the underlying executor.
TEST(FairShareExecutor, throwingTaskReleasesItsSlot) {
  boost::asio::io_context ctx;
  auto executor = makeFairShareExecutor(ctx.get_executor(), 1);
  bool secondHasRun = false;
  boost::asio::post(executor, []() { throw std::runtime_error{"expected"}; });
  boost::asio::post(executor, [&secondHasRun]() { secondHasRun = true; });
  EXPECT_THROW(ctx.run(), std::runtime_error);
  EXPECT_FALSE(secondHasRun);
  ctx.run();
  EXPECT_TRUE(secondHasRun);
}

// `setFairShare` sets the share on the scheduler of a `FairShareExecutor`, and
// does nothing for other executors.
TEST(FairShareExecutor, setFairShare) {
  boost::asio::thread_pool pool{1};
  auto fair = makeFairShareExecutor(pool.get_executor(), 1);
  EXPECT_NO_THROW(setFairShare(fair, FairShareClass::Foreground, 0.5));
  // The share is checked by the scheduler, so the call reached it.
  EXPECT_ANY_THROW(setFairShare(fair, FairShareClass::Foreground, 0.0));
  EXPECT_NO_THROW(setFairShare(qlever::Executor{pool.get_executor()},
                               FairShareClass::Foreground, 0.0));
  pool.join();
}

// Invalid arguments are rejected.
TEST(FairShareExecutor, invalidArguments) {
  boost::asio::thread_pool pool{1};
  EXPECT_ANY_THROW(FairScheduler(ql::any_io_executor{}, 1));
  EXPECT_ANY_THROW(FairScheduler(pool.get_executor(), 0));
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 1);
  EXPECT_ANY_THROW(scheduler->makeClient(FairShareClass::Foreground, 0));
  EXPECT_ANY_THROW(scheduler->setShare(FairShareClass::Foreground, 0.0));
  auto client = scheduler->makeClient(FairShareClass::Foreground);
  EXPECT_ANY_THROW(FairShareExecutor(nullptr, client));
  EXPECT_ANY_THROW(FairShareExecutor(scheduler, nullptr));
  EXPECT_ANY_THROW(makeFairShareClientExecutor(
      FairShareExecutor{scheduler, client}, FairShareClass::Foreground, 0));
  pool.join();
}
