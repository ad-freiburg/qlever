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

// Post `numTasks` tasks to `executor` and wait for all of them.
void runAndWait(const qlever::Executor& executor, size_t numTasks,
                std::chrono::microseconds duration,
                ConcurrencyTracker* tracker = nullptr) {
  std::vector<std::future<void>> futures;
  for (size_t i = 0; i < numTasks; ++i) {
    futures.push_back(boost::asio::post(
        executor, boost::asio::use_future([tracker, duration]() {
          if (tracker) tracker->enter();
          spin(duration);
          if (tracker) tracker->leave();
        })));
  }
  for (auto& future : futures) {
    future.get();
  }
}
}  // namespace

// A single client without a limit gets all the slots.
TEST(FairShareExecutor, singleClientIsWorkConserving) {
  boost::asio::thread_pool pool{8};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 4);
  qlever::Executor executor = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Queries)};
  ConcurrencyTracker tracker;
  runAndWait(executor, 64, 2000us, &tracker);
  EXPECT_EQ(tracker.max_, 4u);
  pool.join();
}

// The limit of a client is respected.
TEST(FairShareExecutor, maxConcurrencyOfClient) {
  boost::asio::thread_pool pool{8};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 8);
  qlever::Executor root = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Background)};
  auto limited = makeFairShareClientExecutor(root, FairShareClass::Queries, 2);
  ConcurrencyTracker tracker;
  runAndWait(limited, 32, 2000us, &tracker);
  EXPECT_EQ(tracker.max_, 2u);
  pool.join();
}

// Under contention, the classes get the threads according to their shares,
// no matter how many tasks each of them has queued.
TEST(FairShareExecutor, sharesOfClasses) {
  boost::asio::thread_pool pool{4};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 4);
  scheduler->setShare(FairShareClass::Background, 0.25);
  scheduler->setShare(FairShareClass::Queries, 0.75);
  qlever::Executor background = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Background)};
  // Many query clients, each of which floods the scheduler with tasks.
  std::vector<qlever::Executor> queries;
  for (size_t i = 0; i < 8; ++i) {
    queries.push_back(
        makeFairShareClientExecutor(background, FairShareClass::Queries, 4));
  }
  std::atomic<size_t> backgroundDone = 0;
  std::atomic<size_t> queryDone = 0;
  std::atomic<bool> stop = false;
  // Keep both classes saturated for a while.
  auto feed = [&](qlever::Executor executor, std::atomic<size_t>& counter) {
    auto self = std::make_shared<std::function<void()>>();
    *self = [&, executor, self]() {
      if (stop) return;
      spin(500us);
      ++counter;
      boost::asio::post(executor, *self);
    };
    for (size_t i = 0; i < 8; ++i) {
      boost::asio::post(executor, *self);
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

// If only the background class has work, it gets all the threads.
TEST(FairShareExecutor, idleClassLeavesItsShareToOthers) {
  boost::asio::thread_pool pool{4};
  auto scheduler = std::make_shared<FairScheduler>(pool.get_executor(), 4);
  scheduler->setShare(FairShareClass::Background, 0.1);
  scheduler->setShare(FairShareClass::Queries, 0.9);
  qlever::Executor background = FairShareExecutor{
      scheduler, scheduler->makeClient(FairShareClass::Background)};
  ConcurrencyTracker tracker;
  runAndWait(background, 64, 2000us, &tracker);
  EXPECT_EQ(tracker.max_, 4u);
  pool.join();
}

// `makeFairShareExecutor` wraps a plain executor into a new scheduler, which
// runs at most `numSlots` tasks at the same time, and from which clients of
// other classes can be derived.
TEST(FairShareExecutor, makeFairShareExecutor) {
  boost::asio::thread_pool pool{8};
  auto executor = makeFairShareExecutor(pool.get_executor(), 3);
  EXPECT_NE(executor.target<FairShareExecutor>(), nullptr);
  ConcurrencyTracker tracker;
  runAndWait(executor, 32, 2000us, &tracker);
  EXPECT_EQ(tracker.max_, 3u);

  auto query = makeFairShareClientExecutor(executor, FairShareClass::Queries,
                                           std::nullopt);
  ASSERT_NE(query.target<FairShareExecutor>(), nullptr);
  EXPECT_EQ(query.target<FairShareExecutor>()->scheduler(),
            executor.target<FairShareExecutor>()->scheduler());
  EXPECT_FALSE(query == executor);

  // Two calls create independent schedulers.
  auto other = makeFairShareExecutor(pool.get_executor(), 3);
  EXPECT_NE(other.target<FairShareExecutor>()->scheduler(),
            executor.target<FairShareExecutor>()->scheduler());

  EXPECT_ANY_THROW(makeFairShareExecutor(pool.get_executor(), 0));
  EXPECT_ANY_THROW(makeFairShareExecutor(qlever::Executor{}, 2));
  pool.join();
}

// Executors that are not `FairShareExecutor`s fall back to a concurrency limit.
TEST(FairShareExecutor, fallbackForOtherExecutors) {
  boost::asio::thread_pool pool{8};
  auto limited = makeFairShareClientExecutor(pool.get_executor(),
                                             FairShareClass::Queries, 3);
  ConcurrencyTracker tracker;
  runAndWait(limited, 32, 2000us, &tracker);
  EXPECT_EQ(tracker.max_, 3u);
  auto unlimited = makeFairShareClientExecutor(
      pool.get_executor(), FairShareClass::Queries, std::nullopt);
  EXPECT_TRUE(unlimited == qlever::Executor{pool.get_executor()});
  pool.join();
}
