// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <latch>
#include <memory>
#include <thread>

#include "util/ConcurrencyLimitedExecutor.h"
#include "util/ExecutorStatistics.h"

namespace {
using namespace ad_utility;
using namespace std::chrono_literals;
using Clock = ThreadPoolStatistics::Clock;

// The number of nanoseconds of a duration.
constexpr uint64_t ns(std::chrono::nanoseconds duration) {
  return static_cast<uint64_t>(duration.count());
}

// Use the CPU of the calling thread for at least the given `duration`.
void compute(std::chrono::nanoseconds duration) {
  auto start = threadCpuTime();
  while (threadCpuTime() - start < duration) {
  }
}
}  // namespace

// Test that `SharedTaskStatistics` sums up what is added to it.
TEST(ExecutorStatistics, sharedTaskStatisticsAddsUp) {
  // Two sets of statistics with different values for each member.
  TaskStatistics first{1, 2, 3, 4, 5, 6, 7, 8, 9};
  TaskStatistics second{10, 20, 30, 40, 50, 60, 70, 80, 90};
  SharedTaskStatistics shared;
  EXPECT_EQ(shared.snapshot(), TaskStatistics{});

  // Each member is the sum of the respective members.
  shared.add(first);
  shared.add(second);
  EXPECT_EQ(shared.snapshot(),
            (TaskStatistics{11, 22, 33, 44, 55, 66, 77, 88, 99}));

  // The result wait time can also be added on its own.
  shared.addResultWaitTime(45ns);
  EXPECT_EQ(shared.snapshot().resultWaitTime_, 100u);
}

// Test that `ThreadPoolStatistics` counts exactly the time during which a
// thread is free although a task is waiting.
TEST(ExecutorStatistics, idleWhileWaitingTime) {
  // A pool with two threads and three tasks `A`, `B`, and `C`.
  Clock::time_point t0{};
  ThreadPoolStatistics pool{2, t0};

  // `A` waits for 10 ms while both threads are free. Only one thread could
  // have run it, so this counts once.
  pool.taskWasPosted(t0);
  pool.taskWasStarted(t0 + 10ms);
  EXPECT_EQ(pool.snapshot(t0 + 10ms).idleWhileWaitingTime_, ns(10ms));

  // `B` starts right away and `C` waits for 20 ms while both threads are busy,
  // which does not count.
  pool.taskWasPosted(t0 + 10ms);
  pool.taskWasPosted(t0 + 10ms);
  pool.taskWasStarted(t0 + 10ms);
  EXPECT_EQ(pool.snapshot(t0 + 30ms).idleWhileWaitingTime_, ns(10ms));

  // `A` finishes and `C` waits for another 10 ms while one thread is free.
  pool.taskHasFinished(t0 + 30ms);
  pool.taskWasStarted(t0 + 40ms);
  EXPECT_EQ(pool.snapshot(t0 + 40ms).idleWhileWaitingTime_, ns(20ms));

  // Free threads without waiting tasks do not count.
  pool.taskHasFinished(t0 + 50ms);
  pool.taskHasFinished(t0 + 50ms);
  EXPECT_EQ(pool.snapshot(t0 + 100ms).idleWhileWaitingTime_, ns(20ms));
  EXPECT_EQ(pool.snapshot(t0 + 100ms).numThreads_, 2u);
}

// Test that the snapshot of a `ThreadPoolStatistics` derives the computing and
// the blocked time from the wall and CPU times of all classes.
TEST(ExecutorStatistics, computingAndBlockedTime) {
  // Tasks of two classes, where the CPU time of the second class is slightly
  // larger than its wall time (the two clocks are not read at the same time).
  ThreadPoolStatistics pool{4};
  TaskStatistics query;
  query.wallTime_ = 100;
  query.cpuTime_ = 60;
  TaskStatistics rebuild;
  rebuild.wallTime_ = 30;
  rebuild.cpuTime_ = 31;
  pool.ofClass(TaskClass::Query).add(query);
  pool.ofClass(TaskClass::IndexRebuild).add(rebuild);

  // The blocked time of a class is never negative.
  auto snapshot = pool.snapshot();
  EXPECT_EQ(snapshot.computingTime(), 91u);
  EXPECT_EQ(snapshot.blockedTime(), 40u);
  EXPECT_EQ(snapshot.ofClass(TaskClass::Query), query);
  EXPECT_EQ(snapshot.total().wallTime_, 130u);
  EXPECT_EQ(toString(TaskClass::IndexRebuild), "index_rebuild");
}

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17
// Test that an `ExecutorWithStatistics` measures the wait time, the wall time,
// and the CPU time of its tasks.
TEST(ExecutorStatistics, executorMeasuresTasks) {
  // A pool with a single thread, and statistics for it and for a single query.
  boost::asio::thread_pool threadPool{1};
  auto pool = std::make_shared<ThreadPoolStatistics>(1);
  auto query = std::make_shared<SharedTaskStatistics>();
  ExecutorWithStatistics executor{threadPool.get_executor(), pool,
                                  TaskClass::Query, query};

  // The first task sleeps (so it holds the thread without using a CPU), and
  // the second task, which has to wait for the first one, computes.
  boost::asio::post(executor, [] { std::this_thread::sleep_for(50ms); });
  boost::asio::post(executor, [] { compute(20ms); });
  threadPool.join();

  // Both tasks are counted for the class and for the query, and nowhere else.
  auto snapshot = pool->snapshot();
  const TaskStatistics& tasks = snapshot.ofClass(TaskClass::Query);
  EXPECT_EQ(tasks, query->snapshot());
  EXPECT_EQ(snapshot.ofClass(TaskClass::Other), TaskStatistics{});
  EXPECT_EQ(tasks.numTasks_, 2u);

  // The second task waited while the first one slept. The sleep is wall time,
  // but not CPU time.
  EXPECT_GE(tasks.waitTime_, ns(40ms));
  EXPECT_GE(tasks.wallTime_, ns(70ms));
  EXPECT_GE(tasks.cpuTime_, ns(20ms));
  EXPECT_LE(tasks.cpuTime_, tasks.wallTime_ - ns(40ms));
  EXPECT_GE(snapshot.blockedTime(), ns(40ms));
  EXPECT_EQ(tasks.numScanBlocks_, 0u);
}

// Test that `withStatisticsFor` measures each task once, including the time
// during which a `ConcurrencyLimitedExecutor` holds it back.
TEST(ExecutorStatistics, withStatisticsForIncludesTheWaitForTheLimit) {
  // A pool with four threads, of which a query may use only one.
  boost::asio::thread_pool threadPool{4};
  auto pool = std::make_shared<ThreadPoolStatistics>(4);
  auto query = std::make_shared<SharedTaskStatistics>();
  qlever::Executor base =
      makeExecutorWithStatistics(threadPool.get_executor(), pool);
  auto limitToOne = [](const qlever::Executor& underlyingExecutor) {
    return makeConcurrencyLimitedExecutor(underlyingExecutor, size_t{1});
  };
  qlever::Executor executor =
      withStatisticsFor(base, TaskClass::Query, query, limitToOne);

  // The second task is held back by the limit while three threads are free.
  boost::asio::post(executor, [] { std::this_thread::sleep_for(50ms); });
  boost::asio::post(executor, [] {});
  threadPool.join();

  // Both tasks are counted exactly once, as tasks of the query.
  auto snapshot = pool->snapshot();
  EXPECT_EQ(snapshot.total().numTasks_, 2u);
  EXPECT_EQ(snapshot.ofClass(TaskClass::Query).numTasks_, 2u);
  EXPECT_EQ(query->snapshot().numTasks_, 2u);
  EXPECT_GE(query->snapshot().waitTime_, ns(40ms));
  EXPECT_GE(snapshot.idleWhileWaitingTime_, ns(40ms));

  // For an executor without statistics, the wrapper is applied and nothing is
  // measured.
  std::atomic<bool> hasRun = false;
  boost::asio::thread_pool otherThreadPool{1};
  qlever::Executor withoutStatistics =
      withStatisticsFor(qlever::Executor{otherThreadPool.get_executor()},
                        TaskClass::Query, query, limitToOne);
  EXPECT_NE(withoutStatistics.target<ConcurrencyLimitedExecutor>(), nullptr);
  boost::asio::post(withoutStatistics, [&hasRun] { hasRun = true; });
  otherThreadPool.join();
  EXPECT_TRUE(hasRun);
  EXPECT_EQ(query->snapshot().numTasks_, 2u);
}

// Test that `recordScanBlockOfCurrentTask` adds the blocks to the task that
// reads them and counts the changes of the thread.
TEST(ExecutorStatistics, scanBlocksOfTasks) {
  // A pool with two threads and a scan with three blocks.
  boost::asio::thread_pool threadPool{2};
  auto pool = std::make_shared<ThreadPoolStatistics>(2);
  ExecutorWithStatistics executor{threadPool.get_executor(), pool,
                                  TaskClass::Query};
  std::atomic<std::thread::id> threadOfPreviousBlock;

  // Outside of a task, nothing is recorded.
  recordScanBlockOfCurrentTask(1000, threadOfPreviousBlock);
  EXPECT_EQ(threadOfPreviousBlock.load(), std::thread::id{});

  // The first two blocks are read by two different threads (each task waits
  // until the other one has started), which is one thread change.
  std::latch bothStarted{2};
  auto readBlock = [&](size_t numBytes) {
    return [&, numBytes] {
      bothStarted.count_down();
      bothStarted.wait();
      compute(1ms);
      recordScanBlockOfCurrentTask(numBytes, threadOfPreviousBlock);
    };
  };
  boost::asio::post(executor, readBlock(100));
  boost::asio::post(executor, readBlock(200));

  // A task that reads no block does not count as scan time.
  boost::asio::post(executor, [] { compute(1ms); });
  threadPool.join();

  auto tasks = pool->snapshot().ofClass(TaskClass::Query);
  EXPECT_EQ(tasks.numTasks_, 3u);
  EXPECT_EQ(tasks.numScanBlocks_, 2u);
  EXPECT_EQ(tasks.numScanBytes_, 300u);
  EXPECT_EQ(tasks.numScanThreadChanges_, 1u);
  EXPECT_GE(tasks.scanCpuTime_, ns(2ms));
  EXPECT_LE(tasks.scanCpuTime_, tasks.cpuTime_ - ns(1ms));
}
#endif
