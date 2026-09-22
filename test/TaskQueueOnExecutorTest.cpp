// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio/strand.hpp>
#include <boost/asio/thread_pool.hpp>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "util/TaskQueueOnExecutor.h"
#include "util/jthread.h"

namespace {
using ad_utility::TaskQueueOnExecutor;
using namespace std::chrono_literals;

namespace net = boost::asio;

// The time that the tests wait for something that is expected to happen. It is
// deliberately generous, because it is only waited for in the (rare) case that
// the awaited event is slow, but never in the case that it never happens.
constexpr auto timeout = 10s;

// The time that the tests wait to become reasonably confident that something
// does NOT happen. It is deliberately short, because it is always waited for
// in full.
constexpr auto shortTimeout = 50ms;

// A helper to block a task until it is explicitly released. All the waiting is
// bounded, so that a broken implementation makes the tests fail instead of
// hang.
class Latch {
 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool isReleased_ = false;

 public:
  // Block until `release` is called (or until the `timeout` has passed).
  void wait() {
    std::unique_lock lock{mutex_};
    cv_.wait_for(lock, timeout, [this]() { return isReleased_; });
  }

  // Unblock all current and future calls to `wait`.
  void release() {
    std::lock_guard lock{mutex_};
    isReleased_ = true;
    cv_.notify_all();
  }
};

// Return true if the `future` becomes ready within the `timeout`.
template <typename T>
bool becomesReady(const std::future<T>& future) {
  return future.wait_for(timeout) == std::future_status::ready;
}

// Block until the `predicate` is true, but at most for the `timeout`. Return
// the final value of the `predicate`.
template <typename Predicate>
bool waitUntil(const Predicate& predicate) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!predicate() && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  return predicate();
}
}  // namespace

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, invalidArguments) {
  net::thread_pool pool{2};
  // The executor must not be empty.
  EXPECT_ANY_THROW(TaskQueueOnExecutor(net::any_io_executor{}, 2));
  // At least one task has to be allowed in flight.
  EXPECT_ANY_THROW(TaskQueueOnExecutor(pool.get_executor(), 0));
  // A valid combination.
  TaskQueueOnExecutor queue{pool.get_executor(), 3, "valid queue"};
  EXPECT_EQ(queue.maxNumTasksInFlight(), 3u);
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, allTasksAreRunAndFinishWaitsForThem) {
  net::thread_pool pool{4};
  std::atomic<size_t> counter = 0;
  constexpr size_t numTasks = 200;
  {
    TaskQueueOnExecutor queue{pool.get_executor(), 5, "allTasksAreRun"};
    for (size_t i = 0; i < numTasks; ++i) {
      queue.push([&counter]() { ++counter; });
    }
    queue.finish();
    EXPECT_EQ(counter.load(), numTasks);
    // A second call to `finish` returns immediately.
    queue.finish();
  }
  EXPECT_EQ(counter.load(), numTasks);
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, destructorWaitsForAllTasks) {
  net::thread_pool pool{4};
  std::atomic<size_t> counter = 0;
  constexpr size_t numTasks = 100;
  {
    TaskQueueOnExecutor queue{pool.get_executor(), 3, "destructorWaits"};
    for (size_t i = 0; i < numTasks; ++i) {
      queue.push([&counter]() {
        // The `yield` slows the tasks down, such that the destructor below is
        // likely to be entered while tasks are still in flight (otherwise the
        // test would also pass for a destructor that doesn't wait at all).
        std::this_thread::yield();
        ++counter;
      });
    }
  }
  EXPECT_EQ(counter.load(), numTasks);
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, inFlightBoundIsRespected) {
  // A single thread, so that at most one task runs at a time, and a bound of
  // two tasks in flight.
  net::thread_pool pool{1};
  Latch latch;
  std::atomic<size_t> numStartedTasks = 0;
  TaskQueueOnExecutor queue{pool.get_executor(), 2, "inFlightBound"};

  // The first two tasks fill the queue. The first one blocks the only thread
  // of the pool.
  queue.push([&latch, &numStartedTasks]() {
    ++numStartedTasks;
    latch.wait();
  });
  queue.push([&numStartedTasks]() { ++numStartedTasks; });
  ASSERT_TRUE(waitUntil([&numStartedTasks]() {
    return numStartedTasks.load() == 1u;
  })) << "The first task was not started by the pool";

  // The third `push` has to block, because two tasks are already in flight.
  std::atomic<bool> thirdPushHasReturned = false;
  ad_utility::JThread pusher{[&queue, &thirdPushHasReturned]() {
    queue.push([]() {});
    thirdPushHasReturned = true;
  }};
  std::this_thread::sleep_for(shortTimeout);
  EXPECT_FALSE(thirdPushHasReturned.load());
  EXPECT_EQ(numStartedTasks.load(), 1u);

  // Releasing the latch lets all the tasks and the blocked `push` proceed.
  latch.release();
  pusher.join();
  EXPECT_TRUE(thirdPushHasReturned.load());
  queue.finish();
  EXPECT_EQ(numStartedTasks.load(), 2u);
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, submitPropagatesValuesAndExceptions) {
  net::thread_pool pool{2};
  TaskQueueOnExecutor queue{pool.get_executor(), 4, "submit"};

  auto valueFuture = queue.submit([]() { return 42; });
  auto voidFuture = queue.submit([]() {});
  auto throwingFuture =
      queue.submit([]() -> int { throw std::runtime_error{"expected"}; });

  ASSERT_TRUE(becomesReady(valueFuture));
  EXPECT_EQ(valueFuture.get(), 42);
  ASSERT_TRUE(becomesReady(voidFuture));
  EXPECT_NO_THROW(voidFuture.get());
  ASSERT_TRUE(becomesReady(throwingFuture));
  EXPECT_THROW(throwingFuture.get(), std::runtime_error);
  queue.finish();
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, pushFromSeveralThreads) {
  net::thread_pool pool{4};
  std::atomic<size_t> counter = 0;
  constexpr size_t numThreads = 4;
  constexpr size_t numTasksPerThread = 100;
  {
    TaskQueueOnExecutor queue{pool.get_executor(), 3, "concurrentPushes"};
    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back([&queue, &counter]() {
        for (size_t j = 0; j < numTasksPerThread; ++j) {
          queue.push([&counter]() { ++counter; });
        }
      });
    }
  }
  EXPECT_EQ(counter.load(), numThreads * numTasksPerThread);
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, waitUntilFinished) {
  net::thread_pool pool{2};
  Latch latch;
  TaskQueueOnExecutor queue{pool.get_executor(), 2, "waitUntilFinished"};
  queue.push([&latch]() { latch.wait(); });

  std::atomic<bool> hasWaited = false;
  ad_utility::JThread waiter{[&queue, &hasWaited]() {
    queue.waitUntilFinished();
    hasWaited = true;
  }};
  std::this_thread::sleep_for(shortTimeout);
  EXPECT_FALSE(hasWaited.load());

  latch.release();
  queue.finish();
  waiter.join();
  EXPECT_TRUE(hasWaited.load());
  // `waitUntilFinished` returns immediately once the queue is finished.
  queue.waitUntilFinished();
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, waitUntilAllTasksAreDoneWorksForSeveralBatches) {
  net::thread_pool pool{4};
  std::atomic<size_t> counter = 0;
  constexpr size_t numTasksPerBatch = 20;
  constexpr size_t numBatches = 3;
  TaskQueueOnExecutor queue{pool.get_executor(), 3, "batches"};
  for (size_t batch = 0; batch < numBatches; ++batch) {
    Latch latch;
    // None of the tasks of this batch can complete before the `latch` is
    // released, so a `counter` that has the expected value below proves that
    // `waitUntilAllTasksAreDone` has really waited for all of them.
    ad_utility::JThread releaser{[&latch]() {
      std::this_thread::sleep_for(shortTimeout);
      latch.release();
    }};
    for (size_t i = 0; i < numTasksPerBatch; ++i) {
      queue.push([&counter, &latch]() {
        latch.wait();
        ++counter;
      });
    }
    queue.waitUntilAllTasksAreDone();
    EXPECT_EQ(counter.load(), (batch + 1) * numTasksPerBatch);
  }
  queue.finish();
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, pushAfterFinishIsAContractViolation) {
  net::thread_pool pool{2};
  TaskQueueOnExecutor queue{pool.get_executor(), 2, "pushAfterFinish"};
  queue.push([]() {});
  queue.finish();
  EXPECT_ANY_THROW(queue.push([]() {}));
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, aStrandRunsTheTasksInOrder) {
  // The queue itself doesn't guarantee any ordering, but with a strand as the
  // executor the tasks are run one after the other and in the order in which
  // they were pushed (by a single thread). This is the mode in which the first
  // user of this class (which will be added in a follow-up PR) runs callbacks
  // that have to observe the tasks in their original order.
  net::thread_pool pool{4};
  std::vector<size_t> result;
  constexpr size_t numTasks = 100;
  {
    TaskQueueOnExecutor queue{net::make_strand(pool.get_executor()), 3,
                              "strand"};
    for (size_t i = 0; i < numTasks; ++i) {
      // The `result` needs no synchronization, because the strand guarantees
      // that the tasks don't run concurrently.
      queue.push([&result, i]() { result.push_back(i); });
    }
  }
  std::vector<size_t> expected;
  for (size_t i = 0; i < numTasks; ++i) {
    expected.push_back(i);
  }
  EXPECT_EQ(result, expected);
}

// _____________________________________________________________________________
TEST(TaskQueueOnExecutor, aBoundOfOneRunsTheTasksSequentiallyAndInOrder) {
  // With a maximum of one task in flight, the next task is only pushed to the
  // executor after the previous one has completed. The tasks therefore never
  // run concurrently, and they run in the order in which they were pushed by
  // the single pushing thread, even though the underlying executor has
  // several threads.
  net::thread_pool pool{4};
  std::vector<size_t> result;
  std::atomic<size_t> numRunningTasks = 0;
  std::atomic<bool> sawConcurrentTasks = false;
  constexpr size_t numTasks = 100;
  {
    TaskQueueOnExecutor queue{pool.get_executor(), 1, "boundOfOne"};
    for (size_t i = 0; i < numTasks; ++i) {
      // The `result` needs no synchronization, because the tasks are ordered
      // by the queue (the completion of a task happens before the push of the
      // next one, which happens before the execution of that next task).
      queue.push([&result, &numRunningTasks, &sawConcurrentTasks, i]() {
        if (++numRunningTasks > 1) {
          sawConcurrentTasks = true;
        }
        result.push_back(i);
        --numRunningTasks;
      });
    }
  }
  std::vector<size_t> expected;
  for (size_t i = 0; i < numTasks; ++i) {
    expected.push_back(i);
  }
  EXPECT_EQ(result, expected);
  EXPECT_FALSE(sawConcurrentTasks);
}
