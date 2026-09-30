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

#include <algorithm>
#include <boost/asio/post.hpp>
#include <boost/asio/thread_pool.hpp>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <vector>

#include "backports/algorithm.h"
#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/AllocatorTestHelpers.h"
#include "util/Exception.h"
#include "util/GTestHelpers.h"
#include "util/GlobalExecutor.h"
#include "util/HashSet.h"
#include "util/HpxAsioExecutor.h"

namespace {

namespace net = boost::asio;
using ad_utility::HpxAsioExecutor;

// The number of threads of the `asio` thread pool that the tests below run
// their work on.
constexpr size_t NUM_THREADS = 4;

// The number of elements that the sorting tests use. HPX only sorts in
// parallel if the input is larger than its minimal chunk size (which is 65536
// elements in HPX 1.11.0), so this deliberately is much larger than that.
constexpr size_t NUM_ELEMENTS = 500'000;

// A thread pool together with the IDs of its threads, such that the tests can
// check on which threads work has run, see `makeThreadPool`.
struct ThreadPoolWithIds {
  std::mutex mutex_;
  std::condition_variable conditionVariable_;
  size_t numStartedTasks_ = 0;
  ad_utility::HashSet<std::thread::id> threadIds_;
  // NOTE: The pool has to be the last member, such that it is the first member
  // that is destroyed, because its threads use the members above.
  net::thread_pool pool_{NUM_THREADS};

  // The executor of the pool, adapted for the use with HPX's algorithms.
  HpxAsioExecutor executor() {
    return HpxAsioExecutor{pool_.get_executor(), NUM_THREADS};
  }
};

// Create a thread pool and determine the IDs of all its threads. The IDs are
// determined by posting one task per thread, each of which only returns after
// all the other tasks have started, so that each of them runs on a different
// thread.
std::unique_ptr<ThreadPoolWithIds> makeThreadPool() {
  auto result = std::make_unique<ThreadPoolWithIds>();
  auto* pool = result.get();
  auto allTasksHaveStarted = [pool]() {
    return pool->numStartedTasks_ == NUM_THREADS;
  };
  for (size_t i = 0; i < NUM_THREADS; ++i) {
    net::post(pool->pool_, [pool, allTasksHaveStarted]() {
      std::unique_lock lock{pool->mutex_};
      pool->threadIds_.insert(std::this_thread::get_id());
      ++pool->numStartedTasks_;
      pool->conditionVariable_.notify_all();
      pool->conditionVariable_.wait(lock, allTasksHaveStarted);
    });
  }
  // NOTE: We deliberately do not `wait()` for the pool here, because that would
  // join its threads, after which it could no longer run the work of the tests.
  // All the IDs have been inserted once the condition below is true.
  std::unique_lock lock{pool->mutex_};
  pool->conditionVariable_.wait(lock, allTasksHaveStarted);
  AD_CORRECTNESS_CHECK(pool->threadIds_.size() == NUM_THREADS);
  return result;
}

// Create `NUM_ELEMENTS` pseudorandom numbers, always the same ones.
std::vector<int64_t> randomNumbers() {
  std::mt19937_64 randomEngine{4032};
  std::vector<int64_t> result;
  result.reserve(NUM_ELEMENTS);
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    result.push_back(static_cast<int64_t>(randomEngine() % 1'000'000));
  }
  return result;
}

// Compare two rows of an `IdTable` by their first column.
struct SortByFirstColumn {
  template <typename Row1, typename Row2>
  bool operator()(const Row1& a, const Row2& b) const {
    return a[0].getBits() < b[0].getBits();
  }
};

}  // namespace

// _____________________________________________________________________________
TEST(HpxAsioExecutor, asyncExecuteRunsOnTheAsioExecutor) {
  ad_utility::ensureHpxRuntimeIsRunning();
  auto pool = makeThreadPool();
  auto executor = pool->executor();

  auto future = hpx::parallel::execution::async_execute(
      executor, []() { return std::this_thread::get_id(); });
  EXPECT_THAT(pool->threadIds_, ::testing::Contains(future.get()));

  // The arguments of the function are forwarded by HPX.
  auto futureWithArgs = hpx::parallel::execution::async_execute(
      executor, [](int a, int b) { return a + b; }, 20, 22);
  EXPECT_EQ(futureWithArgs.get(), 42);
}

// _____________________________________________________________________________
TEST(HpxAsioExecutor, postRunsOnTheAsioExecutor) {
  ad_utility::ensureHpxRuntimeIsRunning();
  auto pool = makeThreadPool();
  auto executor = pool->executor();

  std::promise<std::thread::id> promise;
  auto future = promise.get_future();
  hpx::parallel::execution::post(executor, [&promise]() {
    promise.set_value(std::this_thread::get_id());
  });
  EXPECT_THAT(pool->threadIds_, ::testing::Contains(future.get()));
}

// _____________________________________________________________________________
TEST(HpxAsioExecutor, exceptionsArePropagated) {
  ad_utility::ensureHpxRuntimeIsRunning();
  auto pool = makeThreadPool();
  auto executor = pool->executor();

  auto future = hpx::parallel::execution::async_execute(
      executor, []() -> int { throw std::runtime_error{"expected error"}; });
  AD_EXPECT_THROW_WITH_MESSAGE(future.get(),
                               ::testing::HasSubstr("expected error"));
}

// _____________________________________________________________________________
TEST(HpxAsioExecutor, numberOfProcessingUnitsIsTheNumberOfThreads) {
  auto pool = makeThreadPool();
  EXPECT_EQ(
      hpx::execution::experimental::processing_units_count(pool->executor()),
      NUM_THREADS);
}

// _____________________________________________________________________________
TEST(HpxAsioExecutor, sortAVector) {
  ad_utility::ensureHpxRuntimeIsRunning();
  auto pool = makeThreadPool();
  auto executor = pool->executor();

  auto numbers = randomNumbers();
  auto expected = numbers;
  ql::ranges::sort(expected);

  // Record the threads on which the comparator runs, to make sure that the
  // work really is performed by the `asio` thread pool.
  std::mutex mutex;
  ad_utility::HashSet<std::thread::id> comparisonThreadIds;
  auto comparator = [&](int64_t a, int64_t b) {
    // NOTE: The thread-local flag makes each thread record its ID only once;
    // locking the mutex for every single comparison would dominate the runtime
    // of this test.
    thread_local bool idWasRecorded = false;
    if (!idWasRecorded) {
      idWasRecorded = true;
      std::lock_guard lock{mutex};
      comparisonThreadIds.insert(std::this_thread::get_id());
    }
    return a < b;
  };
  hpx::sort(hpx::execution::par.on(executor), numbers.begin(), numbers.end(),
            comparator);

  EXPECT_EQ(numbers, expected);
  // Every comparison ran either on one of the pool's threads, or on the thread
  // that called `hpx::sort` (HPX performs a few of the comparisons, for
  // example its check whether the input is already sorted, inline), and at
  // least one of them ran on the pool.
  auto allowedIds = pool->threadIds_;
  allowedIds.insert(std::this_thread::get_id());
  EXPECT_THAT(comparisonThreadIds, ::testing::Each(::testing::AnyOfArray(
                                       allowedIds.begin(), allowedIds.end())));
  EXPECT_THAT(comparisonThreadIds,
              ::testing::Contains(::testing::AnyOfArray(
                  pool->threadIds_.begin(), pool->threadIds_.end())));
}

// _____________________________________________________________________________
TEST(HpxAsioExecutor, sortAnIdTable) {
  ad_utility::ensureHpxRuntimeIsRunning();
  auto pool = makeThreadPool();
  auto executor = pool->executor();

  // Note: The rows of an `IdTableStatic` are proxy references, not references,
  // which is the interesting part of this test: HPX's `sort` has to handle
  // them correctly.
  IdTableStatic<2> table{ad_utility::testing::makeAllocator()};
  table.reserve(NUM_ELEMENTS);
  auto numbers = randomNumbers();
  for (size_t i = 0; i < NUM_ELEMENTS; ++i) {
    table.push_back({Id::makeFromInt(numbers[i]),
                     Id::makeFromInt(static_cast<int64_t>(i))});
  }
  auto expected = table.clone();
  ql::ranges::sort(expected, SortByFirstColumn{});

  hpx::sort(hpx::execution::par.on(executor), table.begin(), table.end(),
            SortByFirstColumn{});

  ASSERT_EQ(table.size(), expected.size());
  EXPECT_TRUE(ql::ranges::is_sorted(table, SortByFirstColumn{}));
  // The sort is not stable, so only the first column (the one that is sorted
  // by) can be compared directly.
  for (size_t i = 0; i < table.size(); ++i) {
    EXPECT_EQ(table(i, 0), expected(i, 0)) << "at row " << i;
  }
  // No row was lost or duplicated.
  std::vector<int64_t> secondColumn;
  secondColumn.reserve(table.size());
  for (const auto& row : table) {
    secondColumn.push_back(row[1].getInt());
  }
  ql::ranges::sort(secondColumn);
  for (size_t i = 0; i < secondColumn.size(); ++i) {
    ASSERT_EQ(secondColumn[i], static_cast<int64_t>(i));
  }
}

// _____________________________________________________________________________
TEST(HpxAsioExecutor, exceptionsFromTheComparatorArePropagated) {
  ad_utility::ensureHpxRuntimeIsRunning();
  auto pool = makeThreadPool();
  auto executor = pool->executor();

  auto numbers = randomNumbers();
  auto comparator = [](int64_t a, int64_t b) -> bool {
    if (a % 1000 == 17) {
      throw std::runtime_error{"expected error from the comparator"};
    }
    return a < b;
  };
  EXPECT_THROW(hpx::sort(hpx::execution::par.on(executor), numbers.begin(),
                         numbers.end(), comparator),
               std::exception);
}

// _____________________________________________________________________________
TEST(HpxAsioExecutor, theGlobalExecutorCanBeUsed) {
  auto executor = ad_utility::globalHpxExecutor();
  EXPECT_EQ(hpx::execution::experimental::processing_units_count(executor),
            ad_utility::globalExecutorNumThreads());

  auto numbers = randomNumbers();
  auto expected = numbers;
  ql::ranges::sort(expected);
  hpx::sort(hpx::execution::par.on(executor), numbers.begin(), numbers.end());
  EXPECT_EQ(numbers, expected);
}
