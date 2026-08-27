//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <optional>
#include <stdexcept>
#include <vector>

#include "util/GTestHelpers.h"
#include "util/TaskQueue.h"
#include "util/ThreadSafeQueue.h"
#include "util/ValueIdentity.h"

using namespace std::chrono_literals;

TEST(TaskQueue, SimpleSum) {
  std::atomic<int> result;
  ad_utility::TaskQueue q{10, 5};
  for (size_t i = 0; i <= 1000; ++i) {
    q.push([&result, i] { result += i; });
  }
  q.finish();
  ASSERT_EQ(result, 500500);
}

TEST(TaskQueue, SimpleSumWithDestructor) {
  std::atomic<int> result;
  {
    ad_utility::TaskQueue q{10, 5};
    for (size_t i = 0; i <= 1000; ++i) {
      q.push([&result, i] { result += i; });
    }
  }  // destructor blocks, until everything is finished
  ASSERT_EQ(result, 500500);
}

TEST(TaskQueue, SimpleSumWithWait) {
  std::atomic<int> result;
  ad_utility::TaskQueue q{10, 5};
  for (size_t i = 0; i <= 1000; ++i) {
    q.push([&result, i] {
      std::this_thread::sleep_for(1ms);
      result += i;
    });
  }
  q.finish();
  ASSERT_EQ(result, 500500);
}

// ___________________________________________________________________
TEST(TaskQueue, ThrowOnMaxQueueSizeZero) {
  EXPECT_ANY_THROW((ad_utility::TaskQueue{0, 5}));
}

// ___________________________________________________________________
TEST(TaskQueue, finishFromWorkerThreadDoesntDeadlock) {
  auto runTest = [](auto trackTimes, auto destructorRunsFinish) {
    // We need the size of the queue to be larger than the number of pushes,
    // otherwise we cannot test the case where the destructor runs before
    // any of the threads have reached the call to `finish()`.
    ad_utility::TaskQueue<trackTimes> q{200, 5};
    for (size_t i = 0; i <= 100; ++i) {
      q.push([&q, destructorRunsFinish] {
        if (destructorRunsFinish) {
          std::this_thread::sleep_for(10ms);
        }
        q.finish();
      });
    }
    if (!destructorRunsFinish) {
      std::this_thread::sleep_for(10ms);
    }
  };
  using ad_utility::use_value_identity::vi;
  EXPECT_NO_THROW((runTest(vi<true>, vi<true>)));
  EXPECT_NO_THROW((runTest(vi<true>, vi<false>)));
  EXPECT_NO_THROW((runTest(vi<false>, vi<true>)));
  EXPECT_NO_THROW((runTest(vi<false>, vi<false>)));
}

// _____________________________________________________________________________
TEST(TaskQueue, runProducersCollectsAllValuesFromAllProducers) {
  ad_utility::TaskQueue pool{4, 4};
  ad_utility::data_structures::ThreadSafeQueue<size_t> queue{10};

  // A producer that yields the numbers 0 to 4 and then `std::nullopt`.
  std::atomic<size_t> counter1 = 0;
  std::atomic<size_t> counter2 = 0;
  auto makeProducer = [](std::atomic<size_t>& counter) {
    return [&counter]() -> std::optional<size_t> {
      size_t i = counter.fetch_add(1);
      if (i < 5) {
        return i;
      }
      return std::nullopt;
    };
  };

  std::vector<size_t> result;
  {
    auto cleanup = ad_utility::runProducers(pool, queue, makeProducer(counter1),
                                            makeProducer(counter2));
    while (auto value = queue.pop()) {
      result.push_back(value.value());
    }
  }
  EXPECT_THAT(result,
              ::testing::UnorderedElementsAre(0, 0, 1, 1, 2, 2, 3, 3, 4, 4));
}

// _____________________________________________________________________________
TEST(TaskQueue, runProducersPropagatesException) {
  ad_utility::TaskQueue pool{2, 2};
  ad_utility::data_structures::ThreadSafeQueue<int> queue{5};

  // A producer that yields a single value and then throws on the next call.
  std::atomic<int> count = 0;
  auto producer = [&count]() -> std::optional<int> {
    if (count.fetch_add(1) == 0) {
      return 42;
    }
    throw std::runtime_error("producer failure");
  };

  auto cleanup = ad_utility::runProducers(pool, queue, std::move(producer));
  // Draining the queue eventually rethrows the exception that the producer
  // pushed via `pushException`.
  AD_EXPECT_THROW_WITH_MESSAGE_AND_TYPE(
      {
        while (queue.pop()) {
        }
      },
      ::testing::StrEq("producer failure"), std::runtime_error);
}

// _____________________________________________________________________________
TEST(TaskQueue, runProducersStopsWhenQueueIsFinishedByConsumer) {
  ad_utility::TaskQueue pool{2, 2};
  // A queue size of one makes the producer block on a full queue, so that it is
  // guaranteed to observe the `finish` while trying to `push`.
  ad_utility::data_structures::ThreadSafeQueue<int> queue{1};
  std::atomic<size_t> numProduced = 0;

  {
    // An infinite producer.
    auto producer = [&numProduced]() -> std::optional<int> {
      ++numProduced;
      return 42;
    };

    auto cleanup = ad_utility::runProducers(pool, queue, producer);
    // Consume some elements, then close the queue from the consuming side.
    EXPECT_EQ(queue.pop(), 42);
    EXPECT_EQ(queue.pop(), 42);
    queue.finish();
    // The destructor of `cleanup` runs here and joins the producer thread. It
    // only returns if the producer actually broke out of its loop.
  }
  EXPECT_GT(numProduced.load(), 0u);
}
