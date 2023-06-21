// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include <gtest/gtest.h>

#include <atomic>

#include "util/ThreadSafeQueue.h"
#include "util/TypeTraits.h"
#include "util/jthread.h"

using namespace ad_utility::data_structures;

namespace {

template <typename Queue>
auto makePush(Queue& queue) {
  return [&queue](size_t i) {
    if constexpr (ad_utility::similarToInstantiation<Queue, ThreadSafeQueue>) {
      return queue.push(i);
    } else {
      return queue.push(i, i);
    }
  };
}

constexpr size_t queueSize = 5;

void runWithBothQueueTypes(const auto& testFunction) {
  testFunction(ThreadSafeQueue<size_t>{queueSize});
  testFunction(OrderedThreadSafeQueue<size_t>{queueSize});
}
}  // namespace

// ________________________________________________________________
TEST(ThreadSafeQueue, BufferSizeIsRespected) {
  auto runTest = [](auto queue) {
    std::atomic<int> numPushed = 0;
    auto push = makePush(queue);

    ad_utility::JThread t([&numPushed, &push, &queue] {
      while (numPushed < 200) {
        push(numPushed++);
      }
      queue.signalLastElementWasPushed();
    });

    size_t numPopped = 0;
    while (auto opt = queue.pop()) {
      EXPECT_EQ(opt.value(), numPopped);
      ++numPopped;
      EXPECT_LE(numPushed, numPopped + queueSize + 1);
    }
  };
  runWithBothQueueTypes(runTest);
}

// _______________________________________________________________
TEST(ThreadSafeQueue, ReturnValueOfPush) {
  auto runTest = [](auto queue) {
    auto push = makePush(queue);
    EXPECT_TRUE(push(0));
    EXPECT_EQ(queue.pop(), 0);
    queue.disablePush();
    EXPECT_FALSE(push(1));
  };
  runWithBothQueueTypes(runTest);
}

// ________________________________________________________________
TEST(ThreadSafeQueue, Concurrency) {
  auto runTest = []<typename Queue>(Queue queue) {
    std::atomic<size_t> numPushed = 0;
    std::atomic<size_t> numThreadsDone = 0;
    auto push = makePush(queue);

    size_t numThreads = 20;
    auto threadFunction = [&numPushed, &queue, numThreads, &push,
                           &numThreadsDone] {
      for (size_t i = 0; i < 200; ++i) {
        push(numPushed++);
      }
      numThreadsDone++;
      if (numThreadsDone == numThreads) {
        queue.signalLastElementWasPushed();
      }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back(threadFunction);
    }

    size_t numPopped = 0;
    std::vector<size_t> result;
    while (auto opt = queue.pop()) {
      ++numPopped;
      result.push_back(opt.value());
      EXPECT_LE(numPushed, numPopped + 6 + numThreads);
    }

    if (ad_utility::isInstantiation<Queue, ThreadSafeQueue>) {
      std::ranges::sort(result);
    }
    std::vector<size_t> expected;
    for (size_t i = 0; i < 200 * numThreads; ++i) {
      expected.push_back(i);
    }
    EXPECT_EQ(result, expected);
  };
  runWithBothQueueTypes(runTest);
}

// ________________________________________________________________
TEST(ThreadSafeQueue, PushException) {
  auto runTest = [](auto queue) {
    std::atomic<size_t> numPushed = 0;
    std::atomic<size_t> threadIndex = 0;

    auto push = makePush(queue);

    struct IntegerException : public std::exception {
      int value_;
      explicit IntegerException(int value) : value_{value} {}
    };

    size_t numThreads = 20;
    auto threadFunction = [&numPushed, &queue, &threadIndex, &push] {
      bool hasThrown = false;
      for (size_t i = 0; i < 200; ++i) {
        if (numPushed > 300 && !hasThrown) {
          hasThrown = true;
          try {
            int idx = threadIndex++;
            throw IntegerException(idx);
          } catch (...) {
            queue.pushException(std::current_exception());
          }
          EXPECT_FALSE(push(numPushed++));
        } else {
          push(numPushed++);
        }
      }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back(threadFunction);
    }

    size_t numPopped = 0;

    try {
      while (auto opt = queue.pop()) {
        ++numPopped;
        EXPECT_LE(numPushed, numPopped + 6 + numThreads);
      }
      FAIL() << "Should have thrown" << std::endl;
    } catch (const IntegerException& i) {
      EXPECT_LT(i.value_, numThreads);
    }
  };
  runWithBothQueueTypes(runTest);
}

// ________________________________________________________________
TEST(ThreadSafeQueue, DisablePush) {
  auto runTest = [](auto queue) {
    std::atomic<size_t> numPushed = 0;
    auto push = makePush(queue);

    size_t numThreads = 20;
    auto threadFunction = [&numPushed, &push] {
      for (size_t i = 0; i < 200; ++i) {
        if (!push(numPushed++)) {
          return;
        }
      }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back(threadFunction);
    }

    size_t numPopped = 0;
    std::vector<size_t> result;
    while (auto opt = queue.pop()) {
      ++numPopped;
      result.push_back(opt.value());
      EXPECT_LE(numPushed, numPopped + 6 + numThreads);

      if (numPopped == 400) {
        queue.disablePush();
        break;
      }
    }

    // When terminating early, we cannot actually say much about the result.
    std::ranges::sort(result);
    EXPECT_TRUE(std::unique(result.begin(), result.end()) == result.end());
  };
  runWithBothQueueTypes(runTest);
}
