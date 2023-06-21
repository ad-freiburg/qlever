// Copyright 2023, University of Freiburg,
//                 Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach (kalmbach@cs.uni-freiburg.de)

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <atomic>

#include "util/ThreadSafeQueue.h"
#include "util/jthread.h"


using namespace ad_utility::data_structures;

// ________________________________________________________________
TEST(ThreadSafeQueue, BufferSizeIsRespected) {
  std::atomic<int> numPushed = 0;

  ThreadSafeQueue<int> queue{5};

  ad_utility::JThread t([&numPushed, &queue]{
    while(numPushed < 200) {
      queue.push(numPushed++);
    }
    queue.signalLastElementWasPushed();
  });

  size_t numPopped = 0;
  while (auto opt = queue.pop()) {
    EXPECT_EQ(opt.value(), numPopped);
    ++numPopped;
    EXPECT_LE(numPushed, numPopped + 6);
  }
}

// _______________________________________________________________
TEST(ThreadSafeQueue, ReturnValueOfPush) {
  ThreadSafeQueue<int> queue{3};
  EXPECT_TRUE(queue.push(42));
  EXPECT_EQ(queue.pop(), 42);
  queue.disablePush();
  EXPECT_FALSE(queue.push(15));
}

// ________________________________________________________________
TEST(ThreadSafeQueue, Concurrency) {
    std::atomic<size_t> numPushed = 0;

    ThreadSafeQueue<size_t> queue{5};

    size_t numThreads = 20;
    auto push = [&numPushed, &queue, numThreads]{
        for (size_t i = 0; i < 200; ++i) {
            queue.push(numPushed++);
        }
        if (numPushed == 200 * numThreads) {
            queue.signalLastElementWasPushed();
        }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
      threads.emplace_back(push);
    }

    size_t numPopped = 0;
    std::vector<size_t> result;
    while (auto opt = queue.pop()) {
        ++numPopped;
        result.push_back(opt.value());
        EXPECT_LE(numPushed, numPopped + 6 + numThreads);
    }

    std::ranges::sort(result);
    std::vector<size_t> expected;
    for (size_t i = 0; i < 200 * numThreads; ++i) {
        expected.push_back(i);
    }
    EXPECT_EQ(result, expected);
}

// ________________________________________________________________
TEST(ThreadSafeQueue, PushException) {
    std::atomic<size_t> numPushed = 0;
    std::atomic<size_t> threadIndex = 0;

    ThreadSafeQueue<size_t> queue{5};

    size_t numThreads = 20;
    auto push = [&numPushed, &queue, &threadIndex]{
        for (size_t i = 0; i < 200; ++i) {
            if (numPushed > 300) {
              try {
                throw static_cast<int>(threadIndex++);
              } catch(...) {
                queue.pushException(std::current_exception());
              }
              EXPECT_FALSE(queue.push(numPushed++));
            } else {
                queue.push(numPushed++);
            }
        }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back(push);
    }

    size_t numPopped = 0;

    try {
        while (auto opt = queue.pop()) {
          ++numPopped;
          EXPECT_LE(numPushed, numPopped + 6 + numThreads);
        }
        FAIL() << "Should have thrown" << std::endl;
    } catch(int i) {
        EXPECT_LT(i, numThreads);
    }
}

// ________________________________________________________________
TEST(ThreadSafeQueue, DisablePush) {
    std::atomic<size_t> numPushed = 0;

    ThreadSafeQueue<size_t> queue{5};

    size_t numThreads = 20;
    auto push = [&numPushed, &queue, numThreads]{
      for (size_t i = 0; i < 200; ++i) {
        if (!queue.push(numPushed++)) {
          return;
        }
      }
    };

    std::vector<ad_utility::JThread> threads;
    for (size_t i = 0; i < numThreads; ++i) {
        threads.emplace_back(push);
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
}

