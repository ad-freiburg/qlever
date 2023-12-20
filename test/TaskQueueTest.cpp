//  Copyright 2021, University of Freiburg, Chair of Algorithms and Data
//  Structures. Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <vector>

#include "../src/util/TaskQueue.h"

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
  auto runTest = []<bool trackTimes, bool destructorRunsFinish>() {
    // We need the size of the queue to be larger than the number of pushes,
    // otherwise we cannot test the case where the destructor runs before
    // any of the threads have reached the call to `finish()`.
    ad_utility::TaskQueue<trackTimes> q{200, 5};
    for (size_t i = 0; i <= 100; ++i) {
      q.push([&q] {
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
  EXPECT_NO_THROW((runTest.template operator()<true, true>()));
  EXPECT_NO_THROW((runTest.template operator()<true, false>()));
  EXPECT_NO_THROW((runTest.template operator()<false, true>()));
  EXPECT_NO_THROW((runTest.template operator()<false, false>()));
}
