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

TEST(TaskQueue, SimpleSumWithManualPop) {
  std::atomic<int> result;
  // 0 threads, so we have to pickUp the threads manually from another thread.
  ad_utility::TaskQueue q{10, 0};
  auto future = std::async(std::launch::async, [&] {
    for (size_t i = 0; i <= 1000; ++i) {
      q.push([&result, i] { result += i; });
    }
    q.finish();
  });
  while (true) {
    auto taskAsOptional = q.popManually();
    if (!taskAsOptional) {
      break;
    }
    // execute the task;
    (*taskAsOptional)();
  }
  future.get();
  ASSERT_EQ(result, 500500);
}
