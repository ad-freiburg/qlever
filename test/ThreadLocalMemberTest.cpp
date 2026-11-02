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
#include <memory>
#include <thread>
#include <vector>

#include "util/ThreadLocalMember.h"

namespace {

using ad_utility::ThreadLocalMember;

// Run `f` on a new thread and wait for it to finish.
template <typename F>
void runOnThread(F f) {
  std::thread{std::move(f)}.join();
}

// Each thread and each object has its own value, which is only created by
// `getOrCreate()`.
TEST(ThreadLocalMember, valuesPerThreadAndObject) {
  ThreadLocalMember<int> a{[](int&) {}};
  ThreadLocalMember<int> b{[](int&) {}};
  EXPECT_EQ(a.getIfExists(), nullptr);
  int* valueA = a.getOrCreate();
  ASSERT_NE(valueA, nullptr);
  EXPECT_EQ(*valueA, 0);
  *valueA = 3;
  EXPECT_EQ(a.getOrCreate(), valueA);
  EXPECT_EQ(*a.getIfExists(), 3);
  *b.getOrCreate() = 5;
  EXPECT_EQ(*a.getIfExists(), 3);

  runOnThread([&]() {
    EXPECT_EQ(a.getIfExists(), nullptr);
    *a.getOrCreate() = 7;
    EXPECT_EQ(*a.getIfExists(), 7);
  });
  EXPECT_EQ(*a.getIfExists(), 3);
  EXPECT_EQ(*b.getIfExists(), 5);
}

// When a thread exits, the callback of each object is called with the value of
// that thread.
TEST(ThreadLocalMember, onThreadExit) {
  std::atomic<int> sum = 0;
  ThreadLocalMember<int> member{[&sum](int& value) { sum += value; }};
  std::vector<std::thread> threads;
  for (int i = 1; i <= 10; ++i) {
    threads.emplace_back([&member, i]() { *member.getOrCreate() = i; });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(sum, 55);
}

// Destroying an object discards its values in all threads, so that a new
// object, which might reuse its id, starts with fresh values, and the callback
// of the destroyed object is not called anymore.
TEST(ThreadLocalMember, destructionResetsValues) {
  std::atomic<int> step = 0;
  std::atomic<int> numCallbacks = 0;
  auto onExit = [&numCallbacks](int& value) {
    if (value != 0) {
      ++numCallbacks;
    }
  };
  auto member = std::make_unique<ThreadLocalMember<int>>(onExit);
  auto waitFor = [&step](int s) {
    while (step.load() != s) {
      std::this_thread::yield();
    }
  };
  std::thread worker{[&]() {
    *member->getOrCreate() = 42;
    step = 1;
    waitFor(2);
    // The new object doesn't see the value of the old one.
    EXPECT_EQ(*member->getOrCreate(), 0);
  }};
  waitFor(1);
  member = std::make_unique<ThreadLocalMember<int>>(onExit);
  step = 2;
  worker.join();
  EXPECT_EQ(numCallbacks, 0);
}

// An object that accesses a `ThreadLocalMember` in its destructor.
struct AccessesInDestructor {
  ThreadLocalMember<int>* member_ = nullptr;
  bool* gotValue_ = nullptr;
  ~AccessesInDestructor() {
    if (member_ != nullptr) {
      *gotValue_ = member_->getOrCreate() != nullptr ||
                   member_->getIfExists() != nullptr;
    }
  }
};

// After the values of a thread were destroyed at thread exit, no value is
// created anymore.
TEST(ThreadLocalMember, accessAfterThreadValuesWereDestroyed) {
  ThreadLocalMember<int> member{[](int&) {}};
  bool gotValue = true;
  runOnThread([&]() {
    // This object is constructed before the values, so it is destroyed after.
    static thread_local AccessesInDestructor object;
    object.member_ = &member;
    object.gotValue_ = &gotValue;
    *member.getOrCreate() = 1;
  });
  EXPECT_FALSE(gotValue);
}

}  // namespace
