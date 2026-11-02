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

// A stand-in for the values of a thread, which records the calls of the
// registry.
struct MockThread {
  std::vector<uint32_t> resetIds_;
  size_t size_ = 0;
  void reset(uint32_t id) { resetIds_.push_back(id); }
  void resize(size_t size) { size_ = size; }
};
using Registry = ad_utility::detail::ThreadLocalMemberRegistry<int, MockThread>;

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

// The registry assigns small ids, reuses the ids of released objects, and
// stores the assigned id.
TEST(ThreadLocalMemberRegistry, ids) {
  Registry registry;
  int a = 0;
  int b = 0;
  int c = 0;
  std::atomic<uint32_t> idA = Registry::noId;
  std::atomic<uint32_t> idB = Registry::noId;
  std::atomic<uint32_t> idC = Registry::noId;
  EXPECT_EQ(registry.getOrAllocateId(idA, &a), 0u);
  EXPECT_EQ(idA, 0u);
  // An assigned id is returned unchanged.
  EXPECT_EQ(registry.getOrAllocateId(idA, &a), 0u);
  EXPECT_EQ(registry.getOrAllocateId(idB, &b), 1u);
  registry.releaseId(0);
  // The released id is reused.
  EXPECT_EQ(registry.getOrAllocateId(idC, &c), 0u);
  EXPECT_EQ(idC, 0u);
}

// Releasing an id resets it in all registered threads, the callback of
// `unregisterThread` sees exactly the live objects below the given bound, and
// unregistered threads are not reset anymore.
TEST(ThreadLocalMemberRegistry, threads) {
  Registry registry;
  MockThread thread1;
  MockThread thread2;
  registry.registerThread(&thread1);
  registry.registerThread(&thread2);
  registry.resize(thread1, 3);
  EXPECT_EQ(thread1.size_, 3u);

  std::vector<int> objects(3);
  std::vector<std::atomic<uint32_t>> ids(3);
  for (size_t i = 0; i < 3; ++i) {
    ids[i] = Registry::noId;
    EXPECT_EQ(registry.getOrAllocateId(ids[i], &objects[i]), i);
  }
  registry.releaseId(1);
  EXPECT_THAT(thread1.resetIds_, ::testing::ElementsAre(1u));
  EXPECT_THAT(thread2.resetIds_, ::testing::ElementsAre(1u));

  std::vector<std::pair<int*, uint32_t>> visited;
  auto visit = [&visited](int& object, uint32_t id) {
    visited.emplace_back(&object, id);
  };
  // The bound may also exceed the number of ids.
  registry.unregisterThread(&thread1, 10, visit);
  EXPECT_THAT(visited, ::testing::ElementsAre(std::pair{&objects[0], 0u},
                                              std::pair{&objects[2], 2u}));
  visited.clear();
  registry.unregisterThread(&thread2, 1, visit);
  EXPECT_THAT(visited, ::testing::ElementsAre(std::pair{&objects[0], 0u}));

  registry.releaseId(2);
  EXPECT_THAT(thread1.resetIds_, ::testing::ElementsAre(1u));
  EXPECT_THAT(thread2.resetIds_, ::testing::ElementsAre(1u));
}

// Many threads concurrently allocate and release ids. At no time do two live
// objects share an id.
TEST(ThreadLocalMemberRegistry, concurrentIds) {
  Registry registry;
  constexpr size_t numThreads = 8;
  // For each id, the number of objects that currently hold it.
  std::vector<std::atomic<int>> holders(numThreads);
  std::atomic<bool> sharedId = false;
  std::vector<std::thread> threads;
  for (size_t t = 0; t < numThreads; ++t) {
    threads.emplace_back([&]() {
      int object = 0;
      for (size_t i = 0; i < 10'000; ++i) {
        std::atomic<uint32_t> id = Registry::noId;
        uint32_t assigned = registry.getOrAllocateId(id, &object);
        // At most `numThreads` objects are alive at the same time.
        if (assigned >= numThreads || holders[assigned]++ != 0) {
          sharedId = true;
        }
        --holders[std::min<size_t>(assigned, numThreads - 1)];
        registry.releaseId(assigned);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_FALSE(sharedId);
}

}  // namespace
