// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "backports/atomic_flag.h"

using namespace std::chrono_literals;

// _____________________________________________________________________________
// Basic flag operations
TEST(AtomicFlagTest, Construction) {
  using ql::backports::atomic_flag;
  {
    atomic_flag flag;
    // Default constructed flag should be false
    EXPECT_FALSE(flag.test());
  }
  {
    atomic_flag flag{false};
    // Default constructed flag should be false
    EXPECT_FALSE(flag.test());
  }
  {
    atomic_flag flag{true};
    // Default constructed flag should be false
    EXPECT_TRUE(flag.test());
  }
}

// _____________________________________________________________________________
TEST(AtomicFlagTest, TestAndSet) {
  ql::backports::atomic_flag flag;

  // First test_and_set should return false (previous value)
  EXPECT_FALSE(flag.test_and_set());
  // Flag should now be true
  EXPECT_TRUE(flag.test());

  // Second test_and_set should return true (previous value)
  EXPECT_TRUE(flag.test_and_set());
  // Flag should still be true
  EXPECT_TRUE(flag.test());
}

// _____________________________________________________________________________
TEST(AtomicFlagTest, Clear) {
  ql::backports::atomic_flag flag;

  flag.test_and_set();
  EXPECT_TRUE(flag.test());

  flag.clear();
  EXPECT_FALSE(flag.test());
}

// _____________________________________________________________________________
TEST(AtomicFlagTest, TestWithoutModifying) {
  ql::backports::atomic_flag flag;

  // Multiple calls to test() should not change the flag
  EXPECT_FALSE(flag.test());
  EXPECT_FALSE(flag.test());
  EXPECT_FALSE(flag.test());

  flag.test_and_set();
  EXPECT_TRUE(flag.test());
  EXPECT_TRUE(flag.test());
  EXPECT_TRUE(flag.test());
}

// _____________________________________________________________________________
// Memory ordering tests.
// Note: These currently don't do much. Proper stress tests for the memory order
// behavior are difficult to write, especially on Intel/AMD where the hardware
// memory model is very strong by default.
TEST(AtomicFlagTest, MemoryOrderingRelaxed) {
  ql::backports::atomic_flag flag;

  EXPECT_FALSE(flag.test(std::memory_order_relaxed));
  EXPECT_FALSE(flag.test_and_set(std::memory_order_relaxed));
  EXPECT_TRUE(flag.test(std::memory_order_relaxed));
  flag.clear(std::memory_order_relaxed);
  EXPECT_FALSE(flag.test(std::memory_order_relaxed));
}

// _____________________________________________________________________________
TEST(AtomicFlagTest, MemoryOrderingAcquireRelease) {
  ql::backports::atomic_flag flag;

  EXPECT_FALSE(flag.test(std::memory_order_acquire));
  EXPECT_FALSE(flag.test_and_set(std::memory_order_acquire));
  EXPECT_TRUE(flag.test(std::memory_order_acquire));
  flag.clear(std::memory_order_release);
  EXPECT_FALSE(flag.test(std::memory_order_acquire));
}

// _____________________________________________________________________________
// Wait/notify functionality tests
TEST(AtomicFlagTest, WaitNotifyOne) {
  ql::backports::atomic_flag flag;
  ql::backports::atomic_flag thread_ready;
  ql::backports::atomic_flag test_passed;

  std::thread waiter([&]() {
    thread_ready.test_and_set();
    flag.wait(false);  // Wait until flag becomes true
    if (flag.test()) {
      test_passed.test_and_set();
    }
  });

  // Wait for thread to be ready
  thread_ready.wait(false);

  flag.test_and_set();  // This will notify
  waiter.join();
  EXPECT_TRUE(test_passed.test());
}

// _____________________________________________________________________________
TEST(AtomicFlagTest, WaitNotifyAll) {
  ql::backports::atomic_flag flag;
  std::atomic<int> threads_ready{0};
  std::atomic<int> threads_passed{0};
  constexpr int num_threads = 5;
  ql::backports::atomic_flag all_ready;

  std::vector<std::thread> waiters;
  for (int i = 0; i < num_threads; ++i) {
    waiters.emplace_back([&]() {
      if (threads_ready.fetch_add(1) + 1 == num_threads) {
        all_ready.test_and_set();
      }
      flag.wait(false);  // Wait until flag becomes true
      if (flag.test()) {
        threads_passed.fetch_add(1);
      }
    });
  }

  // Wait for all threads to be ready
  all_ready.wait(false);

  flag.test_and_set();  // This will notify all

  for (auto& thread : waiters) {
    thread.join();
  }

  EXPECT_EQ(threads_passed.load(), num_threads);
}

// _____________________________________________________________________________
TEST(AtomicFlagTest, WaitDoesNotBlockIfValueDifferent) {
  ql::backports::atomic_flag flag;
  flag.test_and_set();  // Set flag to true

  // This should return immediately since flag is true, not false
  auto start = std::chrono::steady_clock::now();
  flag.wait(false);
  auto duration = std::chrono::steady_clock::now() - start;

  // Should complete nearly instantly (definitely less than 100ms)
  EXPECT_LT(duration, 100ms);
}

// _____________________________________________________________________________
// Concurrent access tests
TEST(AtomicFlagTest, ConcurrentTestAndSet) {
  ql::backports::atomic_flag flag;
  std::atomic<int> success_count{0};
  constexpr int num_threads = 10;

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      // Only the first thread to call test_and_set should see false
      if (!flag.test_and_set()) {
        success_count.fetch_add(1);
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Exactly one thread should have succeeded
  EXPECT_EQ(success_count.load(), 1);
  EXPECT_TRUE(flag.test());
}

// _____________________________________________________________________________
TEST(AtomicFlagTest, ConcurrentClearAndTestAndSet) {
  ql::backports::atomic_flag flag;
  std::atomic<bool> stop{false};
  std::atomic<int> set_count{0};

  auto setter = [&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      if (!flag.test_and_set()) {
        set_count.fetch_add(1);
      }
      std::this_thread::sleep_for(1ms);
    }
  };

  auto clearer = [&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      flag.clear();
      std::this_thread::sleep_for(1ms);
    }
  };

  std::thread t1(setter);
  std::thread t2(setter);
  std::thread t3(clearer);

  std::this_thread::sleep_for(50ms);
  stop.store(true);

  t1.join();
  t2.join();
  t3.join();

  // Should have had multiple successful sets
  EXPECT_GT(set_count.load(), 0);
}

// _____________________________________________________________________________
// Signal/Wait pattern tests
TEST(AtomicFlagTest, SignalWaitPattern) {
  ql::backports::atomic_flag ready;
  std::atomic<int> data{0};
  ql::backports::atomic_flag worker_done;

  std::thread worker([&]() {
    // Wait for signal
    ready.wait(false);

    // Process data
    EXPECT_EQ(data.load(), 42);
    data.store(100);
    worker_done.test_and_set();
  });

  // Prepare data
  data.store(42);

  // Signal worker
  ready.test_and_set();  // This will notify

  worker.join();
  EXPECT_TRUE(worker_done.test());
  EXPECT_EQ(data.load(), 100);
}

// _____________________________________________________________________________
// Test that clear() also notifies
TEST(AtomicFlagTest, ClearNotifies) {
  ql::backports::atomic_flag flag;
  flag.test_and_set();  // Start with flag set to true

  ql::backports::atomic_flag thread_ready;
  ql::backports::atomic_flag wait_completed;

  std::thread waiter([&]() {
    thread_ready.test_and_set();
    flag.wait(true);  // Wait for flag to become false
    wait_completed.test_and_set();
  });

  // Wait for thread to be ready
  thread_ready.wait(false);

  // Clear should notify
  flag.clear();

  waiter.join();
  EXPECT_TRUE(wait_completed.test());
  EXPECT_FALSE(flag.test());
}

// _____________________________________________________________________________
// Test explicit notify_one
TEST(AtomicFlagTest, ExplicitNotifyOne) {
  ql::backports::atomic_flag flag;
  ql::backports::atomic_flag thread_ready;
  ql::backports::atomic_flag wait_completed;

  std::thread waiter([&]() {
    thread_ready.test_and_set();
    flag.wait(false);
    wait_completed.test_and_set();
  });

  // Wait for thread to be ready
  thread_ready.wait(false);

  // Set flag without automatic notification in the middle
  // (test_and_set does notify, so we test the explicit API)
  flag.test_and_set();

  waiter.join();
  EXPECT_TRUE(wait_completed.test());
}

// _____________________________________________________________________________
// Test explicit notify_all
TEST(AtomicFlagTest, ExplicitNotifyAll) {
  ql::backports::atomic_flag flag;
  std::atomic<int> threads_ready{0};
  std::atomic<int> waits_completed{0};
  constexpr int num_threads = 3;
  ql::backports::atomic_flag all_ready;

  std::vector<std::thread> waiters;
  for (int i = 0; i < num_threads; ++i) {
    waiters.emplace_back([&]() {
      if (threads_ready.fetch_add(1) + 1 == num_threads) {
        all_ready.test_and_set();
      }
      flag.wait(false);
      waits_completed.fetch_add(1);
    });
  }

  // Wait for all threads to be ready
  all_ready.wait(false);

  // Set and notify all
  flag.test_and_set();

  for (auto& thread : waiters) {
    thread.join();
  }

  EXPECT_EQ(waits_completed.load(), num_threads);
}

// _____________________________________________________________________________
// Stress test with many operations
TEST(AtomicFlagTest, StressTest) {
  ql::backports::atomic_flag flag;
  std::atomic<bool> stop{false};
  std::atomic<int> operations{0};

  auto worker = [&]() {
    while (!stop.load(std::memory_order_relaxed)) {
      flag.test_and_set();
      operations.fetch_add(1, std::memory_order_relaxed);
      flag.clear();
      operations.fetch_add(1, std::memory_order_relaxed);
      flag.test();
      operations.fetch_add(1, std::memory_order_relaxed);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(worker);
  }

  std::this_thread::sleep_for(100ms);
  stop.store(true, std::memory_order_relaxed);

  for (auto& thread : threads) {
    thread.join();
  }

  // Should have performed many operations
  EXPECT_GT(operations.load(), 1000);
}

// _____________________________________________________________________________
// Test that wait returns immediately when value already differs
TEST(AtomicFlagTest, WaitReturnsImmediatelyWhenValueDiffers) {
  ql::backports::atomic_flag flag;

  // Flag is false, wait for it to be different from true
  auto start = std::chrono::steady_clock::now();
  flag.wait(true);
  auto duration = std::chrono::steady_clock::now() - start;
  EXPECT_LT(duration, 10ms);

  // Set flag to true
  flag.test_and_set();

  // Wait for it to be different from false
  start = std::chrono::steady_clock::now();
  flag.wait(false);
  duration = std::chrono::steady_clock::now() - start;
  EXPECT_LT(duration, 10ms);
}
