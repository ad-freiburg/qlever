// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_BACKPORTS_ATOMIC_FLAG_H
#define QLEVER_SRC_BACKPORTS_ATOMIC_FLAG_H

#include <atomic>
#include <condition_variable>
#include <mutex>

#include "backports/cppTemplate2.h"

namespace ql::backports {

// C++17 backport of C++20's `std::atomic_flag` with wait/notify functionality.
// This implementation uses std::atomic<bool>, std::mutex, and
// std::condition_variable to provide the same interface as C++20's
// std::atomic_flag including wait(), notify_one(), and notify_all().
// Note: Same as `std::atomic_flag`, this implementation is prone to the ABA
// problem, i.e. if the value of the flag is changed, and then immediately
// changed back, threads waiting for that change might miss it. This allows to
// implement many operations without acquiring a lock.
class atomic_flag {
 private:
  std::atomic<bool> flag_{false};
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;

 public:
  // Default constructor - initializes flag to false
  atomic_flag() = default;

  // Deleted copy and move operations (same as std::atomic_flag)
  atomic_flag(const atomic_flag&) = delete;
  atomic_flag& operator=(const atomic_flag&) = delete;
  atomic_flag(atomic_flag&&) = delete;
  atomic_flag& operator=(atomic_flag&&) = delete;

  // Clear the flag (set to false)
  void clear(std::memory_order order = std::memory_order_seq_cst) noexcept {
    flag_.store(false, order);
    // Notify waiters that the flag has changed
    notify_all();
  }

  // Test and set the flag (returns previous value)
  bool test_and_set(
      std::memory_order order = std::memory_order_seq_cst) noexcept {
    bool result = flag_.exchange(true, order);
    // Notify waiters that the flag has changed
    notify_all();
    return result;
  }

  // Test the flag without modifying it (C++20 feature)
  bool test(
      std::memory_order order = std::memory_order_seq_cst) const noexcept {
    return flag_.load(order);
  }

  // Wait for the flag to become different from the given value
  void wait(bool old, std::memory_order order =
                          std::memory_order_seq_cst) const noexcept {
    // Fast path: check without locking first
    if (flag_.load(order) != old) {
      return;
    }
    // Slow path: lock and wait
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this, old, order]() { return flag_.load(order) != old; });
  }

  // Notify one waiting thread
  void notify_one() noexcept { cv_.notify_one(); }

  // Notify all waiting threads
  void notify_all() noexcept { cv_.notify_all(); }
};

}  // namespace ql::backports

namespace ql {
#ifndef QLEVER_CPP_17
// In C++20 mode, use the standard atomic_flag
using atomic_flag = std::atomic_flag;
#else
// In C++17 mode, provide a backport implementation
using ql::backports::atomic_flag;
#endif  // QLEVER_CPP_17
}  // namespace ql

#endif  // QLEVER_SRC_BACKPORTS_ATOMIC_FLAG_H
