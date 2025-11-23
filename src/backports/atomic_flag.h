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
// This implementation uses `std::mutex`, and
// `std::condition_variable` to provide the same interface as C++20's
// std::atomic_flag including wait(), notify_one(), and notify_all().
// Note: This implementation is less efficient than the C++20 implementation,
// which is implemented using low-level compiler and OS intrinsics, which are
// more efficient than using a mutex.
class atomic_flag {
 private:
  bool flag_{false};
  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;

 public:
  // Default constructor - initializes flag to false
  atomic_flag() = default;
  // Initialize with an explicit value.
  explicit atomic_flag(bool init) : flag_{init} {}

  // Deleted copy and move operations (same as std::atomic_flag)
  atomic_flag(const atomic_flag&) = delete;
  atomic_flag& operator=(const atomic_flag&) = delete;
  atomic_flag(atomic_flag&&) = delete;
  atomic_flag& operator=(atomic_flag&&) = delete;

  // Clear the flag (set to false)
  void clear([[maybe_unused]] std::memory_order order =
                 std::memory_order_seq_cst) noexcept {
    {
      std::lock_guard lock{mutex_};
      flag_ = false;
    }
    // Notify waiters that the flag has changed
    notify_all();
  }

  // Test and set the flag (returns previous value)
  bool test_and_set([[maybe_unused]] std::memory_order order =
                        std::memory_order_seq_cst) noexcept {
    bool result = [this]

    {
      std::lock_guard lock{mutex_};
      return std::exchange(flag_, true);
    }();
    notify_all();
    return result;
  }

  // Test the flag without modifying it (C++20 feature)
  bool test([[maybe_unused]] std::memory_order order =
                std::memory_order_seq_cst) const noexcept {
    std::lock_guard lock{mutex_};
    return flag_;
  }

  // Wait for the flag to become different from the given value
  void wait(bool old, [[maybe_unused]] std::memory_order order =
                          std::memory_order_seq_cst) const noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this, old]() { return flag_ != old; });
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
