// Copyright 2020 - 2026 The QLever Authors, in particular:
//
// 2020 Johannes Kalmbach <kalmbach@informatik.uni-freiburg.de>, UFR
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_MEMORYLIMITTRACKER_H
#define QLEVER_SRC_UTIL_MEMORYLIMITTRACKER_H

#include <absl/strings/str_cat.h>

#include <functional>
#include <memory>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/Synchronized.h"

// This header contains the *shared* memory-limit bookkeeping used by both
// allocator backends: the classical `allocatorImpl::AllocatorWithLimit` (see
// `AllocatorWithLimitImpl.h`) and the `ql::pmr`-based `LimitedMemoryResource`
// (see `AllocatorPmr.h`). It was factored out of `AllocatorWithLimitImpl.h` so
// that the PMR backend can reuse the tracker without depending on the concrete
// classical allocator implementation.
//
// The types live in their historical namespaces (`ad_utility` and
// `ad_utility::detail`) so that existing users keep working unchanged.

namespace ad_utility {

namespace detail {

// This exception is supposed to be thrown when an allocation is requested that
// exceeds the limit of the allocator.
class AllocationExceedsLimitException : public std::exception {
 public:
  AllocationExceedsLimitException(MemorySize requestedMemory,
                                  MemorySize freeMemory)
      : _message{absl::StrCat("Tried to allocate ", requestedMemory.asString(),
                              ", but only ", freeMemory.asString(),
                              " were available")} {};

  const char* what() const noexcept override { return _message.c_str(); }

 private:
  const std::string _message;
};

// Class to keep track of the amount of memory that is left for allocation. When
// not enough memory is left, an AllocationExceedsLimitException is thrown. Note
// that need a separate class for this because there can be many Allocation
// objects at the same time (hence the wrapper class and the synchronization
// below).
class AllocationMemoryLeft {
  // Remaining free memory.
  MemorySize free_;

 public:
  AllocationMemoryLeft(MemorySize n) : free_(n) {}

  // Called before memory is allocated.
  bool decrease_if_enough_left_or_return_false(MemorySize n) noexcept {
    if (n <= free_) {
      free_ -= n;
      return true;
    } else {
      return false;
    }
  }

  // Called before memory is allocated.
  void decrease_if_enough_left_or_throw(MemorySize n) {
    if (!decrease_if_enough_left_or_return_false(n)) {
      throw AllocationExceedsLimitException{n, free_};
    }
  }

  // Called after memory is deallocated.
  void increase(MemorySize n) { free_ += n; }
  [[nodiscard]] MemorySize amountMemoryLeft() const { return free_; }
};

// Threadsafe Wrapper around `AllocationMemoryLeft`.
// Copies of objects of this class will refer to the same `AllocationMemoryLeft`
// object. Concurrent access is handled via `ad_utility::Synchronized`.
class AllocationMemoryLeftThreadsafe {
 public:
  AllocationMemoryLeftThreadsafe() = delete;
  using T =
      std::shared_ptr<ad_utility::Synchronized<AllocationMemoryLeft, SpinLock>>;
  explicit AllocationMemoryLeftThreadsafe(T ptr) : ptr_{std::move(ptr)} {}
  T& ptr() { return ptr_; }
  const T& ptr() const { return ptr_; }

  friend bool operator==(const AllocationMemoryLeftThreadsafe& a,
                         const AllocationMemoryLeftThreadsafe& b) {
    return a.ptr_ == b.ptr_;
  }

 private:
  T ptr_;
};
}  // namespace detail

// Set up a shared allocation state. For the usage see documentation of the
// Limited Allocator class.
inline detail::AllocationMemoryLeftThreadsafe
makeAllocationMemoryLeftThreadsafeObject(MemorySize n) {
  return detail::AllocationMemoryLeftThreadsafe{std::make_shared<
      ad_utility::Synchronized<detail::AllocationMemoryLeft, SpinLock>>(n)};
}

/*
A lambda for use with `AllocatorWithLimit`.

Called, when there is not enough memory left for an allocation and is supposed
to try to free the given amount of memory.

The lambda is given at construction.
*/
using ClearOnAllocation = std::function<void(MemorySize)>;

/// A Noop lambda that will be used as a template default parameter
/// in the `AllocatorWithLimit` class.
inline ClearOnAllocation noClearOnAllocation = [](MemorySize) {};

namespace detail {

// Shared memory-limit bookkeeping used by both the classical AllocatorWithLimit
// and the PMR LimitedMemoryResource. Holds the shared counter + the
// clearOnAllocation hook and implements the check/clear/throw logic once.
class MemoryLimitTracker {
 private:
  AllocationMemoryLeftThreadsafe memoryLeft_;
  ClearOnAllocation clearOnAllocation_;

 public:
  explicit MemoryLimitTracker(
      AllocationMemoryLeftThreadsafe memoryLeft,
      ClearOnAllocation clearOnAllocation = noClearOnAllocation)
      : memoryLeft_{std::move(memoryLeft)},
        clearOnAllocation_{std::move(clearOnAllocation)} {}

  MemoryLimitTracker(MemorySize limit,
                     ClearOnAllocation clearOnAllocation = noClearOnAllocation)
      : MemoryLimitTracker{makeAllocationMemoryLeftThreadsafeObject(limit),
                           std::move(clearOnAllocation)} {}

  // Reserve `n` bytes or throw AllocationExceedsLimitException, running the
  // clearOnAllocation hook first if the budget is momentarily exceeded.
  void reserveOrThrow(MemorySize n) {
    const bool ok =
        memoryLeft_.ptr()->wlock()->decrease_if_enough_left_or_return_false(n);
    if (!ok) {
      AD_CORRECTNESS_CHECK(clearOnAllocation_);
      clearOnAllocation_(n);
      memoryLeft_.ptr()->wlock()->decrease_if_enough_left_or_throw(n);
    }
  }

  void release(MemorySize n) { memoryLeft_.ptr()->wlock()->increase(n); }

  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return memoryLeft_.ptr()->wlock()->amountMemoryLeft();
  }

  const AllocationMemoryLeftThreadsafe& memoryLeft() const {
    return memoryLeft_;
  }
  const ClearOnAllocation& clearOnAllocation() const {
    return clearOnAllocation_;
  }

  bool operator==(const MemoryLimitTracker& other) const {
    return memoryLeft_ == other.memoryLeft_;
  }
};

}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MEMORYLIMITTRACKER_H
