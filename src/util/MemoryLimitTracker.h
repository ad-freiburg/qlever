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
#include "util/ExceptionHandling.h"
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
  // Constructor from requested and currently free memory.
  AllocationExceedsLimitException(MemorySize requestedMemory,
                                  MemorySize freeMemory)
      : message_{absl::StrCat("Tried to allocate ", requestedMemory.asString(),
                              ", but only ", freeMemory.asString(),
                              " were available")} {};

  // Returns the human-readable error message.
  const char* what() const noexcept override { return message_.c_str(); }

 private:
  // Error message returned by `what()`.
  const std::string message_;
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
  // Constructor from the initial amount of free memory.
  explicit AllocationMemoryLeft(MemorySize n) : free_(n) {}

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

  // Returns the amount of memory still available.
  [[nodiscard]] MemorySize amountMemoryLeft() const { return free_; }
};

// Threadsafe Wrapper around `AllocationMemoryLeft`.
// Copies of objects of this class will refer to the same `AllocationMemoryLeft`
// object. Concurrent access is handled via `ad_utility::Synchronized`.
class AllocationMemoryLeftThreadsafe {
 public:
  // Deleted because the shared counter must always be provided explicitly.
  AllocationMemoryLeftThreadsafe() = delete;

  // Type of the shared, synchronized memory counter.
  using T =
      std::shared_ptr<ad_utility::Synchronized<AllocationMemoryLeft, SpinLock>>;

  // Constructor from an existing shared, synchronized memory counter.
  explicit AllocationMemoryLeftThreadsafe(T ptr) : ptr_{std::move(ptr)} {}

  // Copying a shared_ptr never throws in practice (its refcount increment is
  // an atomic operation that cannot fail). Declare copy ops noexcept explicitly
  // so that downstream static_asserts and noexcept move constructors relying on
  // these operations compile correctly.
  AllocationMemoryLeftThreadsafe(
      const AllocationMemoryLeftThreadsafe&) noexcept = default;
  AllocationMemoryLeftThreadsafe& operator=(
      const AllocationMemoryLeftThreadsafe&) noexcept = default;

  // Returns the shared, synchronized memory counter.
  T& ptr() { return ptr_; }

  // Returns the shared, synchronized memory counter.
  const T& ptr() const { return ptr_; }

  // Compares whether two wrappers refer to the same shared memory counter.
  bool operator==(const AllocationMemoryLeftThreadsafe&) const = default;

 private:
  // Shared synchronized memory counter.
  T ptr_;
};
}  // namespace detail

// Constructs a shared allocation state with `n` bytes initially available.
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
inline const ClearOnAllocation noClearOnAllocation = [](MemorySize) {
  // Intentionally empty: default policy performs no memory reclamation.
};

namespace detail {

// Shared memory-limit bookkeeping used by both the classical AllocatorWithLimit
// and the PMR LimitedMemoryResource. Holds the shared counter + the
// clearOnAllocation hook and implements the check/clear/throw logic once.
class MemoryLimitTracker {
 private:
  // Shared state for the amount of memory still available. This is separate
  // from `clearOnAllocation_`: The counter stores the actual budget, while the
  // hook is only called on a failed reservation attempt to possibly free memory
  // elsewhere before retrying the same counter.
  // Copying a shared_ptr-backed wrapper never throws; assert this so that the
  // noexcept move operations below can copy this member unconditionally.
  AllocationMemoryLeftThreadsafe sharedMemoryLeft_;
  static_assert(std::is_nothrow_copy_constructible_v<AllocationMemoryLeftThreadsafe>);
  static_assert(std::is_nothrow_copy_assignable_v<AllocationMemoryLeftThreadsafe>);

  // Hook that may free memory before a reservation finally fails. `std::function`
  // copy can throw (it may allocate), so we deliberately do NOT assert it noexcept.
  ClearOnAllocation clearOnAllocation_;

 public:
  // Constructor from a shared memory counter and an optional clear hook.
  explicit MemoryLimitTracker(
      AllocationMemoryLeftThreadsafe memoryLeft,
      ClearOnAllocation clearOnAllocation = noClearOnAllocation)
      : sharedMemoryLeft_{std::move(memoryLeft)},
        clearOnAllocation_{std::move(clearOnAllocation)} {}

  // Constructor from an initial memory limit and an optional clear hook.
  explicit MemoryLimitTracker(
      MemorySize limit,
      ClearOnAllocation clearOnAllocation = noClearOnAllocation)
      : MemoryLimitTracker{makeAllocationMemoryLeftThreadsafeObject(limit),
                           std::move(clearOnAllocation)} {}

  // Copy operations: explicitly defaulted so that declaring move operations
  // below does not suppress the implicit copy.
  MemoryLimitTracker(const MemoryLimitTracker&) = default;
  MemoryLimitTracker& operator=(const MemoryLimitTracker&) = default;

  // Move operations deliberately COPY the source rather than steal it, so that
  // the moved-from tracker remains valid and continues to share the same memory
  // budget. This mirrors the semantics required by AllocatorWithLimit (a
  // moved-from allocator must still hold a usable tracker).
  //
  // `sharedMemoryLeft_` copy is noexcept (shared_ptr refcount increment);
  // `clearOnAllocation_` copy (std::function) may throw in theory, so we wrap
  // it in terminateIfThrows — these exceptions should never occur in practice.
  MemoryLimitTracker(MemoryLimitTracker&& other) noexcept
      : sharedMemoryLeft_{other.sharedMemoryLeft_} {
    ad_utility::terminateIfThrows(
        [this, &other]() { clearOnAllocation_ = other.clearOnAllocation_; },
        "The move constructor of `MemoryLimitTracker` copied "
        "`clearOnAllocation_` which threw unexpectedly.");
  }
  MemoryLimitTracker& operator=(MemoryLimitTracker&& other) noexcept {
    sharedMemoryLeft_ = other.sharedMemoryLeft_;
    ad_utility::terminateIfThrows(
        [this, &other]() { clearOnAllocation_ = other.clearOnAllocation_; },
        "The move assignment operator of `MemoryLimitTracker` copied "
        "`clearOnAllocation_` which threw unexpectedly.");
    return *this;
  }

  // Reserve `n` bytes or throw AllocationExceedsLimitException, running the
  // clearOnAllocation hook first if the budget is momentarily exceeded.
  void reserveOrThrow(MemorySize n) {
    const bool ok = sharedMemoryLeft_.ptr()
                        ->wlock()
                        ->decrease_if_enough_left_or_return_false(n);
    if (!ok) {
      AD_CORRECTNESS_CHECK(clearOnAllocation_);
      clearOnAllocation_(n);
      sharedMemoryLeft_.ptr()->wlock()->decrease_if_enough_left_or_throw(n);
    }
  }

  // Releases `n` bytes back to the shared memory counter.
  void release(MemorySize n) { sharedMemoryLeft_.ptr()->wlock()->increase(n); }

  // Returns the amount of memory still available.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return sharedMemoryLeft_.ptr()->wlock()->amountMemoryLeft();
  }

  // Compares whether two trackers share the same memory counter.
  bool operator==(const MemoryLimitTracker& other) const {
    return sharedMemoryLeft_ == other.sharedMemoryLeft_;
  }
};

}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MEMORYLIMITTRACKER_H
