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

#include <atomic>
#include <functional>
#include <memory>
#include <stdexcept>

#include "util/Exception.h"
#include "util/MemorySize/MemorySize.h"
#include "util/TransparentFunctors.h"

// This header contains the memory-limit bookkeeping used by both
// allocator backends: the classical `allocatorImpl::AllocatorWithLimit` (see
// `AllocatorWithLimitImpl.h`) and the `ql::pmr`-based `LimitedMemoryResource`
// (see `AllocatorPmr.h`).

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
                              " were available")} {}

  // Returns the human-readable error message.
  const char* what() const noexcept override { return message_.c_str(); }

 private:
  // Error message returned by `what()`.
  const std::string message_;
};

// Class to keep track of the amount of memory that is left for allocation. When
// not enough memory is left, an `AllocationExceedsLimitException` is thrown.
// Many allocators share one object of this class via a `std::shared_ptr`, so
// all operations are lock-free atomic operations on the single counter
// `free_`. The memory limit itself is not stored, it can only be changed
// relative to its current value.
class AllocationMemoryLeft {
  // Remaining free memory in bytes.
  std::atomic<size_t> free_;

 public:
  // Constructor from the initial amount of free memory.
  explicit AllocationMemoryLeft(MemorySize n) : free_(n.getBytes()) {}

  // Called before memory is allocated.
  bool decreaseIfEnoughLeftOrReturnFalse(MemorySize n) noexcept {
    size_t current = free_.load();
    do {
      if (current < n.getBytes()) {
        return false;
      }
    } while (!free_.compare_exchange_weak(current, current - n.getBytes()));
    return true;
  }

  // Called before memory is allocated.
  void decreaseIfEnoughLeftOrThrow(MemorySize n) {
    if (!decreaseIfEnoughLeftOrReturnFalse(n)) {
      throw AllocationExceedsLimitException{n, amountMemoryLeft()};
    }
  }

  // Called after memory is deallocated, or when the memory limit is increased.
  // Throws `std::overflow_error` if the free memory would overflow, in which
  // case nothing is changed.
  void increase(MemorySize n) {
    size_t current = free_.load();
    while (!free_.compare_exchange_weak(
        current, (MemorySize::bytes(current) + n).getBytes())) {
    }
  }

  // Returns the amount of memory still available.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return MemorySize::bytes(free_.load());
  }
};
}  // namespace detail

// Constructs a shared allocation state with `n` bytes initially available.
inline std::shared_ptr<detail::AllocationMemoryLeft>
makeSharedAllocationMemoryLeft(MemorySize n) {
  return std::make_shared<detail::AllocationMemoryLeft>(n);
}

/*
A lambda for use with `AllocatorWithLimit`.

Called, when there is not enough memory left for an allocation and is supposed
to try to free the given amount of memory.

The lambda is given at construction.
*/
using ClearOnAllocation = std::function<void(MemorySize)>;

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
  // Copying a shared_ptr never throws; assert this so that the noexcept move
  // operations below can copy this member unconditionally.
  std::shared_ptr<AllocationMemoryLeft> sharedMemoryLeft_;
  static_assert(std::is_nothrow_copy_constructible_v<
                std::shared_ptr<AllocationMemoryLeft>>);
  static_assert(
      std::is_nothrow_copy_assignable_v<std::shared_ptr<AllocationMemoryLeft>>);

  // The clear-on-allocation hook, held behind a `shared_ptr` following the SAME
  // principle as `sharedMemoryLeft_`: the hook is shared (never deep-copied),
  // so copying/moving a tracker only copies a shared_ptr. This is nothrow,
  // which is why the move operations need no `terminateIfThrows` guard. The
  // pointee is `const` because the hook is fixed at construction.
  std::shared_ptr<const ClearOnAllocation> clearOnAllocation_;
  static_assert(std::is_nothrow_copy_constructible_v<
                std::shared_ptr<const ClearOnAllocation>>);
  static_assert(std::is_nothrow_copy_assignable_v<
                std::shared_ptr<const ClearOnAllocation>>);

 public:
  // Constructor from a shared memory counter and an optional clear hook.
  explicit MemoryLimitTracker(
      std::shared_ptr<AllocationMemoryLeft> memoryLeft,
      ClearOnAllocation clearOnAllocation = ad_utility::noop)
      : sharedMemoryLeft_{std::move(memoryLeft)},
        clearOnAllocation_{std::make_shared<const ClearOnAllocation>(
            std::move(clearOnAllocation))} {
    AD_CONTRACT_CHECK(sharedMemoryLeft_);
  }

  // Constructor from an initial memory limit and an optional clear hook.
  explicit MemoryLimitTracker(
      MemorySize limit, ClearOnAllocation clearOnAllocation = ad_utility::noop)
      : MemoryLimitTracker{makeSharedAllocationMemoryLeft(limit),
                           std::move(clearOnAllocation)} {}

  // Both members are shared_ptr-backed, so copying is nothrow and shares the
  // underlying budget and hook.
  MemoryLimitTracker(const MemoryLimitTracker&) = default;
  MemoryLimitTracker& operator=(const MemoryLimitTracker&) = default;

  // Move operations deliberately COPY the source rather than steal it, so that
  // the moved-from tracker remains valid and continues to share the same memory
  // budget and hook. This mirrors the semantics required by
  // `AllocatorWithLimit` (a moved-from allocator must still hold a usable
  // tracker).
  //
  // These are declared explicitly (rather than left undeclared to fall back
  // on the copy constructor) together with the destructor below so that all
  // five special member functions are declared consistently.
  MemoryLimitTracker(MemoryLimitTracker&& other) noexcept
      : MemoryLimitTracker(other) {}

  MemoryLimitTracker& operator=(MemoryLimitTracker&& other) noexcept {
    *this = other;
    return *this;
  }

  ~MemoryLimitTracker() = default;

  // Reserve `n` bytes or throw AllocationExceedsLimitException, running the
  // clearOnAllocation hook first if the budget is momentarily exceeded.
  void reserveOrThrow(MemorySize n) {
    if (!sharedMemoryLeft_->decreaseIfEnoughLeftOrReturnFalse(n)) {
      AD_CORRECTNESS_CHECK(clearOnAllocation_);
      (*clearOnAllocation_)(n);
      sharedMemoryLeft_->decreaseIfEnoughLeftOrThrow(n);
    }
  }

  // Releases `n` bytes back to the shared memory counter.
  void release(MemorySize n) { sharedMemoryLeft_->increase(n); }

  // Returns the amount of memory still available.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return sharedMemoryLeft_->amountMemoryLeft();
  }

  // Increase the memory limit by `n`.
  void increaseMemoryLimit(MemorySize n) { sharedMemoryLeft_->increase(n); }

  // Decrease the memory limit by `n`. This requires `n` to be currently free,
  // otherwise an exception is thrown and nothing is changed.
  void decreaseMemoryLimit(MemorySize n) {
    if (!sharedMemoryLeft_->decreaseIfEnoughLeftOrReturnFalse(n)) {
      throw std::runtime_error{absl::StrCat(
          "Cannot reduce the memory limit by ", n.asString(), ", because only ",
          amountMemoryLeft().asString(), " are currently unused")};
    }
  }

  // Compares whether two trackers share the same memory counter.
  bool operator==(const MemoryLimitTracker& other) const {
    return sharedMemoryLeft_ == other.sharedMemoryLeft_;
  }
};

}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MEMORYLIMITTRACKER_H
