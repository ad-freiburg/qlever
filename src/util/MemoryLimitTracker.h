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
#include <limits>
#include <memory>
#include <stdexcept>

#include "util/Exception.h"
#include "util/MaybeOwningPtr.h"
#include "util/MemorySize/MemorySize.h"
#include "util/ThreadLocalMember.h"
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

  // Same as `increase`, but saturate at the maximal value instead of throwing.
  void increaseSaturating(MemorySize n) noexcept {
    size_t current = free_.load();
    while (!free_.compare_exchange_weak(current,
                                        saturatingAdd(current, n.getBytes()))) {
    }
  }

  // Returns the amount of memory still available.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return MemorySize::bytes(free_.load());
  }

  // Return `a + b`, or the maximal value if that sum overflows.
  //
  // NOTE: Neither Abseil nor the Boost libraries that we use provide a
  // saturating addition of integers, and the standard library only has one
  // since C++26 (`std::add_sat`), so we implement it ourselves.
  static size_t saturatingAdd(size_t a, size_t b) noexcept {
    return b > std::numeric_limits<size_t>::max() - a
               ? std::numeric_limits<size_t>::max()
               : a + b;
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

// The state that is shared by all copies of an allocator: the counter of the
// free memory, the `ClearOnAllocation` hook, and a per-thread cache of free
// memory.
//
// To avoid contention on the shared counter, small allocations (at most
// `maxCachedAllocationSize` bytes) are served from the cache of the calling
// thread: when that cache is empty, it reserves `cacheRefillSize` bytes at once
// from the counter, and when deallocations make it grow beyond
// `maxCachedBytes`, everything beyond `cacheRefillSize` is given back. Each
// thread therefore holds at most `maxCachedBytes` of the budget that might
// currently be unused. When an allocation fails, the calling thread's cache is
// first returned to the counter, so small limits are still enforced exactly.
// When a thread exits, its cache is returned to the counter, so that starting
// and stopping threads never silently depletes a memory limit.
class SharedMemoryLimit {
 public:
  // Allocations up to this size are served from the per-thread cache.
  static constexpr MemorySize maxCachedAllocationSize =
      MemorySize::kilobytes(1);
  // The amount of memory that a thread reserves from the shared counter at
  // once.
  static constexpr MemorySize cacheRefillSize = MemorySize::kilobytes(32);
  // The maximal amount of memory that a thread keeps in its cache.
  static constexpr MemorySize maxCachedBytes = 2 * cacheRefillSize;

 private:
  std::shared_ptr<AllocationMemoryLeft> memoryLeft_;
  // Called on a failed reservation attempt to possibly free memory elsewhere
  // before retrying.
  ClearOnAllocation clearOnAllocation_;
  // The bytes that a thread has reserved from `memoryLeft_`, but not yet
  // handed out. When a thread exits, `ThreadLocalMember` calls the lambda with
  // the bytes that this thread still has cached, which gives them back to
  // `memoryLeft_`. For details see `ThreadLocalMember`.
  //
  // NOTE: Giving the bytes back saturates instead of throwing on overflow. This
  // makes it more robust, because `increaseMemoryLimit` only checks the shared
  // counter, but not the bytes that the threads have cached. Increasing the
  // limit of an already (almost) unlimited resource while some threads have
  // cached bytes thus has to work, and so has giving those bytes back later.
  ThreadLocalMember<size_t> threadCache_{[this](size_t& cached) {
    memoryLeft_->increaseSaturating(MemorySize::bytes(cached));
    cached = 0;
  }};

 public:
  // Constructor from a shared memory counter and the hook.
  SharedMemoryLimit(std::shared_ptr<AllocationMemoryLeft> memoryLeft,
                    ClearOnAllocation clearOnAllocation)
      : memoryLeft_{std::move(memoryLeft)},
        clearOnAllocation_{std::move(clearOnAllocation)} {
    AD_CONTRACT_CHECK(memoryLeft_);
  }

  // Reserve `n` bytes, either from the calling thread's cache or from the
  // shared counter. If not enough memory is left, call the `ClearOnAllocation`
  // hook and retry once, then throw an `AllocationExceedsLimitException`.
  void reserveOrThrow(MemorySize memorySize) {
    size_t n = memorySize.getBytes();
    // There is no cache for large allocations, and also not if the calling
    // thread is already exiting (see `ThreadLocalMember::getOrCreate`).
    size_t* cached = memorySize <= maxCachedAllocationSize
                         ? threadCache_.getOrCreate()
                         : nullptr;
    if (cached == nullptr) {
      reserveExactlyOrThrow(memorySize);
      return;
    }
    if (*cached >= n) {
      *cached -= n;
      return;
    }
    // Refill the cache s.t. it holds `cacheRefillSize` bytes after serving
    // this request. Note that `*cached < n` at this point.
    if (memoryLeft_->decreaseIfEnoughLeftOrReturnFalse(
            cacheRefillSize + MemorySize::bytes(n - *cached))) {
      *cached = cacheRefillSize.getBytes();
      return;
    }
    reserveExactlyOrThrow(memorySize);
  }

  // Give `n` bytes back, either to the calling thread's cache or to the shared
  // counter.
  void release(MemorySize memorySize) {
    size_t* cached = memorySize <= maxCachedAllocationSize
                         ? threadCache_.getIfExists()
                         : nullptr;
    if (cached == nullptr) {
      memoryLeft_->increase(memorySize);
      return;
    }
    *cached += memorySize.getBytes();
    if (*cached > maxCachedBytes.getBytes()) {
      memoryLeft_->increase(MemorySize::bytes(*cached) - cacheRefillSize);
      *cached = cacheRefillSize.getBytes();
    }
  }

  // Return the bytes that the calling thread has cached to the shared counter.
  void flushThreadCache() noexcept {
    if (size_t* cached = threadCache_.getIfExists()) {
      memoryLeft_->increaseSaturating(MemorySize::bytes(*cached));
      *cached = 0;
    }
  }

  // Return the amount of memory still available. This includes the bytes that
  // the calling thread has cached, but not those cached by other threads.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    size_t result = memoryLeft_->amountMemoryLeft().getBytes();
    if (const size_t* cached = threadCache_.getIfExists()) {
      result = AllocationMemoryLeft::saturatingAdd(result, *cached);
    }
    return MemorySize::bytes(result);
  }

  // Return the shared counter.
  AllocationMemoryLeft& memoryLeft() const { return *memoryLeft_; }

 private:
  // Reserve exactly `n` bytes from the shared counter, without refilling the
  // calling thread's cache. On failure, first return the calling thread's cache
  // to the shared counter, then call the `ClearOnAllocation` hook, which might
  // free memory into that cache again, and then throw.
  void reserveExactlyOrThrow(MemorySize n) {
    if (memoryLeft_->decreaseIfEnoughLeftOrReturnFalse(n)) {
      return;
    }
    flushThreadCache();
    if (memoryLeft_->decreaseIfEnoughLeftOrReturnFalse(n)) {
      return;
    }
    clearOnAllocation_(n);
    flushThreadCache();
    memoryLeft_->decreaseIfEnoughLeftOrThrow(n);
  }
};

inline namespace QL_LIFETIME_CHECKS_NAMESPACE {

// Shared memory-limit bookkeeping used by both the classical AllocatorWithLimit
// and the PMR LimitedMemoryResource. It is a thin handle to a
// `SharedMemoryLimit`, which holds the shared counter, the clearOnAllocation
// hook and the per-thread caches. The handle either shares the ownership of the
// `SharedMemoryLimit`, or it is a non-owning copy (see `nonOwningCopy()`),
// copying which costs no atomic operation. This matters because a tracker is
// copied on every copy of an allocator (e.g. when copying a string).
class MemoryLimitTracker {
 private:
  MaybeOwningPtr<SharedMemoryLimit> sharedLimit_;

 public:
  // Constructor from a shared memory counter and an optional clear hook.
  explicit MemoryLimitTracker(
      std::shared_ptr<AllocationMemoryLeft> memoryLeft,
      ClearOnAllocation clearOnAllocation = ad_utility::noop)
      : sharedLimit_{std::make_shared<SharedMemoryLimit>(
            std::move(memoryLeft), std::move(clearOnAllocation))} {}

  // Constructor from an initial memory limit and an optional clear hook.
  explicit MemoryLimitTracker(
      MemorySize limit, ClearOnAllocation clearOnAllocation = ad_utility::noop)
      : MemoryLimitTracker{makeSharedAllocationMemoryLeft(limit),
                           std::move(clearOnAllocation)} {}

  // Copying is cheap and shares the underlying budget and hook. As
  // `MaybeOwningPtr` has no move operations, moving falls back to copying
  // rather than stealing, so that a moved-from tracker remains valid and
  // continues to share the same memory budget and hook. This mirrors the
  // semantics required by `AllocatorWithLimit` (a moved-from allocator must
  // still hold a usable tracker).
  MemoryLimitTracker(const MemoryLimitTracker&) = default;
  MemoryLimitTracker& operator=(const MemoryLimitTracker&) = default;
  MemoryLimitTracker(MemoryLimitTracker&&) noexcept = default;
  MemoryLimitTracker& operator=(MemoryLimitTracker&&) noexcept = default;
  ~MemoryLimitTracker() = default;

  // Return a tracker for the same budget that does not keep the
  // `SharedMemoryLimit` alive. This tracker (or another owning copy of it)
  // has to outlive the result and all its copies.
  MemoryLimitTracker nonOwningCopy() const {
    return MemoryLimitTracker{sharedLimit_.nonOwningCopy()};
  }

  // Return true iff this tracker keeps its `SharedMemoryLimit` alive.
  bool isOwning() const { return sharedLimit_.isOwning(); }

  // Reserve `n` bytes or throw AllocationExceedsLimitException, running the
  // clearOnAllocation hook first if the budget is momentarily exceeded.
  void reserveOrThrow(MemorySize n) { sharedLimit_->reserveOrThrow(n); }

  // Releases `n` bytes back to the shared memory counter.
  void release(MemorySize n) { sharedLimit_->release(n); }

  // Returns the amount of memory still available.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return sharedLimit_->amountMemoryLeft();
  }

  // Increase the memory limit by `n`.
  void increaseMemoryLimit(MemorySize n) {
    sharedLimit_->memoryLeft().increase(n);
  }

  // Decrease the memory limit by `n`. This requires `n` to be currently free,
  // otherwise an exception is thrown and nothing is changed.
  void decreaseMemoryLimit(MemorySize n) {
    // Bytes cached by the calling thread count as free.
    sharedLimit_->flushThreadCache();
    if (!sharedLimit_->memoryLeft().decreaseIfEnoughLeftOrReturnFalse(n)) {
      throw std::runtime_error{absl::StrCat(
          "Cannot reduce the memory limit by ", n.asString(), ", because only ",
          amountMemoryLeft().asString(), " are currently unused")};
    }
  }

  // Compares whether two trackers share the same memory counter.
  bool operator==(const MemoryLimitTracker& other) const {
    return &sharedLimit_->memoryLeft() == &other.sharedLimit_->memoryLeft();
  }

 private:
  explicit MemoryLimitTracker(MaybeOwningPtr<SharedMemoryLimit> sharedLimit)
      : sharedLimit_{std::move(sharedLimit)} {}
};

}  // namespace QL_LIFETIME_CHECKS_NAMESPACE
}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MEMORYLIMITTRACKER_H
