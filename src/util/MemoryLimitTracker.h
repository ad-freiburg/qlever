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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "util/Exception.h"
#include "util/MaybeOwningPtr.h"
#include "util/MemorySize/MemorySize.h"
#include "util/TransparentFunctors.h"

// This header contains the memory-limit bookkeeping used by both
// allocator backends: the classical `allocatorImpl::AllocatorWithLimit` (see
// `AllocatorWithLimitImpl.h`) and the `ql::pmr`-based `LimitedMemoryResource`
// (see `AllocatorPmr.h`).

namespace ad_utility {

/*
A lambda for use with `AllocatorWithLimit`.

Called, when there is not enough memory left for an allocation and is supposed
to try to free the given amount of memory.

The lambda is given at construction.
*/
using ClearOnAllocation = std::function<void(MemorySize)>;

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

class AllocationMemoryLeft;
class ThreadBudgetCache;

// The part of a thread's budget cache (see `ThreadBudgetCache` below) that is
// read on the hot path of every small allocation and deallocation. It is
// trivially destructible and constant-initialized, so accessing it requires no
// initialization guard. `slots_[id]` is the number of bytes of the
// `AllocationMemoryLeft` with id `id` that the current thread has reserved from
// that pool's shared counter, but not yet handed out.
struct ThreadBudgetFastState {
  size_t* slots_ = nullptr;
  size_t numSlots_ = 0;
  // Set when the `ThreadBudgetCache` of this thread was destroyed at thread
  // exit. Allocations that happen afterwards (e.g. in the destructors of other
  // `thread_local` objects) then bypass the cache.
  bool isDestroyed_ = false;
};
inline thread_local ThreadBudgetFastState threadBudgetFastState;

// The global bookkeeping that connects the `AllocationMemoryLeft` pools with
// the per-thread caches. All members are protected by `mutex_`, which is only
// locked on the slow paths: when a pool is used for the first time on a thread,
// when a pool is destroyed, and when a thread exits.
class ThreadBudgetRegistry {
 public:
  std::mutex mutex_;
  // The pool for each id, `nullptr` for ids that are currently unused.
  std::vector<AllocationMemoryLeft*> pools_;
  // The ids that are currently unused, to keep the ids (and therefore the
  // per-thread slot arrays) small.
  std::vector<uint32_t> freeIds_;
  // The caches of all threads that currently have one.
  std::vector<ThreadBudgetCache*> threads_;

  // Return the single global registry. It is intentionally never destroyed,
  // because threads may exit, and pools with static storage duration may be
  // destroyed, after the destruction of all function-local static variables.
  static ThreadBudgetRegistry& get() {
    static auto* registry = new ThreadBudgetRegistry{};
    return *registry;
  }

  // Assign an unused id to `pool` and return it. The `mutex_` must be held.
  uint32_t allocateId(AllocationMemoryLeft* pool) {
    if (!freeIds_.empty()) {
      uint32_t id = freeIds_.back();
      freeIds_.pop_back();
      pools_[id] = pool;
      return id;
    }
    AD_CORRECTNESS_CHECK(pools_.size() < std::numeric_limits<uint32_t>::max());
    pools_.push_back(pool);
    return static_cast<uint32_t>(pools_.size() - 1);
  }

  // Mark `id` as unused. The `mutex_` must be held.
  void releaseId(uint32_t id) {
    pools_[id] = nullptr;
    freeIds_.push_back(id);
  }
};

// The cache of a single thread. It owns the slot array that
// `threadBudgetFastState` points to. On thread exit, its destructor returns the
// cached bytes of all pools to their shared counters, s.t. starting and
// stopping threads never silently depletes a memory limit.
class ThreadBudgetCache {
 private:
  std::vector<size_t> slots_;

 public:
  // Register the cache in the registry. The registry's `mutex_` must be held.
  ThreadBudgetCache() { ThreadBudgetRegistry::get().threads_.push_back(this); }

  // Return the cached bytes to their pools and unregister the cache.
  inline ~ThreadBudgetCache();

  ThreadBudgetCache(const ThreadBudgetCache&) = delete;
  ThreadBudgetCache& operator=(const ThreadBudgetCache&) = delete;

  // Return the cache of the calling thread, creating it on the first call. The
  // registry's `mutex_` must be held and the cache must not have been destroyed
  // yet (see `ThreadBudgetFastState::isDestroyed_`).
  static ThreadBudgetCache& getForCurrentThread() {
    static thread_local ThreadBudgetCache cache;
    return cache;
  }

  // Make sure that the cache has a slot for the pool with id `id`. The
  // registry's `mutex_` must be held, because other threads might concurrently
  // clear slots of this cache in `clearSlot`.
  void ensureSlot(uint32_t id) {
    if (id >= slots_.size()) {
      slots_.resize(std::max(size_t{id} + 1, 2 * slots_.size()), 0);
      threadBudgetFastState.slots_ = slots_.data();
      threadBudgetFastState.numSlots_ = slots_.size();
    }
  }

  // Forget the cached bytes of the pool with id `id`, which is being destroyed.
  // The registry's `mutex_` must be held.
  void clearSlot(uint32_t id) {
    if (id < slots_.size()) {
      slots_[id] = 0;
    }
  }
};

// Class to keep track of the amount of memory that is left for allocation. When
// not enough memory is left, an `AllocationExceedsLimitException` is thrown.
// Many allocators share one object of this class via a `std::shared_ptr`.
//
// The budget is stored in the shared atomic counter `free_`. To avoid
// contention on that counter, small allocations (at most
// `maxCachedAllocationSize` bytes) are served from a per-thread cache of this
// pool: when a thread's cache is empty, it reserves `cacheRefillSize` bytes at
// once from `free_`, and when deallocations make it grow beyond
// `maxCachedBytes`, everything beyond `cacheRefillSize` is given back. Each
// pool thus behaves as if it had a `thread_local` data member. Each thread
// therefore holds at most `maxCachedBytes` of the budget that might currently
// be unused. When an allocation fails, the calling thread's cache is first
// returned to `free_`, so small limits are still enforced exactly.
class AllocationMemoryLeft {
 public:
  // Allocations up to this size are served from the per-thread cache.
  static constexpr size_t maxCachedAllocationSize = 1024;
  // The number of bytes that a thread reserves from the shared counter at once.
  static constexpr size_t cacheRefillSize = 32 * 1024;
  // The maximal number of bytes that a thread keeps in its cache.
  static constexpr size_t maxCachedBytes = 2 * cacheRefillSize;

 private:
  static constexpr uint32_t noId = std::numeric_limits<uint32_t>::max();

  // Remaining free memory in bytes, without the bytes in the thread caches.
  std::atomic<size_t> free_;
  // See `ClearOnAllocation`.
  ClearOnAllocation clearOnAllocation_;
  // The index of this pool's slot in the per-thread caches. It is assigned
  // lazily when the pool serves its first small allocation, s.t. pools that
  // never do so never lock the registry.
  std::atomic<uint32_t> id_{noId};

 public:
  // Constructor from the initial amount of free memory and the hook that is
  // called when an allocation exceeds the limit.
  explicit AllocationMemoryLeft(
      MemorySize n, ClearOnAllocation clearOnAllocation = ad_utility::noop)
      : free_(n.getBytes()), clearOnAllocation_{std::move(clearOnAllocation)} {}

  // Forget the bytes that the threads have cached for this pool.
  ~AllocationMemoryLeft() {
    // There are no concurrent users of a pool that is being destroyed.
    uint32_t id = id_.load(std::memory_order_relaxed);
    if (id == noId) {
      return;
    }
    auto& registry = ThreadBudgetRegistry::get();
    std::lock_guard lock{registry.mutex_};
    for (auto* thread : registry.threads_) {
      thread->clearSlot(id);
    }
    registry.releaseId(id);
  }

  // The pool is shared via its address, so it can be neither copied nor moved.
  AllocationMemoryLeft(const AllocationMemoryLeft&) = delete;
  AllocationMemoryLeft& operator=(const AllocationMemoryLeft&) = delete;

  // Reserve `n` bytes, either from the calling thread's cache or from the
  // shared counter. If not enough memory is left, call the `ClearOnAllocation`
  // hook and retry once, then throw an `AllocationExceedsLimitException`.
  void reserveOrThrow(MemorySize memorySize) {
    size_t n = memorySize.getBytes();
    if (n <= maxCachedAllocationSize) {
      size_t* slot = existingSlot();
      if (slot != nullptr && *slot >= n) {
        *slot -= n;
        return;
      }
      if (slot == nullptr) {
        slot = createSlot();
      }
      // Refill the cache s.t. it holds `cacheRefillSize` bytes after serving
      // this request. Note that `*slot < n` at this point.
      if (slot != nullptr &&
          decreaseIfEnoughLeftOrReturnFalse(
              MemorySize::bytes(cacheRefillSize + n - *slot))) {
        *slot = cacheRefillSize;
        return;
      }
    }
    reserveExactlyOrThrow(memorySize);
  }

  // Give `n` bytes back, either to the calling thread's cache or to the shared
  // counter.
  void release(MemorySize memorySize) {
    size_t n = memorySize.getBytes();
    if (n <= maxCachedAllocationSize) {
      if (size_t* slot = existingSlot()) {
        *slot += n;
        if (*slot > maxCachedBytes) {
          increase(MemorySize::bytes(*slot - cacheRefillSize));
          *slot = cacheRefillSize;
        }
        return;
      }
    }
    increase(memorySize);
  }

  // Return the bytes that the calling thread has cached for this pool to the
  // shared counter.
  void flushThreadCache() noexcept {
    if (size_t* slot = existingSlot()) {
      addSaturating(*slot);
      *slot = 0;
    }
  }

  // Decrease the shared counter by `n` if enough memory is left there, else
  // return false. The thread caches are not taken into account.
  bool decreaseIfEnoughLeftOrReturnFalse(MemorySize n) noexcept {
    size_t current = free_.load();
    do {
      if (current < n.getBytes()) {
        return false;
      }
    } while (!free_.compare_exchange_weak(current, current - n.getBytes()));
    return true;
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

  // Return the amount of memory still available. This includes the bytes that
  // the calling thread has cached for this pool, but not those cached by other
  // threads.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    size_t result = free_.load();
    if (const size_t* slot = existingSlot()) {
      size_t cached = *slot;
      result = cached > std::numeric_limits<size_t>::max() - result
                   ? std::numeric_limits<size_t>::max()
                   : result + cached;
    }
    return MemorySize::bytes(result);
  }

 private:
  // Reserve exactly `n` bytes from the shared counter, without refilling the
  // calling thread's cache. On failure, first return the calling thread's cache
  // to the shared counter, then call the `ClearOnAllocation` hook, which might
  // free memory into that cache again, and then throw.
  void reserveExactlyOrThrow(MemorySize n) {
    if (decreaseIfEnoughLeftOrReturnFalse(n)) {
      return;
    }
    flushThreadCache();
    if (decreaseIfEnoughLeftOrReturnFalse(n)) {
      return;
    }
    clearOnAllocation_(n);
    flushThreadCache();
    if (!decreaseIfEnoughLeftOrReturnFalse(n)) {
      throw AllocationExceedsLimitException{n, amountMemoryLeft()};
    }
  }

  // Add `n` bytes to the shared counter, saturating at the maximal value. This
  // is used for returning cached bytes, which were taken from the counter
  // before, so it can only saturate if the limit was increased meanwhile.
  void addSaturating(size_t n) noexcept {
    size_t current = free_.load();
    while (!free_.compare_exchange_weak(
        current, n > std::numeric_limits<size_t>::max() - current
                     ? std::numeric_limits<size_t>::max()
                     : current + n)) {
    }
  }

  // Return the calling thread's slot for this pool, or `nullptr` if the thread
  // has no slot for it yet. This is the hot path, it requires neither a lock
  // nor a read-modify-write operation.
  size_t* existingSlot() const noexcept {
    // The acquire synchronizes with the release in `createSlot`, which
    // happens after a previous pool with the same id cleared its slots.
    uint32_t id = id_.load(std::memory_order_acquire);
    const auto& state = threadBudgetFastState;
    return id < state.numSlots_ ? state.slots_ + id : nullptr;
  }

  // Create the calling thread's slot for this pool (and if necessary this
  // pool's id and the thread's cache) and return it. Return `nullptr` if the
  // thread's cache was already destroyed because the thread is exiting.
  size_t* createSlot() {
    if (threadBudgetFastState.isDestroyed_) {
      return nullptr;
    }
    auto& registry = ThreadBudgetRegistry::get();
    std::lock_guard lock{registry.mutex_};
    uint32_t id = id_.load(std::memory_order_relaxed);
    if (id == noId) {
      id = registry.allocateId(this);
      id_.store(id, std::memory_order_release);
    }
    ThreadBudgetCache::getForCurrentThread().ensureSlot(id);
    return threadBudgetFastState.slots_ + id;
  }

  friend class ThreadBudgetCache;
};

// _____________________________________________________________________________
ThreadBudgetCache::~ThreadBudgetCache() {
  auto& registry = ThreadBudgetRegistry::get();
  std::lock_guard lock{registry.mutex_};
  for (size_t id = 0; id < slots_.size(); ++id) {
    // A slot is nonzero only if its pool is alive, because the destructor of
    // the pool clears it.
    if (slots_[id] != 0) {
      AD_CORRECTNESS_CHECK(registry.pools_[id] != nullptr);
      registry.pools_[id]->addSaturating(slots_[id]);
    }
  }
  auto& threads = registry.threads_;
  threads.erase(std::find(threads.begin(), threads.end(), this));
  threadBudgetFastState = {nullptr, 0, true};
}
}  // namespace detail

// Constructs a shared allocation state with `n` bytes initially available and
// the given `ClearOnAllocation` hook.
inline std::shared_ptr<detail::AllocationMemoryLeft>
makeSharedAllocationMemoryLeft(
    MemorySize n, ClearOnAllocation clearOnAllocation = ad_utility::noop) {
  return std::make_shared<detail::AllocationMemoryLeft>(
      n, std::move(clearOnAllocation));
}

namespace detail {
inline namespace QL_LIFETIME_CHECKS_NAMESPACE {

// Shared memory-limit bookkeeping used by both the classical AllocatorWithLimit
// and the PMR LimitedMemoryResource. It is a thin handle to an
// `AllocationMemoryLeft`, which holds the budget, the per-thread caches and
// the clearOnAllocation hook. The handle either shares the ownership of the
// `AllocationMemoryLeft`, or it is a non-owning copy (see `nonOwningCopy()`),
// copying which costs no atomic operation. This matters because a tracker is
// copied on every copy of an allocator (e.g. when copying a string).
class MemoryLimitTracker {
 private:
  MaybeOwningPtr<AllocationMemoryLeft> memoryLeft_;

 public:
  // Constructor from a shared memory counter. The tracker shares its
  // ownership.
  explicit MemoryLimitTracker(std::shared_ptr<AllocationMemoryLeft> memoryLeft)
      : memoryLeft_{std::move(memoryLeft)} {}

  // Constructor from an initial memory limit and an optional clear hook.
  explicit MemoryLimitTracker(
      MemorySize limit, ClearOnAllocation clearOnAllocation = ad_utility::noop)
      : MemoryLimitTracker{makeSharedAllocationMemoryLeft(
            limit, std::move(clearOnAllocation))} {}

  // Copying is cheap and shares the underlying budget and hook. The move
  // operations of `MaybeOwningPtr` deliberately COPY the source rather than
  // steal it, so that a moved-from tracker remains valid and continues to
  // share the same memory budget and hook. This mirrors the semantics
  // required by `AllocatorWithLimit` (a moved-from allocator must still hold a
  // usable tracker).
  MemoryLimitTracker(const MemoryLimitTracker&) = default;
  MemoryLimitTracker& operator=(const MemoryLimitTracker&) = default;
  MemoryLimitTracker(MemoryLimitTracker&&) noexcept = default;
  MemoryLimitTracker& operator=(MemoryLimitTracker&&) noexcept = default;
  ~MemoryLimitTracker() = default;

  // Return a tracker for the same budget that does not keep the
  // `AllocationMemoryLeft` alive. This tracker (or another owning copy of it)
  // has to outlive the result and all its copies.
  MemoryLimitTracker nonOwningCopy() const {
    return MemoryLimitTracker{memoryLeft_.nonOwningCopy()};
  }

  // Return true iff this tracker keeps its `AllocationMemoryLeft` alive.
  bool isOwning() const { return memoryLeft_.isOwning(); }

  // Reserve `n` bytes or throw AllocationExceedsLimitException, running the
  // clearOnAllocation hook first if the budget is momentarily exceeded.
  void reserveOrThrow(MemorySize n) { memoryLeft_->reserveOrThrow(n); }

  // Releases `n` bytes back to the shared memory counter.
  void release(MemorySize n) { memoryLeft_->release(n); }

  // Returns the amount of memory still available.
  [[nodiscard]] MemorySize amountMemoryLeft() const {
    return memoryLeft_->amountMemoryLeft();
  }

  // Increase the memory limit by `n`.
  void increaseMemoryLimit(MemorySize n) { memoryLeft_->increase(n); }

  // Decrease the memory limit by `n`. This requires `n` to be currently free,
  // otherwise an exception is thrown and nothing is changed.
  void decreaseMemoryLimit(MemorySize n) {
    // Bytes cached by the calling thread count as free.
    memoryLeft_->flushThreadCache();
    if (!memoryLeft_->decreaseIfEnoughLeftOrReturnFalse(n)) {
      throw std::runtime_error{absl::StrCat(
          "Cannot reduce the memory limit by ", n.asString(), ", because only ",
          amountMemoryLeft().asString(), " are currently unused")};
    }
  }

  // Compares whether two trackers share the same memory counter.
  bool operator==(const MemoryLimitTracker& other) const {
    return memoryLeft_.get() == other.memoryLeft_.get();
  }

 private:
  explicit MemoryLimitTracker(MaybeOwningPtr<AllocationMemoryLeft> memoryLeft)
      : memoryLeft_{std::move(memoryLeft)} {}
};

}  // namespace QL_LIFETIME_CHECKS_NAMESPACE
}  // namespace detail

}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_MEMORYLIMITTRACKER_H
