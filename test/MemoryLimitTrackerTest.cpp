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
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "util/AllocatorWithLimitImpl.h"
#include "util/MemoryLimitTracker.h"

// Tests for the per-thread caches of `SharedMemoryLimit`. The accounting
// that is visible from a single thread is also tested for both allocator
// backends in `AllocatorBackendTest.cpp`.

namespace {

using namespace ad_utility::memory_literals;
using ad_utility::MemorySize;
using ad_utility::detail::AllocationExceedsLimitException;
using ad_utility::detail::SharedMemoryLimit;
using Alloc = ad_utility::allocatorImpl::AllocatorWithLimit<char>;

constexpr size_t maxSmall =
    SharedMemoryLimit::maxCachedAllocationSize.getBytes();
constexpr size_t refill = SharedMemoryLimit::cacheRefillSize.getBytes();
constexpr size_t maxCached = SharedMemoryLimit::maxCachedBytes.getBytes();

// Return `alloc.amountMemoryLeft()` as seen from a fresh thread, which has no
// cache for the pool of `alloc`. This is the value of the shared counter.
MemorySize amountLeftOnFreshThread(const Alloc& alloc) {
  MemorySize result;
  std::thread{[&]() { result = alloc.amountMemoryLeft(); }}.join();
  return result;
}

// Run `f` on a new thread and wait for it to finish.
template <typename F>
void runOnThread(F f) {
  std::thread{std::move(f)}.join();
}

// A small allocation reserves a whole chunk from the shared counter, which then
// serves the following small allocations of the same thread. The calling thread
// sees the exact amount, other threads see the chunk as used.
TEST(MemoryLimitTracker, smallAllocationsUseThreadCache) {
  const MemorySize limit = 1_MB;
  auto alloc = Alloc::makeLimited(limit);
  runOnThread([&]() {
    char* p = alloc.allocate(100);
    EXPECT_EQ(alloc.amountMemoryLeft(), limit - 100_B);
    EXPECT_EQ(amountLeftOnFreshThread(alloc),
              limit - MemorySize::bytes(refill + 100));

    // The next small allocation is served from the cache.
    char* p2 = alloc.allocate(maxSmall);
    EXPECT_EQ(alloc.amountMemoryLeft(),
              limit - MemorySize::bytes(100 + maxSmall));
    EXPECT_EQ(amountLeftOnFreshThread(alloc),
              limit - MemorySize::bytes(refill + 100));

    alloc.deallocate(p2, maxSmall);
    alloc.deallocate(p, 100);
    EXPECT_EQ(alloc.amountMemoryLeft(), limit);
    // All bytes were given back to the cache of this thread, which now holds
    // the refill and the bytes of the first allocation.
    EXPECT_EQ(amountLeftOnFreshThread(alloc),
              limit - MemorySize::bytes(refill + 100));
  });
  // The cache was returned when the thread exited.
  EXPECT_EQ(alloc.amountMemoryLeft(), limit);
}

// Allocations that are larger than `maxCachedAllocationSize` bypass the cache.
TEST(MemoryLimitTracker, largeAllocationsBypassCache) {
  const MemorySize limit = 1_MB;
  auto alloc = Alloc::makeLimited(limit);
  runOnThread([&]() {
    char* p = alloc.allocate(maxSmall + 1);
    EXPECT_EQ(amountLeftOnFreshThread(alloc),
              limit - MemorySize::bytes(maxSmall + 1));
    alloc.deallocate(p, maxSmall + 1);
    EXPECT_EQ(amountLeftOnFreshThread(alloc), limit);
  });
}

// If the shared counter cannot refill the cache, exactly the requested bytes
// are reserved, so that small limits are enforced exactly.
TEST(MemoryLimitTracker, smallLimitIsEnforcedExactly) {
  auto alloc = Alloc::makeLimited(100_B);
  char* p = alloc.allocate(60);
  char* p2 = alloc.allocate(40);
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);
  EXPECT_THROW(alloc.allocate(1), AllocationExceedsLimitException);
  alloc.deallocate(p, 60);
  // The 60 freed bytes are in the cache of this thread, which serves the next
  // allocation.
  EXPECT_EQ(amountLeftOnFreshThread(alloc), 0_B);
  EXPECT_EQ(alloc.amountMemoryLeft(), 60_B);
  char* p3 = alloc.allocate(60);
  alloc.deallocate(p3, 60);
  alloc.deallocate(p2, 40);
  EXPECT_EQ(alloc.amountMemoryLeft(), 100_B);
}

// When a cache grows beyond `maxCachedBytes`, it gives everything beyond
// `cacheRefillSize` back to the shared counter.
TEST(MemoryLimitTracker, cacheIsDrained) {
  const MemorySize limit = 10_MB;
  auto alloc = Alloc::makeLimited(limit);
  runOnThread([&]() {
    // Allocate many small blocks, which repeatedly refill the cache.
    constexpr size_t numBlocks = 4 * maxCached / maxSmall;
    std::vector<char*> blocks;
    for (size_t i = 0; i < numBlocks; ++i) {
      blocks.push_back(alloc.allocate(maxSmall));
    }
    const size_t used = numBlocks * maxSmall;
    EXPECT_EQ(alloc.amountMemoryLeft(), limit - MemorySize::bytes(used));
    auto shared = amountLeftOnFreshThread(alloc);
    EXPECT_LE(shared, limit - MemorySize::bytes(used));
    EXPECT_GE(shared, limit - MemorySize::bytes(used + refill));

    for (char* block : blocks) {
      alloc.deallocate(block, maxSmall);
      // The cache never holds more than `maxCachedBytes`.
      EXPECT_GE(amountLeftOnFreshThread(alloc) + MemorySize::bytes(maxCached),
                alloc.amountMemoryLeft());
    }
    EXPECT_EQ(alloc.amountMemoryLeft(), limit);
  });
  EXPECT_EQ(alloc.amountMemoryLeft(), limit);
}

// Starting and stopping many threads that use the cache doesn't deplete the
// budget, also when memory is allocated on one thread and freed on another.
TEST(MemoryLimitTracker, threadExitReturnsCache) {
  const MemorySize limit = 1_MB;
  auto alloc = Alloc::makeLimited(limit);
  std::vector<char*> kept;
  for (size_t i = 0; i < 100; ++i) {
    runOnThread([&]() {
      char* p = alloc.allocate(10);
      alloc.deallocate(p, 10);
      kept.push_back(alloc.allocate(20));
    });
  }
  EXPECT_EQ(alloc.amountMemoryLeft(), limit - 2000_B);
  // Free the memory on yet another thread.
  runOnThread([&]() {
    for (char* p : kept) {
      alloc.deallocate(p, 20);
    }
  });
  EXPECT_EQ(alloc.amountMemoryLeft(), limit);
}

// Destroying a pool forgets the bytes that other (still running) threads have
// cached for it, so that a new pool, which might reuse its slot, starts
// with empty caches.
TEST(MemoryLimitTracker, poolDestructionClearsCachesOfOtherThreads) {
  std::atomic<int> step = 0;
  std::shared_ptr<Alloc> alloc =
      std::make_shared<Alloc>(Alloc::makeLimited(1_MB));
  auto waitFor = [&step](int s) {
    while (step.load() != s) {
      std::this_thread::yield();
    }
  };
  std::thread worker{[&]() {
    char* p = alloc->allocate(10);
    alloc->deallocate(p, 10);
    step = 1;
    waitFor(2);
    // The old pool is gone, use the new one.
    p = alloc->allocate(10);
    alloc->deallocate(p, 10);
    step = 3;
  }};
  waitFor(1);
  // Destroy the pool while the worker still has bytes cached for it, and
  // create a new pool.
  alloc = std::make_shared<Alloc>(Alloc::makeLimited(2_MB));
  step = 2;
  waitFor(3);
  worker.join();
  EXPECT_EQ(alloc->amountMemoryLeft(), 2_MB);
}

// The `ClearOnAllocation` hook might free memory into the cache of the
// calling thread, which is then available to the retried allocation.
TEST(MemoryLimitTracker, hookFreesIntoThreadCache) {
  std::function<void()> freeSomething;
  bool hookCalled = false;
  auto alloc = Alloc::makeLimited(2000_B, [&](MemorySize) {
    hookCalled = true;
    freeSomething();
  });
  char* p1 = alloc.allocate(1000);
  char* p2 = alloc.allocate(1000);
  freeSomething = [&]() { alloc.deallocate(p1, 1000); };
  char* p3 = alloc.allocate(500);
  EXPECT_TRUE(hookCalled);
  EXPECT_EQ(alloc.amountMemoryLeft(), 500_B);
  alloc.deallocate(p3, 500);
  alloc.deallocate(p2, 1000);
  EXPECT_EQ(alloc.amountMemoryLeft(), 2000_B);
}

// Bytes in the cache of the calling thread count as unused when decreasing the
// memory limit.
TEST(MemoryLimitTracker, decreaseMemoryLimitUsesThreadCache) {
  const MemorySize limit = 1_MB;
  auto alloc = Alloc::makeLimited(limit);
  char* p = alloc.allocate(10);
  alloc.deallocate(p, 10);
  alloc.decreaseMemoryLimit(limit);
  EXPECT_EQ(alloc.amountMemoryLeft(), 0_B);
  EXPECT_THROW(alloc.allocate(1), AllocationExceedsLimitException);
}

// An object that allocates and frees memory in its destructor.
struct AllocatesInDestructor {
  Alloc* alloc_ = nullptr;
  ~AllocatesInDestructor() {
    if (alloc_ != nullptr) {
      char* p = alloc_->allocate(10);
      alloc_->deallocate(p, 10);
      alloc_->deallocate(alloc_->allocate(20), 20);
    }
  }
};

// Allocations in the destructors of `thread_local` objects that are destroyed
// after the cache of the thread bypass the cache.
TEST(MemoryLimitTracker, allocationAfterCacheDestruction) {
  const MemorySize limit = 1_MB;
  auto alloc = Alloc::makeLimited(limit);
  runOnThread([&]() {
    // This object is constructed before the cache, so it is destroyed after.
    static thread_local AllocatesInDestructor object;
    object.alloc_ = &alloc;
    char* p = alloc.allocate(10);
    alloc.deallocate(p, 10);
  });
  EXPECT_EQ(alloc.amountMemoryLeft(), limit);
}

// Many threads concurrently allocate and free blocks of random sizes, some of
// which are freed on a different thread. Afterwards, the whole budget is
// available again.
TEST(MemoryLimitTracker, concurrentStressTest) {
  const MemorySize limit = 100_MB;
  auto alloc = Alloc::makeLimited(limit);
  constexpr size_t numThreads = 8;
  std::vector<std::vector<std::pair<char*, size_t>>> handedOver(numThreads);
  std::vector<std::thread> threads;
  for (size_t t = 0; t < numThreads; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937 rng(t);
      std::uniform_int_distribution<size_t> sizeDist(1, 3 * maxSmall);
      std::vector<std::pair<char*, size_t>> blocks;
      for (size_t i = 0; i < 20'000; ++i) {
        if (blocks.empty() || rng() % 3 != 0) {
          size_t size = sizeDist(rng);
          blocks.emplace_back(alloc.allocate(size), size);
        } else {
          auto [p, size] = blocks.back();
          blocks.pop_back();
          alloc.deallocate(p, size);
        }
      }
      handedOver[t] = std::move(blocks);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Free the remaining blocks of each thread on another thread.
  threads.clear();
  for (size_t t = 0; t < numThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (auto [p, size] : handedOver[(t + 1) % numThreads]) {
        alloc.deallocate(p, size);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(alloc.amountMemoryLeft(), limit);
}

// A non-owning copy shares the budget of its owner. Copies of a non-owning
// allocator (also rebound ones) are non-owning, too. The same is tested for the
// PMR backend in `AllocatorPmrTest.cpp`.
TEST(MemoryLimitTracker, nonOwningCopy) {
  auto owner = Alloc::makeLimited(8_B);
  EXPECT_TRUE(owner.isOwning());
  auto nonOwning = owner.nonOwningCopy();
  EXPECT_FALSE(nonOwning.isOwning());
  EXPECT_EQ(nonOwning, owner);

  char* p = nonOwning.allocate(4);
  EXPECT_EQ(owner.amountMemoryLeft(), 4_B);

  Alloc copy{nonOwning};
  EXPECT_FALSE(copy.isOwning());
  auto moved = std::move(copy);
  EXPECT_FALSE(moved.isOwning());
  auto rebound = nonOwning.as<int>();
  EXPECT_FALSE(rebound.isOwning());
  EXPECT_EQ(rebound.amountMemoryLeft(), 4_B);

  // A non-owning copy of a non-owning copy is fine as well.
  auto nonOwning2 = moved.nonOwningCopy();
  EXPECT_FALSE(nonOwning2.isOwning());
  nonOwning2.deallocate(p, 4);
  EXPECT_EQ(owner.amountMemoryLeft(), 8_B);
}

// A non-owning copy that outlives its owner terminates the program when the
// lifetime checks are enabled.
TEST(MemoryLimitTracker, nonOwningCopyOutlivesOwner) {
#ifdef QL_ENABLE_LIFETIME_CHECKS
  auto outliveOwner = []() {
    std::optional<Alloc> nonOwning;
    {
      auto owner = Alloc::makeLimited(8_B);
      nonOwning.emplace(owner.nonOwningCopy());
    }
    nonOwning.reset();
  };
  EXPECT_DEATH_IF_SUPPORTED(outliveOwner(), "outlived the owner");
#else
  GTEST_SKIP() << "The lifetime checks are disabled in this build";
#endif
}

}  // namespace
