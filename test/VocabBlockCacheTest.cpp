// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "util/VocabBlockCache.h"

namespace {
using ad_utility::vocab::VocabBlockCache;
constexpr size_t blockSize = VocabBlockCache::kDefaultBlockSize;

// Fill a block with deterministic content derived from `seed`.
std::array<char, blockSize> makeBlockData(uint64_t seed) {
  std::array<char, blockSize> data{};
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<char>((seed * 2654435761u + i) % 251);
  }
  return data;
}

// Assert that `cache` holds exactly `expected` under `(dev, ino, blockNo)`.
void expectCachedBlock(VocabBlockCache& cache, dev_t dev, ino_t ino,
                       uint64_t blockNo,
                       const std::array<char, blockSize>& expected) {
  const char* cached = cache.lookup(dev, ino, blockNo);
  ASSERT_NE(cached, nullptr) << "block " << blockNo << " should be cached";
  // NOTE: no braced-init with a comma directly inside EXPECT_EQ (the
  // preprocessor would split the macro arguments); compare named views.
  std::string_view actual{cached, blockSize};
  std::string_view want{expected.data(), blockSize};
  EXPECT_EQ(actual, want);
}

constexpr dev_t testDev = 42;
constexpr ino_t testIno = 1234;

TEST(VocabBlockCache, EmptyCacheAlwaysMisses) {
  VocabBlockCache cache{0};
  EXPECT_EQ(cache.capacity(), 0u);
  EXPECT_EQ(cache.size(), 0u);
  EXPECT_EQ(cache.lookup(testDev, testIno, 0), nullptr);
  // Inserting into a zero-capacity cache is a no-op (in particular, it must
  // not crash or allocate).
  auto data = makeBlockData(1);
  cache.insert(testDev, testIno, 0, data.data());
  EXPECT_EQ(cache.lookup(testDev, testIno, 0), nullptr);
  EXPECT_EQ(cache.numMisses(), 2u);
  EXPECT_EQ(cache.numHits(), 0u);
}

TEST(VocabBlockCache, InsertThenHit) {
  VocabBlockCache cache{4};
  EXPECT_EQ(cache.lookup(testDev, testIno, 7), nullptr);
  auto data = makeBlockData(7);
  cache.insert(testDev, testIno, 7, data.data());
  EXPECT_EQ(cache.size(), 1u);
  expectCachedBlock(cache, testDev, testIno, 7, data);
  // A repeat lookup hits again with identical bytes.
  expectCachedBlock(cache, testDev, testIno, 7, data);
  EXPECT_EQ(cache.numMisses(), 1u);
  EXPECT_EQ(cache.numHits(), 2u);
}

TEST(VocabBlockCache, DifferentFilesDoNotAlias) {
  VocabBlockCache cache{4};
  auto dataA = makeBlockData(1);
  auto dataB = makeBlockData(2);
  cache.insert(testDev, testIno, 3, dataA.data());
  // Same block number, but a different file: must miss.
  EXPECT_EQ(cache.lookup(testDev, testIno + 1, 3), nullptr);
  EXPECT_EQ(cache.lookup(testDev + 1, testIno, 3), nullptr);
  cache.insert(testDev, testIno + 1, 3, dataB.data());
  expectCachedBlock(cache, testDev, testIno, 3, dataA);
  expectCachedBlock(cache, testDev, testIno + 1, 3, dataB);
  EXPECT_EQ(cache.size(), 2u);
}

TEST(VocabBlockCache, ClockEvictionPrefersReferencedBlocks) {
  VocabBlockCache cache{2};
  auto dataA = makeBlockData(10);
  auto dataB = makeBlockData(11);
  auto dataC = makeBlockData(12);
  cache.insert(testDev, testIno, 0, dataA.data());
  cache.insert(testDev, testIno, 1, dataB.data());
  // Reference block 0, so the clock hand must evict block 1 next.
  expectCachedBlock(cache, testDev, testIno, 0, dataA);
  cache.insert(testDev, testIno, 2, dataC.data());
  EXPECT_EQ(cache.numEvictions(), 1u);
  expectCachedBlock(cache, testDev, testIno, 0, dataA);
  expectCachedBlock(cache, testDev, testIno, 2, dataC);
  EXPECT_EQ(cache.lookup(testDev, testIno, 1), nullptr);
  EXPECT_EQ(cache.size(), 2u);
}

TEST(VocabBlockCache, ReinsertOverwritesSameKeyWithoutEviction) {
  VocabBlockCache cache{2};
  auto data = makeBlockData(5);
  cache.insert(testDev, testIno, 0, data.data());
  cache.insert(testDev, testIno, 1, makeBlockData(6).data());
  // Re-inserting an existing key (a re-read of an immutable file) overwrites
  // in place and evicts nothing.
  cache.insert(testDev, testIno, 0, data.data());
  EXPECT_EQ(cache.numEvictions(), 0u);
  expectCachedBlock(cache, testDev, testIno, 0, data);
}

// The vocabulary index files are immutable after the build, so cached blocks
// never go stale and the cache needs (and has) no invalidation: entries only
// ever disappear via clock eviction or an explicit `resize`. In particular, a
// referenced block survives unrelated churn, and repeated lookups always
// return the originally inserted bytes.
TEST(VocabBlockCache, ImmutableBlocksNeverGoStale) {
  VocabBlockCache cache{3};
  auto data = makeBlockData(99);
  cache.insert(testDev, testIno, 0, data.data());
  for (int round = 0; round < 5; ++round) {
    // Unrelated churn on other blocks, always re-referencing block 0 so the
    // clock hand evicts the churn blocks instead.
    cache.insert(testDev, testIno, 1, makeBlockData(round).data());
    expectCachedBlock(cache, testDev, testIno, 0, data);
    cache.insert(testDev, testIno, 2, makeBlockData(1000 + round).data());
    expectCachedBlock(cache, testDev, testIno, 0, data);
  }
  expectCachedBlock(cache, testDev, testIno, 0, data);
}

TEST(VocabBlockCache, ResizeDropsAllEntries) {
  VocabBlockCache cache{4};
  auto data = makeBlockData(1);
  cache.insert(testDev, testIno, 0, data.data());
  ASSERT_NE(cache.lookup(testDev, testIno, 0), nullptr);
  cache.resize(4);
  EXPECT_EQ(cache.size(), 0u);
  EXPECT_EQ(cache.lookup(testDev, testIno, 0), nullptr);
  cache.resize(0);
  EXPECT_EQ(cache.capacity(), 0u);
  cache.insert(testDev, testIno, 0, data.data());
  EXPECT_EQ(cache.lookup(testDev, testIno, 0), nullptr);
}

// Each thread owns its shard without locks; at minimum, the shard of the
// calling thread must be usable and must adopt the requested capacity.
TEST(VocabBlockCache, ThreadLocalShardAdoptsCapacity) {
  // Use an unusual capacity to avoid interference with other tests that share
  // this thread's shard.
  VocabBlockCache& shard = ad_utility::vocab::threadLocalVocabBlockCache(13);
  EXPECT_EQ(shard.capacity(), 13u);
  auto data = makeBlockData(3);
  shard.insert(testDev, testIno, 8, data.data());
  expectCachedBlock(shard, testDev, testIno, 8, data);
}

// Many more blocks than slots: the hash index must stay consistent with the
// slots through evictions, and every lookup must return the bytes of exactly
// the requested block or miss.
TEST(VocabBlockCache, HashIndexStaysConsistentUnderEviction) {
  constexpr size_t capacity = 64;
  VocabBlockCache cache{capacity};
  for (uint64_t blockNo = 0; blockNo < 10 * capacity; ++blockNo) {
    auto data = makeBlockData(blockNo);
    cache.insert(testDev, testIno, blockNo, data.data());
    EXPECT_LE(cache.size(), capacity);
  }
  EXPECT_EQ(cache.size(), capacity);
  size_t numCached = 0;
  for (uint64_t blockNo = 0; blockNo < 10 * capacity; ++blockNo) {
    if (const char* cached = cache.lookup(testDev, testIno, blockNo)) {
      ++numCached;
      auto expected = makeBlockData(blockNo);
      EXPECT_EQ((std::string_view{cached, blockSize}),
                (std::string_view{expected.data(), blockSize}));
    }
  }
  EXPECT_EQ(numCached, capacity);
  // The most recently inserted block is always cached.
  auto last = makeBlockData(10 * capacity - 1);
  expectCachedBlock(cache, testDev, testIno, 10 * capacity - 1, last);
}

// Blocks larger than 4 KiB: the whole block is stored and returned, and a
// block size that is not a positive multiple of 4 KiB is rejected.
TEST(VocabBlockCache, LargerBlockSize) {
  constexpr size_t largeBlockSize = 4 * blockSize;
  VocabBlockCache cache{3, largeBlockSize};
  EXPECT_EQ(cache.blockSize(), largeBlockSize);
  std::string data(largeBlockSize, '\0');
  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<char>((i * 7) % 251);
  }
  cache.insert(testDev, testIno, 5, data.data());
  const char* cached = cache.lookup(testDev, testIno, 5);
  ASSERT_NE(cached, nullptr);
  EXPECT_EQ((std::string_view{cached, largeBlockSize}), data);

  // Changing the block size drops the content.
  cache.resize(3, blockSize);
  EXPECT_EQ(cache.blockSize(), blockSize);
  EXPECT_EQ(cache.lookup(testDev, testIno, 5), nullptr);
  VocabBlockCache& shard =
      ad_utility::vocab::threadLocalVocabBlockCache(13, largeBlockSize);
  EXPECT_EQ(shard.blockSize(), largeBlockSize);
  EXPECT_EQ(shard.capacity(), 13u);

  EXPECT_ANY_THROW(cache.resize(3, 0));
  EXPECT_ANY_THROW(cache.resize(3, blockSize + 512));
}

}  // namespace
