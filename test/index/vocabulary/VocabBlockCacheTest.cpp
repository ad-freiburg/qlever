// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Marvin Stoetzel <stoetzem@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include <array>
#include <string>
#include <vector>

#include "../../util/GTestHelpers.h"
#include "../../util/RuntimeParametersTestHelpers.h"
#include "./VocabularyTestHelpers.h"
#include "global/RuntimeParameters.h"
#include "index/vocabulary/VocabBlockCache.h"
#include "index/vocabulary/VocabularyOnDisk.h"
#include "util/File.h"

namespace {
using ad_utility::vocab::VocabBlockCache;
constexpr size_t blockSize = VocabBlockCache::kBlockSize;

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

// Write `words` to a fresh `VocabularyOnDisk` file and open it. The file is
// deleted when the returned handle is destroyed.
struct VocabularyOnDiskHandle {
  std::string filename_;
  VocabularyOnDisk vocabulary_;
  explicit VocabularyOnDiskHandle(std::string filename,
                                  const std::vector<std::string>& words)
      : filename_{std::move(filename)} {
    ad_utility::deleteFile(filename_, false);
    {
      auto writer = VocabularyOnDisk::WordWriter(filename_);
      for (const auto& word : words) {
        writer(word, false);
      }
      writer.finish();
    }
    vocabulary_.open(filename_);
  }
  ~VocabularyOnDiskHandle() { ad_utility::deleteFile(filename_, false); }
};

// A `lookupBatch` with the block cache enabled must return exactly the same
// words as without the cache, including for words that span multiple 4 KiB
// blocks, empty words, and duplicated indices. The second identical batch must
// additionally be served (at least partially) from the cache.
TEST(VocabBlockCache, LookupBatchWithCacheMatchesIndividualLookups) {
  std::vector<std::string> words{"tiny", std::string(10'000, 'a'), "",
                                 std::string(5'000, 'b'), "tail"};
  VocabularyOnDiskHandle handle{absl::StrCat(gtestCurrentTestName(), ".dat"),
                                words};
  std::vector<size_t> indices{1, 0, 3, 2, 4, 1, 3};

  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::vocabBlockCacheSize_>(
          size_t{64});
  VocabBlockCache& shard = ad_utility::vocab::threadLocalVocabBlockCache(64);

  uint64_t hitsBefore = shard.numHits();
  auto first = handle.vocabulary_.lookupBatch(ql::span<const size_t>{indices});
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      handle.vocabulary_, first, indices);

  auto second = handle.vocabulary_.lookupBatch(ql::span<const size_t>{indices});
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      handle.vocabulary_, second, indices);
  // The multi-block words cover full 4 KiB blocks, so the repeated batch must
  // hit the cache.
  EXPECT_GT(shard.numHits(), hitsBefore);
}

// With the cache disabled (the default of `vocab-block-cache-size`), batched
// lookups behave exactly as before.
TEST(VocabBlockCache, LookupBatchWithDisabledCacheStillMatches) {
  auto cleanup =
      setRuntimeParameterForTest<&RuntimeParameters::vocabBlockCacheSize_>(
          size_t{0});
  std::vector<std::string> words{"alpha", std::string(8'000, 'z'), "omega"};
  VocabularyOnDiskHandle handle{absl::StrCat(gtestCurrentTestName(), ".dat"),
                                words};
  std::vector<size_t> indices{2, 0, 1, 1};
  auto result = handle.vocabulary_.lookupBatch(ql::span<const size_t>{indices});
  vocabulary_test::assertLookupResultMatchesVocabularyAtIndices(
      handle.vocabulary_, result, indices);
}

}  // namespace
