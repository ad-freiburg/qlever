// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "engine/ConstructDeduplicationFilter.h"
#include "global/ValueId.h"

namespace {
using namespace qlever::constructExport;
using ad_utility::DeduplicationMode;

// Helper to build a DeduplicationKey from three raw bit patterns.
// `ValueId::fromBits` constructs a ValueId directly from its underlying
// bit representation — sufficient for tests that only need distinct,
// comparable values without valid RDF semantics.
DeduplicationKey makeKey(uint64_t s, uint64_t p, uint64_t o) {
  return {ValueId::fromBits(s), ValueId::fromBits(p), ValueId::fromBits(o)};
}

const DeduplicationKey kA = makeKey(1, 2, 3);
const DeduplicationKey kB = makeKey(4, 5, 6);
const DeduplicationKey kC = makeKey(7, 8, 9);
const DeduplicationKey kUndef = {ValueId::makeUndefined(),
                                 ValueId::makeUndefined(),
                                 ValueId::makeUndefined()};

// _____________________________________________________________________________

TEST(LruDeduplicationCache, NewKeyReturnsTrue) {
  // we expect, if we initialize a `LruDeduplicationCache` with capacity > 0,
  // and add a single item that a new key can be added.
  LruDeduplicationCache cache{10};
  EXPECT_TRUE(cache.insert(kA));
}

TEST(LruDeduplicationCache, DuplicateKeyReturnsFalse) {
  LruDeduplicationCache cache{10};
  cache.insert(kA);
  // expect that if we insert the same item triple twice that we detect a
  // duplicate.
  EXPECT_FALSE(cache.insert(kA));
}

TEST(LruDeduplicationCache, EvictsLruOnOverFlow) {
  // LRU with capacity 2: insert `kA`, `kB`, then `kC`. `kA` should be evicted.
  LruDeduplicationCache cache{2};
  EXPECT_TRUE(cache.insert(kA));
  EXPECT_TRUE(cache.insert(kB));
  EXPECT_TRUE(cache.insert(kC));  // evicts `kA`
  // `kA` is no longer in cache, reinserting it should return true.
  EXPECT_TRUE(cache.insert(kA));  // evicts `kB`
}

TEST(LruDeduplicationCache, RecentlyAccesedNotEvicted) {
  // capacity 2: insert `kA`, `kB`. Access `kA` (moves it to most recently
  // used). Insert `kC` should evict `kB`, not `kA`.
  LruDeduplicationCache cache{2};
  cache.insert(kA);
  cache.insert(kB);
  cache.insert(kA);  // re-access `kA` -> moves it to front.
  cache.insert(kC);  // evicts `kB`.
  // since `kB` is evicted, re-inserting it returns true.
  EXPECT_TRUE(cache.insert(kB));
  // `kC` still in cache, re-inserting returns false.
  EXPECT_FALSE(cache.insert(kC));
}

TEST(PerTripleFilterLru, NewKeyReturnsTrue) {
  PerTripleFilter f{DeduplicationMode::batchWise(10)};
  EXPECT_TRUE(f.insert(kA));
}

TEST(PerTripleFilterLru, DuplicateReturnsFalse) {
  PerTripleFilter f{DeduplicationMode::batchWise(10)};
  f.insert(kA);
  EXPECT_FALSE(f.insert(kA));
}

TEST(PerTripleFilterLru, EvictedKeyBecomesNew) {
  // Capacity 2: fill with kA and kB, then insert kC which evicts kA.
  PerTripleFilter f{DeduplicationMode::batchWise(2)};
  f.insert(kA);
  f.insert(kB);
  f.insert(kC);  // evicts kA
  EXPECT_TRUE(f.insert(kA));
}

TEST(PerTripleFilterGlobal, NewKeyReturnsTrue) {
  PerTripleFilter f{DeduplicationMode::global()};
  EXPECT_TRUE(f.insert(kA));
}

TEST(PerTripleFilterGlobal, DuplicateReturnsFalse) {
  PerTripleFilter f{DeduplicationMode::global()};
  f.insert(kA);
  EXPECT_FALSE(f.insert(kA));
}

TEST(PerTripleFilterGlobal, EvictedFromLruStillDeduplicated) {
  // Insert 1001 distinct keys to overflow the default LRU capacity (1000).
  // The first key should have been evicted from the LRU but lives in the
  // global hash set, so re-inserting it must still return false.
  PerTripleFilter f{DeduplicationMode::global()};
  for (uint64_t i = 0; i < 1001; ++i) {
    f.insert(makeKey(i, 0, 0));
  }
  EXPECT_FALSE(f.insert(makeKey(0, 0, 0)));
}

TEST(MakeDeduplicationState, CorrectNumberOfFilters) {
  auto state = makeDeduplicationState(DeduplicationMode::global(), 3);
  EXPECT_EQ(state.size(), 3u);
}

TEST(MakeDeduplicationState, FiltersAreIndependent) {
  // Inserting kA into filter 0 must not affect filter 1.
  auto state = makeDeduplicationState(DeduplicationMode::global(), 2);
  EXPECT_TRUE(state[0].insert(kA));
  EXPECT_TRUE(state[1].insert(kA));   // filter 1 has not seen kA
  EXPECT_FALSE(state[0].insert(kA));  // filter 0 has seen kA
}

TEST(MakeDeduplicationState, ZeroTriplesProducesEmptyState) {
  auto state = makeDeduplicationState(DeduplicationMode::none(), 0);
  EXPECT_TRUE(state.empty());
}

}  // namespace
