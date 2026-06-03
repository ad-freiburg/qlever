// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "../util/IdTableHelpers.h"
#include "engine/ConstructDeduplicationFilter.h"
#include "global/ValueId.h"

namespace {
using namespace qlever::constructExport;
using ad_utility::DeduplicationMode;
using ad_utility::testing::makeAllocator;

// Helper to build a `DeduplicationKey` from three raw bit patterns.
// `ValueId::fromBits` constructs a `ValueId` directly from its underlying bit
// representation. Sufficient for tests that only need distinct, comparable
// values without valid RDF semantics.
DeduplicationKey makeKey(uint64_t s, uint64_t p, uint64_t o) {
  return {ValueId::fromBits(s), ValueId::fromBits(p), ValueId::fromBits(o)};
}

const DeduplicationKey kA = makeKey(1, 2, 3);
const DeduplicationKey kB = makeKey(4, 5, 6);
const DeduplicationKey kC = makeKey(7, 8, 9);

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

TEST(PerTripleFilterGlobal, DeduplicatesBeyondBatchWiseCapacity) {
  // Global mode uses an unbounded hash set (no LRU, no eviction): every unique
  // key is remembered forever. Insert 1001 distinct keys (more than the default
  // `BatchWise` capacity of 1000, where the first key would have been evicted),
  // then re-insert the first key; it must still be recognized as a duplicate.
  PerTripleFilter f{DeduplicationMode::global()};
  for (uint64_t i = 0; i < 1001; ++i) {
    f.insert(makeKey(i, 0, 0));
  }
  EXPECT_FALSE(f.insert(makeKey(0, 0, 0)));
}

// Builds a `PrecomputedConstant` term whose `dedupId_` is `ValueId::fromBits`.
// The `evaluatedTerm_` (string form) is left null:
// `ConstructDeduplicationState` only ever reads `dedupId_`.
PreprocessedTerm constTerm(uint64_t bits) {
  return PrecomputedConstant{EvaluatedTerm{}, ValueId::fromBits(bits)};
}

// Builds a `PrecomputedVariable` term referring to `column` of the `IdTable`.
PreprocessedTerm varTerm(size_t column) { return PrecomputedVariable{column}; }

PreprocessedTriple makeTriple(PreprocessedTerm s, PreprocessedTerm p,
                              PreprocessedTerm o) {
  return {std::move(s), std::move(p), std::move(o)};
}

TEST(ConstructDeduplicationState, GlobalDeduplicatesAcrossTemplateTriples) {
  // Two distinct template triples that instantiate to the *same* full triple.
  // In `global` mode the single shared filter must collapse them: the first is
  // new, the second (from a different template triple) is a duplicate.
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {
      makeTriple(constTerm(1), constTerm(2), constTerm(3)),
      makeTriple(constTerm(1), constTerm(2), constTerm(3))};
  tmpl.tripleContainsBlankNode_ = {false, false};

  // All positions are constants, so the `IdTable` is never indexed; a single
  // empty row suffices to satisfy the `BatchEvaluationContext` invariants.
  IdTable idTable = makeIdTableFromVector({{0}});
  BatchEvaluationContext ctx{idTable, 0, 1};

  ConstructDeduplicationState state{DeduplicationMode::global()};
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
  EXPECT_FALSE(state.isNew(1, 0, tmpl, ctx));
}

TEST(ConstructDeduplicationState, GlobalNeverDeduplicatesBlankNodeTriples) {
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {
      makeTriple(constTerm(1), constTerm(2), constTerm(3))};
  tmpl.tripleContainsBlankNode_ = {true};

  IdTable idTable = makeIdTableFromVector({{0}});
  BatchEvaluationContext ctx{idTable, 0, 1};

  ConstructDeduplicationState state{DeduplicationMode::global()};
  // Even the identical instantiation is always emitted.
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
}

TEST(ConstructDeduplicationState, BatchWiseDeduplicatesAcrossTemplateTriples) {
  // Two distinct template triples that instantiate to the same full triple.
  // `batchWise` also uses a single shared filter (a bounded LRU), so the second
  // instantiation is a duplicate even though it comes from a different template
  // triple.
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {
      makeTriple(varTerm(0), constTerm(2), constTerm(3)),
      makeTriple(varTerm(0), constTerm(2), constTerm(3))};
  tmpl.variableColumnsPerTriple_ = {{0}, {0}};
  tmpl.tripleContainsBlankNode_ = {false, false};

  IdTable idTable = makeIdTableFromVector({{5}});
  BatchEvaluationContext ctx{idTable, 0, 1};

  ConstructDeduplicationState state{DeduplicationMode::batchWise(10)};
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));   // triple 0, first time
  EXPECT_FALSE(state.isNew(1, 0, tmpl, ctx));  // same full triple -> duplicate
}

TEST(ConstructDeduplicationState, SeedGroundTripleSuppressesLaterMatch) {
  // Seeding a ground triple's full key into the shared global filter makes a
  // later non-ground instantiation of the same triple a duplicate.
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {
      makeTriple(constTerm(1), constTerm(2), constTerm(3))};
  tmpl.tripleContainsBlankNode_ = {false};

  IdTable idTable = makeIdTableFromVector({{0}});
  BatchEvaluationContext ctx{idTable, 0, 1};

  ConstructDeduplicationState state{DeduplicationMode::global()};
  state.seedGroundTriple(makeKey(1, 2, 3));
  EXPECT_FALSE(state.isNew(0, 0, tmpl, ctx));
}

}  // namespace
