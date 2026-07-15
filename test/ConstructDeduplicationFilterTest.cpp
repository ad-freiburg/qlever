// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gtest/gtest.h>

#include "engine/ConstructDeduplicationFilter.h"
#include "index/LocalVocab.h"
#include "index/LocalVocabEntry.h"
#include "util/AllocatorTestHelpers.h"
#include "util/IdTestHelpers.h"
#include "util/IndexTestHelpers.h"
#include "util/MemorySize/MemorySize.h"

namespace {

using namespace qlever::constructExport;
using ad_utility::DeduplicationMode;
using ad_utility::testing::getQec;
using ad_utility::testing::IntId;

// Add `s` as a literal to `vocab` and return its `Id` (a `LocalVocabIndex`).
Id lvId(LocalVocab& vocab, std::string_view s,
        const QueryExecutionContext& qec) {
  return Id::makeFromLocalVocabIndex(vocab.getIndexAndAddIfNotContained(
      LocalVocabEntry::literalWithoutQuotes(s, qec.getLocalVocabContext())));
}

// A one-column, one-row `IdTable` holding `id`.
IdTable singleIdTable(Id id) {
  IdTable t{1, ad_utility::testing::makeAllocator()};
  t.push_back({id});
  return t;
}

// A template triple with the same variable (column 0) in all three positions.
const PreprocessedTriple kVarTriple{
    PrecomputedVariable{0}, PrecomputedVariable{0}, PrecomputedVariable{0}};

// A minimal template wrapping a single, non-blank-node triple.
PreprocessedConstructTemplate singleTripleTemplate() {
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {kVarTriple};
  tmpl.tripleContainsBlankNode_ = {false};
  return tmpl;
}

// Non-local-vocab ids (here: an encoded integer) pass through unchanged; the
// canonicalization fast path leaves them untouched.
TEST(ConstructDeduplicationFilter, passThroughForNonLocalVocab) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};

  Id id = IntId(42);
  auto table = singleIdTable(id);
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, 1};

  DeduplicationKey key = state.makeFullTripleKey(kVarTriple, 0, ctx);
  EXPECT_EQ(key, (DeduplicationKey{id, id, id}));
}

// The same literal reached via two different `LocalVocab`s must produce the
// same key, because both are reseated into the state's own `dedupVocab_`.
TEST(ConstructDeduplicationFilter, crossVocabCollapse) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};

  LocalVocab v1;
  LocalVocab v2;
  auto t1 = singleIdTable(lvId(v1, "x", *qec));
  auto t2 = singleIdTable(lvId(v2, "x", *qec));
  BatchEvaluationContext c1{t1.asStaticView<0>(), 0, 1};
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, 1};

  EXPECT_EQ(state.makeFullTripleKey(kVarTriple, 0, c1),
            state.makeFullTripleKey(kVarTriple, 0, c2));
}

// A key built from a `LocalVocab` that is then destroyed must not dangle: the
// reseated ids live in `dedupVocab_`, which outlives the source. Run under ASan
// to turn a lifetime regression into a detected use-after-free.
TEST(ConstructDeduplicationFilter, keySurvivesSourceVocabDestruction) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};

  DeduplicationKey key1;
  {
    LocalVocab tmp;  // destroyed at the end of this scope
    auto table = singleIdTable(lvId(tmp, "x", *qec));
    BatchEvaluationContext ctx{table.asStaticView<0>(), 0, 1};
    key1 = state.makeFullTripleKey(kVarTriple, 0, ctx);
  }  // `tmp` (and its entries) gone; `key1` must still be valid.

  LocalVocab fresh;
  auto table = singleIdTable(lvId(fresh, "x", *qec));
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, 1};
  // Comparing/hashing `key1` here dereferences its ids; so it must not read
  // freed memory, and `key1` must still equal the equal-string key from a fresh
  // vocab.
  EXPECT_EQ(key1, state.makeFullTripleKey(kVarTriple, 0, ctx));
}

// `global` dedup collapses the same local-vocab triple across two result
// blocks (each with its own, separately-destroyed `LocalVocab`).
TEST(ConstructDeduplicationFilter, dedupAcrossBlocksGlobal) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};
  auto tmpl = singleTripleTemplate();

  {
    LocalVocab v1;
    auto t1 = singleIdTable(lvId(v1, "x", *qec));
    BatchEvaluationContext c1{t1.asStaticView<0>(), 0, 1};
    EXPECT_TRUE(state.isNew(0, 0, tmpl, c1));  // first occurrence
  }  // block-1 vocab freed before block-2 is seen

  LocalVocab v2;
  auto t2 = singleIdTable(lvId(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, 1};
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c2));  // duplicate across blocks
}

// Same as above, but through the `BatchWise` (LRU) filter path.
TEST(ConstructDeduplicationFilter, dedupAcrossBlocksBatchWise) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::batchWise(10), *qec};
  auto tmpl = singleTripleTemplate();

  LocalVocab v1;
  auto t1 = singleIdTable(lvId(v1, "x", *qec));
  BatchEvaluationContext c1{t1.asStaticView<0>(), 0, 1};
  EXPECT_TRUE(state.isNew(0, 0, tmpl, c1));

  LocalVocab v2;
  auto t2 = singleIdTable(lvId(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, 1};
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c2));
}

// A ground triple seeded with a local-vocab constant suppresses a later
// non-ground instantiation of the same triple. This exercises `canonicalizeKey`
// (the seed and the non-ground key must agree after reseating).
TEST(ConstructDeduplicationFilter, seedGroundTripleSuppressesNonGround) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};
  auto tmpl = singleTripleTemplate();

  LocalVocab v1;
  Id x = lvId(v1, "x", *qec);
  state.seedGroundTriple(DeduplicationKey{x, x, x});

  LocalVocab v2;
  auto t2 = singleIdTable(lvId(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, 1};
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c2));  // suppressed by the ground seed
}

// _____________________________________________________________________________
// A tiny memory threshold forces the filter to drop all dedup state once its
// internal vocab grows past it, which makes deduplication approximate: the same
// local-vocab triple is reported "new" again after the reset (rather than a
// duplicate). Contrast with `dedupAcrossBlocksGlobal`, which uses the default
// (large) threshold and deduplicates.
TEST(ConstructDeduplicationFilter, resetsWhenVocabExceedsThreshold) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec,
                                    ad_utility::MemorySize::bytes(1)};
  auto tmpl = singleTripleTemplate();

  LocalVocab v;
  auto t = singleIdTable(lvId(v, "x", *qec));
  BatchEvaluationContext c{t.asStaticView<0>(), 0, 1};

  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));  // first occurrence
  // The 1-byte threshold was exceeded, so the next call resets the dedup state
  // and the identical triple is treated as new again.
  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));
}

}  // namespace
