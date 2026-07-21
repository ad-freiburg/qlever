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
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};

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
  BatchEvaluationContext c1{t1.asStaticView<0>(), 0, t1.numRows()};
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};

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
    BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};
    key1 = state.makeFullTripleKey(kVarTriple, 0, ctx);
  }  // `tmp` (and its entries) gone; `key1` must still be valid.

  LocalVocab fresh;
  auto table = singleIdTable(lvId(fresh, "x", *qec));
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};
  // Comparing/hashing `key1` here dereferences its ids; so it must not read
  // freed memory, and `key1` must still equal the equal-string key from a fresh
  // vocab.
  EXPECT_EQ(key1, state.makeFullTripleKey(kVarTriple, 0, ctx));
}

// A constant position contributes its precomputed `dedupId_` to the key
// (rather than reading anything from the row).
TEST(ConstructDeduplicationFilter, constantPositionUsesDedupId) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};

  PreprocessedTriple constTriple{PrecomputedConstant{{}, IntId(7)},
                                 PrecomputedConstant{{}, IntId(8)},
                                 PrecomputedConstant{{}, IntId(9)}};
  auto table = singleIdTable(IntId(0));  // no variable positions: row unused
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};

  DeduplicationKey key = state.makeFullTripleKey(constTriple, 0, ctx);
  EXPECT_EQ(key, (DeduplicationKey{IntId(7), IntId(8), IntId(9)}));
}

// Building a key for a blank-node position is a precondition violation:
// blank-node triples bypass deduplication and never reach `makeFullTripleKey`.
TEST(ConstructDeduplicationFilter, blankNodePositionInKeyFails) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};

  PreprocessedTriple bnTriple{PrecomputedBlankNode{"_:g", "_0"},
                              PrecomputedVariable{0}, PrecomputedVariable{0}};
  auto table = singleIdTable(IntId(1));
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};

  EXPECT_ANY_THROW(state.makeFullTripleKey(bnTriple, 0, ctx));
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
    BatchEvaluationContext c1{t1.asStaticView<0>(), 0, t1.numRows()};
    EXPECT_TRUE(state.isNew(0, 0, tmpl, c1));  // first occurrence
  }  // block-1 vocab freed before block-2 is seen

  LocalVocab v2;
  auto t2 = singleIdTable(lvId(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c2));  // duplicate across blocks
}

// Same as above, but through the `BatchWise` (LRU) filter path.
TEST(ConstructDeduplicationFilter, dedupAcrossBlocksBatchWise) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::batchWise(10), *qec};
  auto tmpl = singleTripleTemplate();

  LocalVocab v1;
  auto t1 = singleIdTable(lvId(v1, "x", *qec));
  BatchEvaluationContext c1{t1.asStaticView<0>(), 0, t1.numRows()};
  EXPECT_TRUE(state.isNew(0, 0, tmpl, c1));

  LocalVocab v2;
  auto t2 = singleIdTable(lvId(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c2));
}

// A template triple flagged as containing a blank node is always reported
// "new": blank-node triples bypass deduplication, so no key is built and
// nothing is ever recorded (so a second call is "new" again too).
TEST(ConstructDeduplicationFilter, blankNodeTripleAlwaysNew) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::global(), *qec};
  auto tmpl = singleTripleTemplate();
  tmpl.tripleContainsBlankNode_ = {true};

  auto table = singleIdTable(IntId(1));
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
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
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c2));  // suppressed by the ground seed
}

// `PerTripleFilter` must never be constructed for `none`: the caller creates no
// filter in that mode, so building one is a precondition violation (see the
// `AD_CONTRACT_CHECK` in `PerTripleFilter::makeFilter`).
TEST(ConstructDeduplicationFilter, perTripleFilterRejectsNone) {
  auto qec = getQec("<s> <p> <o>");
  EXPECT_ANY_THROW(PerTripleFilter(DeduplicationMode::none(), *qec));
}

// In `none` mode no filter is built (`makeFilter` returns `nullopt`), and
// `seedGroundTriple` is a no-op (its filter-empty branch). This must not touch
// any dedup state or crash.
TEST(ConstructDeduplicationFilter, noneModeHasNoFilter) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicationState state{DeduplicationMode::none(), *qec};
  state.seedGroundTriple(DeduplicationKey{IntId(1), IntId(1), IntId(1)});
}

}  // namespace
