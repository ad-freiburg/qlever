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
Id makeLocalVocabIndex(LocalVocab& vocab, std::string_view s,
                       const QueryExecutionContext& qec) {
  return Id::makeFromLocalVocabIndex(vocab.getIndexAndAddIfNotContained(
      LocalVocabEntry::literalWithoutQuotes(s, qec.getLocalVocabContext())));
}

// Build a one-column, one-row `IdTable` holding `id`.
IdTable makeIdTable(Id id) {
  IdTable t{1, ad_utility::testing::makeAllocator()};
  t.push_back({id});
  return t;
}

// Make a template triple with the same variable (column 0) in all three
// positions.
const PreprocessedTriple allSameVarTriple{
    PrecomputedVariable{0}, PrecomputedVariable{0}, PrecomputedVariable{0}};

// Build a minimal template wrapping a single, non-blank-node triple.
PreprocessedConstructTemplate MakeSingleTripleTemplate() {
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {allSameVarTriple};
  tmpl.tripleContainsBlankNode_ = {false};
  return tmpl;
}

// Non-local-vocab ids (here: an encoded integer) pass through unchanged; the
// canonicalization fast path leaves them untouched.
TEST(ConstructDeduplicationFilter, passThroughForNonLocalVocab) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator deduplicator{DeduplicationMode::global(), *qec};

  Id id = IntId(42);
  auto table = makeIdTable(id);
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};

  DeduplicationKey key =
      deduplicator.makeFullTripleKey(allSameVarTriple, 0, ctx);
  EXPECT_EQ(key, (DeduplicationKey{id, id, id}));
}

// The same literal reached via two different `LocalVocab`s must be reseated
// into the state's own `dedupVocab_`. We check this at the bit level, not with
// `operator==`: two `LocalVocabIndex`es pointing to equal strings compare equal
// regardless of which vocab they live in, so an `EXPECT_EQ` on the keys would
// pass even without any reseating. Instead, we require that the reseated ids
// are bitwise identical to each other (same `dedupVocab_` entry) yet bitwise
// distinct from both sources (proving they were actually moved).
TEST(ConstructDeduplicationFilter, crossVocabCollapse) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator deduplicator{DeduplicationMode::global(), *qec};

  LocalVocab v1;
  LocalVocab v2;
  Id src1 = makeLocalVocabIndex(v1, "x", *qec);
  Id src2 = makeLocalVocabIndex(v2, "x", *qec);
  auto t1 = makeIdTable(src1);
  auto t2 = makeIdTable(src2);
  BatchEvaluationContext c1{t1.asStaticView<0>(), 0, t1.numRows()};
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};

  // The two sources are genuinely different ids (distinct vocab entries), even
  // though they encode equal strings.
  ASSERT_NE(src1.getBits(), src2.getBits());

  Id reseated1 = deduplicator.makeFullTripleKey(allSameVarTriple, 0, c1)[0];
  Id reseated2 = deduplicator.makeFullTripleKey(allSameVarTriple, 0, c2)[0];

  // Collapsed: both keys hold the same `dedupVocab_` entry, so bitwise equal.
  EXPECT_EQ(reseated1.getBits(), reseated2.getBits());
  // Actually reseated: distinct from either source (not a pass-through).
  EXPECT_NE(reseated1.getBits(), src1.getBits());
  EXPECT_NE(reseated1.getBits(), src2.getBits());
}

// A key built from a `LocalVocab` that is then destroyed must not dangle: the
// reseated ids live in `dedupVocab_`, which outlives the source. Run under ASan
// to turn a lifetime regression into a detected use-after-free.
TEST(ConstructDeduplicationFilter, keySurvivesSourceVocabDestruction) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator deduplicator{DeduplicationMode::global(), *qec};

  DeduplicationKey key1;
  {
    LocalVocab tmp;  // destroyed at the end of this scope
    auto table = makeIdTable(makeLocalVocabIndex(tmp, "x", *qec));
    BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};
    key1 = deduplicator.makeFullTripleKey(allSameVarTriple, 0, ctx);
  }  // `tmp` (and its entries) gone; `key1` must still be valid.

  LocalVocab fresh;
  auto table = makeIdTable(makeLocalVocabIndex(fresh, "x", *qec));
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};
  // Comparing/hashing `key1` here dereferences its ids; so it must not read
  // freed memory, and `key1` must still equal the equal-string key from a fresh
  // vocab.
  EXPECT_EQ(key1, deduplicator.makeFullTripleKey(allSameVarTriple, 0, ctx));
}

// A constant position contributes its precomputed `dedupId_` to the key
// (rather than reading anything from the row).
TEST(ConstructDeduplicationFilter, constantPositionUsesDedupId) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator deduplicator{DeduplicationMode::global(), *qec};

  PreprocessedTriple constTriple{PrecomputedConstant{{}, IntId(7)},
                                 PrecomputedConstant{{}, IntId(8)},
                                 PrecomputedConstant{{}, IntId(9)}};
  auto table = makeIdTable(IntId(0));  // no variable positions: row unused
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};

  DeduplicationKey key = deduplicator.makeFullTripleKey(constTriple, 0, ctx);
  EXPECT_EQ(key, (DeduplicationKey{IntId(7), IntId(8), IntId(9)}));
}

// Building a key for a blank-node position is a precondition violation:
// blank-node triples bypass deduplication and never reach `makeFullTripleKey`.
TEST(ConstructDeduplicationFilter, blankNodePositionInKeyFails) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator deduplicator{DeduplicationMode::global(), *qec};

  PreprocessedTriple blankNodeTriple{PrecomputedBlankNode{"_:g", "_0"},
                                     PrecomputedVariable{0},
                                     PrecomputedVariable{0}};
  auto table = makeIdTable(IntId(1));
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};

  EXPECT_ANY_THROW(deduplicator.makeFullTripleKey(blankNodeTriple, 0, ctx));
}

// `global` dedup collapses the same local-vocab triple across two result
// blocks (each with its own, separately-destroyed `LocalVocab`).
TEST(ConstructDeduplicationFilter, dedupAcrossBlocksGlobal) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator deduplicator{DeduplicationMode::global(), *qec};
  auto tmpl = MakeSingleTripleTemplate();

  {
    LocalVocab v1;
    auto t1 = makeIdTable(makeLocalVocabIndex(v1, "x", *qec));
    BatchEvaluationContext c1{t1.asStaticView<0>(), 0, t1.numRows()};
    EXPECT_TRUE(deduplicator.isNew(0, 0, tmpl, c1));  // first occurrence
  }  // block-1 vocab freed before block-2 is seen

  LocalVocab v2;
  auto t2 = makeIdTable(makeLocalVocabIndex(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};
  EXPECT_FALSE(deduplicator.isNew(0, 0, tmpl, c2));  // duplicate across blocks
}

// Same as above, but through the `BatchWise` (LRU) filter path.
TEST(ConstructDeduplicationFilter, dedupAcrossBlocksBatchWise) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator deduplicator{DeduplicationMode::batchWise(10), *qec};
  auto tmpl = MakeSingleTripleTemplate();

  LocalVocab v1;
  auto t1 = makeIdTable(makeLocalVocabIndex(v1, "x", *qec));
  BatchEvaluationContext c1{t1.asStaticView<0>(), 0, t1.numRows()};
  EXPECT_TRUE(deduplicator.isNew(0, 0, tmpl, c1));

  LocalVocab v2;
  auto t2 = makeIdTable(makeLocalVocabIndex(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};
  EXPECT_FALSE(deduplicator.isNew(0, 0, tmpl, c2));
}

// A template triple flagged as containing a blank node is always reported
// "new": blank-node triples bypass deduplication, so no key is built and
// nothing is ever recorded (so a second call is "new" again too).
TEST(ConstructDeduplicationFilter, blankNodeTripleAlwaysNew) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator state{DeduplicationMode::global(), *qec};
  auto tmpl = MakeSingleTripleTemplate();
  tmpl.tripleContainsBlankNode_ = {true};

  auto table = makeIdTable(IntId(1));
  BatchEvaluationContext ctx{table.asStaticView<0>(), 0, table.numRows()};
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
}

// A ground triple seeded with a local-vocab constant suppresses a later
// non-ground instantiation of the same triple. This exercises `canonicalizeKey`
// (the seed and the non-ground key must agree after reseating).
TEST(ConstructDeduplicationFilter, seedGroundTripleSuppressesNonGround) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator state{DeduplicationMode::global(), *qec};
  auto tmpl = MakeSingleTripleTemplate();

  LocalVocab v1;
  Id x = makeLocalVocabIndex(v1, "x", *qec);
  state.seedGroundTriple(DeduplicationKey{x, x, x});

  LocalVocab v2;
  auto t2 = makeIdTable(makeLocalVocabIndex(v2, "x", *qec));
  BatchEvaluationContext c2{t2.asStaticView<0>(), 0, t2.numRows()};
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c2));  // suppressed by the ground seed
}

// In `batchWise` mode a tiny memory threshold forces the filter to drop all
// dedup state once its internal vocab grows past it, which makes deduplication
// approximate: the same local-vocab triple is reported "new" again after the
// reset (rather than a duplicate). Contrast with `dedupAcrossBlocksBatchWise`,
// which uses the default (large) threshold and deduplicates.
TEST(ConstructDeduplicationFilter, batchWiseResetsWhenVocabExceedsThreshold) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator state{DeduplicationMode::batchWise(10), *qec,
                              ad_utility::MemorySize::bytes(1)};
  auto tmpl = MakeSingleTripleTemplate();

  LocalVocab v;
  auto t = makeIdTable(makeLocalVocabIndex(v, "x", *qec));
  BatchEvaluationContext c{t.asStaticView<0>(), 0, t.numRows()};

  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));  // first occurrence
  // The 1-byte threshold was exceeded, so the next call resets the dedup state
  // and the identical triple is treated as new again.
  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));
}

// `global` dedup is exact and is never reset on memory pressure: the vocab
// threshold does not apply to it. So even with a 1-byte threshold the identical
// triple is still recognized as a duplicate.
TEST(ConstructDeduplicationFilter, globalIgnoresVocabThreshold) {
  auto qec = getQec("<s> <p> <o>");
  ConstructDeduplicator state{DeduplicationMode::global(), *qec,
                              ad_utility::MemorySize::bytes(1)};
  auto tmpl = MakeSingleTripleTemplate();

  LocalVocab v;
  auto t = makeIdTable(makeLocalVocabIndex(v, "x", *qec));
  BatchEvaluationContext c{t.asStaticView<0>(), 0, t.numRows()};

  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));  // first occurrence
  // The tiny threshold is ignored for `global`: no reset, so the identical
  // triple is still a duplicate.
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c));
}

// `TripleDeduplicator` must never be constructed for `none`: the caller creates
// no filter in that mode, so building one is a precondition violation (see the
// `AD_CONTRACT_CHECK` in `TripleDeduplicator::makeDeduplicator`).
TEST(ConstructDeduplicationFilter, perTripleFilterRejectsNone) {
  auto qec = getQec("<s> <p> <o>");
  EXPECT_ANY_THROW(TripleDeduplicator(DeduplicationMode::none(), *qec));
}

// `None` is not modelled by `ConstructDeduplicator`: the caller handles
// it by not constructing the state at all. Constructing it with `None` is a
// precondition violation.
TEST(ConstructDeduplicationFilter, noneModeIsRejected) {
  auto qec = getQec("<s> <p> <o>");
  EXPECT_ANY_THROW(ConstructDeduplicator(DeduplicationMode::none(), *qec));
}

}  // namespace
