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

// Build a one-column, one-row `IdTable` holding `id`.
IdTable makeIdTable(Id id) {
  IdTable t{1, ad_utility::testing::makeAllocator()};
  t.push_back({id});
  return t;
}

BatchEvaluationContext makeFullBatch(const IdTable& table) {
  return BatchEvaluationContext{table.asStaticView<0>(), 0, table.numRows()};
}

// Owns a `LocalVocab` together with a one-row `IdTable` whose single id points
// into that vocab, so the vocab always outlives any view of the table. `ctx()`
// recomputes a full-table `BatchEvaluationContext`; the row must outlive the
// returned context.
struct LocalVocabRow {
  LocalVocab vocab_;
  IdTable table_;
  // The sole id in the table (a `LocalVocabIndex` into `vocab_`).
  Id id() const { return table_(0, 0); }
  BatchEvaluationContext ctx() const { return makeFullBatch(table_); }
};

// Make a template triple with the same variable (column 0) in all three
// positions.
const PreprocessedTriple allSameVarTriple{
    PrecomputedVariable{0}, PrecomputedVariable{0}, PrecomputedVariable{0}};

// Build a minimal template wrapping a single, non-blank-node triple.
PreprocessedConstructTemplate makeSingleTripleTemplate() {
  PreprocessedConstructTemplate tmpl;
  tmpl.preprocessedTriples_ = {allSameVarTriple};
  tmpl.tripleContainsBlankNode_ = {false};
  return tmpl;
}

// Shared fixture: holds the `QueryExecutionContext` every test needs and offers
// helpers to build a `Global`-mode deduplicator and local-vocab ids on it.
class ConstructDeduplicationFilter : public ::testing::Test {
 protected:
  QueryExecutionContext* qec_ = getQec("<s> <p> <o>");

  // Build a `ConstructDeduplicator` in `Global` mode on `qec_`.
  ConstructDeduplicator makeGlobalDeduplicator(
      std::optional<ad_utility::MemorySize> maxDedupVocabSize = std::nullopt) {
    return ConstructDeduplicator{DeduplicationMode::global(), *qec_,
                                 maxDedupVocabSize};
  }

  // Add `s` as a literal to `vocab` and return its `Id` (a `LocalVocabIndex`).
  Id makeLocalVocabIndex(LocalVocab& vocab, std::string_view s) {
    return Id::makeFromLocalVocabIndex(vocab.getIndexAndAddIfNotContained(
        LocalVocabEntry::literalWithoutQuotes(s,
                                              qec_->getLocalVocabContext())));
  }

  // Build a `LocalVocabRow` for a fresh literal `s` on `qec_`, bundling the
  // owning `LocalVocab` and its one-row `IdTable`.
  LocalVocabRow makeLocalVocabRow(std::string_view s) {
    LocalVocab vocab;
    Id id = makeLocalVocabIndex(vocab, s);
    return LocalVocabRow{std::move(vocab), makeIdTable(id)};
  }
};

// `makeFullTripleKey` copies an encoded (non-local-vocab) id into the key
// unchanged, since `canonicalize` only re-anchors `LocalVocabIndex` ids.
TEST_F(ConstructDeduplicationFilter, passThroughForNonLocalVocab) {
  ConstructDeduplicator deduplicator = makeGlobalDeduplicator();

  Id id = IntId(42);
  auto table = makeIdTable(id);
  auto ctx = makeFullBatch(table);

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
TEST_F(ConstructDeduplicationFilter, crossVocabCollapse) {
  ConstructDeduplicator deduplicator = makeGlobalDeduplicator();

  LocalVocabRow row1 = makeLocalVocabRow("x");
  LocalVocabRow row2 = makeLocalVocabRow("x");
  Id src1 = row1.id();
  Id src2 = row2.id();
  auto c1 = row1.ctx();
  auto c2 = row2.ctx();

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
TEST_F(ConstructDeduplicationFilter, keySurvivesSourceVocabDestruction) {
  ConstructDeduplicator deduplicator = makeGlobalDeduplicator();

  DeduplicationKey key1;
  {
    LocalVocabRow tmp = makeLocalVocabRow("x");  // destroyed at end of scope
    auto ctx = tmp.ctx();
    key1 = deduplicator.makeFullTripleKey(allSameVarTriple, 0, ctx);
  }  // `tmp` (and its entries) gone; `key1` must still be valid.

  LocalVocabRow fresh = makeLocalVocabRow("x");
  auto ctx = fresh.ctx();
  // Comparing/hashing `key1` here dereferences its ids; so it must not read
  // freed memory, and `key1` must still equal the equal-string key from a fresh
  // vocab.
  EXPECT_EQ(key1, deduplicator.makeFullTripleKey(allSameVarTriple, 0, ctx));
}

// A constant position contributes its precomputed `dedupId_` to the key
// (rather than reading anything from the row).
TEST_F(ConstructDeduplicationFilter, constantPositionUsesDedupId) {
  ConstructDeduplicator deduplicator = makeGlobalDeduplicator();

  PreprocessedTriple constTriple{PrecomputedConstant{{}, IntId(7)},
                                 PrecomputedConstant{{}, IntId(8)},
                                 PrecomputedConstant{{}, IntId(9)}};
  auto table = makeIdTable(IntId(0));  // no variable positions: row unused
  auto ctx = makeFullBatch(table);

  DeduplicationKey key = deduplicator.makeFullTripleKey(constTriple, 0, ctx);
  EXPECT_EQ(key, (DeduplicationKey{IntId(7), IntId(8), IntId(9)}));
}

// Building a key for a blank-node position is a precondition violation:
// blank-node triples bypass deduplication and never reach `makeFullTripleKey`.
TEST_F(ConstructDeduplicationFilter, blankNodePositionInKeyFails) {
  ConstructDeduplicator deduplicator = makeGlobalDeduplicator();

  PreprocessedTriple blankNodeTriple{PrecomputedBlankNode{"_:g", "_0"},
                                     PrecomputedVariable{0},
                                     PrecomputedVariable{0}};
  auto table = makeIdTable(IntId(1));
  auto ctx = makeFullBatch(table);

  EXPECT_ANY_THROW(deduplicator.makeFullTripleKey(blankNodeTriple, 0, ctx));
}

// `global` dedup collapses the same local-vocab triple across two result
// blocks (each with its own, separately-destroyed `LocalVocab`).
TEST_F(ConstructDeduplicationFilter, dedupAcrossBlocksGlobal) {
  ConstructDeduplicator deduplicator = makeGlobalDeduplicator();
  auto tmpl = makeSingleTripleTemplate();

  {
    LocalVocabRow block1 = makeLocalVocabRow("x");
    auto c1 = block1.ctx();
    EXPECT_TRUE(deduplicator.isNew(0, 0, tmpl, c1));  // first occurrence
  }  // block-1 vocab freed before block-2 is seen

  LocalVocabRow block2 = makeLocalVocabRow("x");
  auto c2 = block2.ctx();
  EXPECT_FALSE(deduplicator.isNew(0, 0, tmpl, c2));  // duplicate across blocks
}

// Same as above, but through the `BatchWise` (LRU) filter path.
TEST_F(ConstructDeduplicationFilter, dedupAcrossBlocksBatchWise) {
  ConstructDeduplicator deduplicator{DeduplicationMode::batchWise(10), *qec_};
  auto tmpl = makeSingleTripleTemplate();

  LocalVocabRow block1 = makeLocalVocabRow("x");
  auto c1 = block1.ctx();
  EXPECT_TRUE(deduplicator.isNew(0, 0, tmpl, c1));

  LocalVocabRow block2 = makeLocalVocabRow("x");
  auto c2 = block2.ctx();
  EXPECT_FALSE(deduplicator.isNew(0, 0, tmpl, c2));
}

// A template triple flagged as containing a blank node is always reported
// "new": blank-node triples bypass deduplication, so no key is built and
// nothing is ever recorded (so a second call is "new" again too).
TEST_F(ConstructDeduplicationFilter, blankNodeTripleAlwaysNew) {
  ConstructDeduplicator state = makeGlobalDeduplicator();
  auto tmpl = makeSingleTripleTemplate();
  tmpl.tripleContainsBlankNode_ = {true};

  auto table = makeIdTable(IntId(1));
  auto ctx = makeFullBatch(table);
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
  EXPECT_TRUE(state.isNew(0, 0, tmpl, ctx));
}

// A ground triple seeded with a local-vocab constant suppresses a later
// non-ground instantiation of the same triple. This exercises `canonicalizeKey`
// (the seed and the non-ground key must agree after reseating).
TEST_F(ConstructDeduplicationFilter, seedGroundTripleSuppressesNonGround) {
  ConstructDeduplicator state = makeGlobalDeduplicator();
  auto tmpl = makeSingleTripleTemplate();

  LocalVocab v1;
  Id x = makeLocalVocabIndex(v1, "x");
  state.seedGroundTriple(DeduplicationKey{x, x, x});

  LocalVocabRow row = makeLocalVocabRow("x");
  auto c = row.ctx();
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c));  // suppressed by the ground seed
}

// In `batchWise` mode a tiny memory threshold forces the filter to drop all
// dedup state once its internal vocab grows past it, which makes deduplication
// approximate: the same local-vocab triple is reported "new" again after the
// reset (rather than a duplicate). Contrast with `dedupAcrossBlocksBatchWise`,
// which uses the default (large) threshold and deduplicates.
TEST_F(ConstructDeduplicationFilter, batchWiseResetsWhenVocabExceedsThreshold) {
  ConstructDeduplicator state{DeduplicationMode::batchWise(10), *qec_,
                              ad_utility::MemorySize::bytes(1)};
  auto tmpl = makeSingleTripleTemplate();

  LocalVocabRow row = makeLocalVocabRow("x");
  auto c = row.ctx();
  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));  // first occurrence
  // The 1-byte threshold was exceeded, so the next call resets the dedup state
  // and the identical triple is treated as new again.
  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));
}

// `global` dedup is exact and is never reset on memory pressure: the vocab
// threshold does not apply to it. So even with a 1-byte threshold the identical
// triple is still recognized as a duplicate.
TEST_F(ConstructDeduplicationFilter, globalIgnoresVocabThreshold) {
  ConstructDeduplicator state =
      makeGlobalDeduplicator(ad_utility::MemorySize::bytes(1));
  auto tmpl = makeSingleTripleTemplate();

  LocalVocabRow row = makeLocalVocabRow("x");
  auto c = row.ctx();
  EXPECT_TRUE(state.isNew(0, 0, tmpl, c));  // first occurrence
  // The tiny threshold is ignored for `global`: no reset, so the identical
  // triple is still a duplicate.
  EXPECT_FALSE(state.isNew(0, 0, tmpl, c));
}

// `TripleDeduplicator` must never be constructed for `none`: the caller creates
// no filter in that mode, so building one is a precondition violation (see the
// `AD_CONTRACT_CHECK` in `TripleDeduplicator::makeDeduplicator`).
TEST_F(ConstructDeduplicationFilter, perTripleFilterRejectsNone) {
  EXPECT_ANY_THROW(TripleDeduplicator(DeduplicationMode::none(), *qec_));
}

// `None` is not modelled by `ConstructDeduplicator`: the caller handles
// it by not constructing the state at all. Constructing it with `None` is a
// precondition violation.
TEST_F(ConstructDeduplicationFilter, noneModeIsRejected) {
  EXPECT_ANY_THROW(ConstructDeduplicator(DeduplicationMode::none(), *qec_));
}

}  // namespace
