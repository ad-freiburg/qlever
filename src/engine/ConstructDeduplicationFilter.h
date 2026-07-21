// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
#define QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H

#include <absl/container/inlined_vector.h>

#include <optional>
#include <variant>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "engine/QueryExecutionContext.h"
#include "global/ValueId.h"
#include "util/ConstructDeduplicationMode.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/LruCache.h"
#include "util/OverloadCallOperator.h"

namespace qlever::constructExport {

// The deduplication key identifying one instantiated triple: the `ValueId`s at
// its three positions (subject, predicate, object), in order. Constants
// contribute their precomputed `dedupId_`, variables their bound `ValueId` from
// the row (see `makeFullTripleKey`).
using DeduplicationKey = std::array<ValueId, NUM_TRIPLE_POSITIONS>;

using LruDeduplicationCache =
    ad_utility::util::LRUCache<DeduplicationKey, std::monostate>;
using ad_utility::DeduplicationMode;
using ad_utility::HashSetWithMemoryLimit;
using ad_utility::OverloadCallOperator;

// Per-template-triple dedup filter for the deduplicating modes only.
// `BatchWise` keeps only the last N unique keys in a bounded LRU cache;
// `Global` keeps every unique key in an unbounded hash set. `None` never
// reaches this class: it is handled by the caller.
class PerTripleFilter {
 public:
  explicit PerTripleFilter(const DeduplicationMode& mode,
                           const QueryExecutionContext& executionContext)
      : filter_{makeFilter(mode, executionContext)} {}

  // Return true if `key` is new (not a duplicate), false otherwise.
  bool insert(const DeduplicationKey& key);

 private:
  using Filter = std::variant<LruDeduplicationCache,
                              HashSetWithMemoryLimit<DeduplicationKey>>;

  Filter filter_;

  // Build the dedup structure for `mode`: an unbounded hash set for `Global`
  // and a bounded LRU cache for `BatchWise` (capacity = batch size).
  //
  // Precondition: `mode` is not `None`. `None` means "no deduplication", so the
  // caller (`ConstructDeduplicationState`) must not construct a filter for it.
  // This invariant is asserted here rather than handled, to keep it explicit.
  static Filter makeFilter(const DeduplicationMode& mode,
                           const QueryExecutionContext& queryExecutionContext);
};

// Deduplication state for a whole CONSTRUCT clause. In every deduplicating mode
// there is one filter, shared by all template triples and keyed on the
// instantiated triple. This is what makes cross-template-triple duplicates
// collapse (the same output triple produced by two different template triples
// is emitted once). The modes differ only in the backing structure of that
// single filter:
// - `DeduplicationMode::Global`: an unbounded hash set (exact deduplication).
// - `DeduplicationMode::BatchWise`: a bounded LRU cache (a bounded
// approximation that only remembers the most recent keys).
// - `DeduplicationMode::None`: no filter at all; the dedup code path is never
// entered.
class ConstructDeduplicationState {
 public:
  // `maxDedupVocabSize` bounds the memory of the internal `dedupVocab_`: once
  // the strings added to it exceed this, all dedup state is dropped (which
  // makes deduplication approximate). Defaults to a quarter of the query's
  // currently available memory. Tests may pass a tiny value to force a reset.
  ConstructDeduplicationState(
      const DeduplicationMode& mode,
      const QueryExecutionContext& queryExecutionContext,
      std::optional<ad_utility::MemorySize> maxDedupVocabSize = std::nullopt);

  // Constructs the deduplication key for the instantiation of `triple` at
  // `absoluteRow`: the `ValueId` at each of the three positions (subject,
  // predicate, object), taken from the constant's `dedupId_` or the variable's
  // bound `ValueId` in the row. Every id is canonicalized into `dedupVocab_` so
  // the key never references a foreign (block/template) `LocalVocab` that may
  // outlive the deduplication filter.
  DeduplicationKey makeFullTripleKey(const PreprocessedTriple& triple,
                                     size_t absoluteRow,
                                     const BatchEvaluationContext& ctx);

  // Returns true if the instantiation of template triple `tripleIdx` at
  // `absoluteRow` is new (should be emitted), false if it is a duplicate
  // (skip).
  bool isNew(size_t tripleIdx, size_t absoluteRow,
             const PreprocessedConstructTemplate& tmpl,
             const BatchEvaluationContext& ctx);

  // Inserts a ground triple's full-triple `key` into the shared filter, so that
  // a later non-ground instantiation of the same triple is suppressed. The key
  // is canonicalized into `dedupVocab_` first, so it matches the keys built by
  // `makeFullTripleKey` (otherwise a ground triple with a local-vocab constant
  // would not suppress its non-ground duplicate). No-op for `none` mode.
  void seedGroundTriple(const DeduplicationKey& key);

 private:
  // Stored by value (not reference): callers may pass a temporary `mode`, and
  // `mode_` is read later in `resetIfVocabTooLarge`.
  const DeduplicationMode mode_;
  const QueryExecutionContext& queryExecutionContext_;
  // Approximate total byte size of the strings currently held in `dedupVocab_`.
  size_t dedupVocabBytes_ = 0;
  // When `dedupVocabBytes_` reaches this, all dedup state is dropped (see
  // `resetIfVocabTooLarge`) to bound the vocab's memory. Set from the query's
  // available memory at construction (see the constructor).
  const size_t maxDedupVocabBytes_;

  // owns every local-vocab entry referenced by a stored key
  LocalVocab dedupVocab_;

  // The single shared filter, or `nullopt` for `none` mode (never consulted).
  std::optional<PerTripleFilter> filter_;

  // The byte threshold for `dedupVocab_`: the explicit `maxDedupVocabSize` if
  // given, else a quarter of the query's currently available memory.
  static size_t computeMaxDedupVocabBytes(
      const QueryExecutionContext& queryExecutionContext,
      std::optional<ad_utility::MemorySize> maxDedupVocabSize);

  // Re-anchor a `LocalVocabIndex` into the `dedupVocab_`, so stored keys never
  // point into a freed block `LocalVocab`, and equal terms from different
  // blocks collapse.
  ValueId canonicalize(ValueId id);

  // Bound `dedupVocab_`s memory: once the accumulated string bytes reach the
  // threshold, either fail (`Global`, which must stay exact) or drop all dedup
  // state and start fresh (`BatchWise`). The filter's keys reference
  // `dedupVocab_`, so both are reset together.
  //
  // NOTE: The `BatchWise` reset makes deduplication approximate (triples seen
  // before a reset may be emitted again). `Global` is exact, so it fails
  // instead. `None` never reaches here (`isNew` asserts a filter exists).
  void resetIfVocabTooLarge();

  // Canonicalize every position of a pre-built key into `dedupVocab_`.
  DeduplicationKey canonicalizeKey(DeduplicationKey key);
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
