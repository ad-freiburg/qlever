// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
#define QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H

#include <absl/container/inlined_vector.h>

#include <functional>
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

// The cache key used to deduplicate exported CONSTRUCT triples. The key is in
// the ID-space s.t. we can detect duplicates before expensively materializing
// them as strings. See `makeFullTripleKey` for how this key is computed.
using DeduplicationKey = std::array<ValueId, NUM_TRIPLE_POSITIONS>;

using LruDeduplicationCache =
    ad_utility::util::LRUCache<DeduplicationKey, std::monostate>;
using ad_utility::DeduplicationMode;
using ad_utility::HashSetWithMemoryLimit;
using ad_utility::OverloadCallOperator;

// Stores either all unique previously seen CONSTRUCT triples (global mode), or
// just the N last seen triples (batchwise mode). When a new triple is passed to
// the filter (via `insert`), it stores it in the cache. For a previously seen
// triple that is already in the cache, `insert` just informs the caller that
// the triple is a duplicate.
class TripleDeduplicator {
 public:
  explicit TripleDeduplicator(const DeduplicationMode& mode,
                              const QueryExecutionContext& executionContext)
      : deduplicator_{makeDeduplicator(mode, executionContext)} {}

  // Return true if `key` is new (not a duplicate), false otherwise.
  bool insert(const DeduplicationKey& key);

 private:
  using Deduplicator = std::variant<LruDeduplicationCache,
                                    HashSetWithMemoryLimit<DeduplicationKey>>;

  Deduplicator deduplicator_;

  // Build the dedup structure for `mode`: an unbounded hash set for `Global`
  // and a bounded LRU cache for `BatchWise` (capacity = batch size).
  //
  // Precondition: `mode` is not `None`. `None` means "no deduplication", so the
  // caller (`ConstructDeduplicator`) must not construct a filter for it.
  // This invariant is asserted here rather than handled, to keep it explicit.
  static Deduplicator makeDeduplicator(
      const DeduplicationMode& mode,
      const QueryExecutionContext& queryExecutionContext);
};

// Deduplicator for a whole CONSTRUCT clause. It internally holds a
// `TripleDeduplicator` (see above) and only emits triples that are not
// duplicates according to `TripleDeduplicator::insert`. The actual
// deduplication semantics depend on the `DeduplicationMode` (see the
// documentation of `TripleDeduplicator` above).

// NOTE: constructing a deduplicator for `DeduplicationMode::None` will throw,
// so callers have to handle this mode by not instantiating a
// `ConstructDeduplicator` at all.
class ConstructDeduplicator {
 public:
  // `maxDedupVocabSize` bounds the memory of the internal `dedupVocab_`, used
  // for `LocalVocabIndex`es stored in the deduplication cache: if the strings
  // added to it exceed this, all dedup state is dropped (which makes
  // deduplication approximate). Defaults to a quarter of the query's
  // currently available memory.
  //
  // Precondition: `mode` is not `DeduplicationMode::None`. For `None` the
  // caller must not construct this state at all (see the class comment).
  ConstructDeduplicator(
      const DeduplicationMode& mode,
      const QueryExecutionContext& queryExecutionContext,
      std::optional<ad_utility::MemorySize> maxDedupVocabSize = std::nullopt);

  // Construct the deduplication key for the instantiation of `triple` at
  // `absoluteRowIdx`: the `ValueId` at each of the three positions (subject,
  // predicate, object), taken from the constant's `dedupId_` or the variable's
  // bound `ValueId` in the row. Every id is canonicalized into `dedupVocab_` so
  // the key never references a foreign (block/template) `LocalVocab` that may
  // outlive the deduplication filter.
  DeduplicationKey makeFullTripleKey(const PreprocessedTriple& triple,
                                     size_t absoluteRowIdx,
                                     const BatchEvaluationContext& ctx);

  // Return true if the instantiation of template triple `tripleIdx` at
  // `absoluteRowIdx` is new (should be emitted), false if it is a duplicate
  // (skip).
  bool isNew(size_t tripleIdx, size_t absoluteRowIdx,
             const PreprocessedConstructTemplate& tmpl,
             const BatchEvaluationContext& ctx);

  // Insert a ground triple's full-triple `key` into the shared filter, so that
  // a later non-ground instantiation of the same triple is suppressed. The key
  // is canonicalized into `dedupVocab_` first, so it matches the keys built by
  // `makeFullTripleKey` (otherwise a ground triple with a local-vocab constant
  // would not suppress its non-ground duplicate).
  void seedGroundTriple(const DeduplicationKey& key);

 private:
  // Stored by value (not reference): callers may pass a temporary `mode`, and
  // `mode_` is read later in `resetIfVocabTooLarge`.
  const DeduplicationMode mode_;
  std::reference_wrapper<const QueryExecutionContext> queryExecutionContext_;
  // Approximate total byte size of the strings currently held in `dedupVocab_`.
  size_t dedupVocabBytes_ = 0;
  // When `dedupVocabBytes_` reaches this, all dedup state is dropped (see
  // `resetIfVocabTooLarge`) to bound the vocab's memory. Set from the query's
  // available memory at construction (see the constructor).
  const size_t maxDedupVocabBytes_;

  // owns every local-vocab entry referenced by a stored key
  LocalVocab dedupVocab_;

  // The single shared filter. Always present: `None` mode is handled by not
  // constructing this state (see the class comment).
  TripleDeduplicator filter_;

  // Compute the byte threshold for `dedupVocab_`: the explicit
  // `maxDedupVocabSize` if given, else a mode-dependent default (batch-size-
  // relative for `BatchWise`, a quarter of available memory for `Global`; see
  // the definition).
  static size_t computeMaxDedupVocabBytes(
      const DeduplicationMode& mode,
      const QueryExecutionContext& queryExecutionContext,
      std::optional<ad_utility::MemorySize> maxDedupVocabSize);

  // Re-anchor a `LocalVocabIndex` into the `dedupVocab_`, so stored keys never
  // point into a freed block `LocalVocab`, and equal terms from different
  // blocks collapse.
  ValueId canonicalize(ValueId id);

  // Canonicalize every position of a pre-built key into `dedupVocab_`.
  DeduplicationKey canonicalizeKey(DeduplicationKey key);

  // Bound `dedupVocab_`s memory in `BatchWise` mode: once the accumulated
  // string bytes reach the threshold, drop all dedup state and start fresh. The
  // filter's keys reference `dedupVocab_`, so both are reset together.
  //
  // NOTE: This reset makes `BatchWise` deduplication approximate (triples seen
  // before a reset may be emitted again). `Global` must stay exact, so it is
  // never reset here.
  void resetIfVocabTooLarge();
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
