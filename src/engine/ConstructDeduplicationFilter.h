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

  // Returns true if `key` is new (not a duplicate), false otherwise.
  bool insert(const DeduplicationKey& key) {
    return std::visit(
        OverloadCallOperator{
            [&key](LruDeduplicationCache& lru) { return lru.insert(key); },
            [&key](HashSetWithMemoryLimit<DeduplicationKey>& set) {
              return set.insert(key).second;
            }},
        filter_);
  }

 private:
  using Filter = std::variant<LruDeduplicationCache,
                              HashSetWithMemoryLimit<DeduplicationKey>>;

  Filter filter_;

  // Builds the dedup structure for `mode`: an unbounded hash set for `Global`
  // and a bounded LRU cache for `BatchWise` (capacity = batch size).
  //
  // Precondition: `mode` is not `None`. `None` means "no deduplication", so the
  // caller (`ConstructDeduplicationState`) must not construct a filter for it.
  // This invariant is asserted here rather than handled, to keep it explicit.
  static Filter makeFilter(const DeduplicationMode& mode,
                           const QueryExecutionContext& queryExecutionContext) {
    AD_CONTRACT_CHECK(
        !std::holds_alternative<DeduplicationMode::None>(mode.value_));
    if (std::holds_alternative<DeduplicationMode::Global>(mode.value_)) {
      return HashSetWithMemoryLimit<DeduplicationKey>{
          queryExecutionContext.getAllocator()};
    }
    const auto& bw = std::get<DeduplicationMode::BatchWise>(mode.value_);
    return LruDeduplicationCache{bw.batchSize_};
  }
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
      std::optional<ad_utility::MemorySize> maxDedupVocabSize = std::nullopt)
      : mode_{mode},
        queryExecutionContext_{queryExecutionContext},
        maxDedupVocabBytes_{computeMaxDedupVocabBytes(queryExecutionContext,
                                                      maxDedupVocabSize)} {
    // Only the deduplicating modes get a filter. `None` leaves `filter_` empty
    // (the dedup path is never entered); constructing a `PerTripleFilter` for
    // it is a precondition violation (see `PerTripleFilter::makeFilter`).
    if (!std::holds_alternative<DeduplicationMode::None>(mode.value_)) {
      filter_.emplace(mode, queryExecutionContext);
    }
  }

  // Constructs the deduplication key for the instantiation of `triple` at
  // `absoluteRow`: the `ValueId` at each of the three positions (subject,
  // predicate, object), taken from the constant's `dedupId_` or the variable's
  // bound `ValueId` in the row. Every id is canonicalized into `dedupVocab_` so
  // the key never references a foreign (block/template) `LocalVocab` that may
  // outlive the deduplication filter.
  DeduplicationKey makeFullTripleKey(const PreprocessedTriple& triple,
                                     size_t absoluteRow,
                                     const BatchEvaluationContext& ctx) {
    DeduplicationKey key;
    for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
      key[pos] = canonicalize(
          std::visit(OverloadCallOperator{
                         [](const PrecomputedConstant& c) {
                           AD_CORRECTNESS_CHECK(!c.dedupId_.isUndefined());
                           return c.dedupId_;
                         },
                         [&ctx, absoluteRow](const PrecomputedVariable& v) {
                           return ctx.idTable_[absoluteRow][v.columnIndex_];
                         },
                         [](const PrecomputedBlankNode&) -> ValueId {
                           // Blank-node triples bypass deduplication, so
                           // their key is never built.
                           AD_FAIL();
                         }},
                     triple[pos]));
    }
    return key;
  }

  // Returns true if the instantiation of template triple `tripleIdx` at
  // `absoluteRow` is new (should be emitted), false if it is a duplicate
  // (skip).
  bool isNew(size_t tripleIdx, size_t absoluteRow,
             const PreprocessedConstructTemplate& tmpl,
             const BatchEvaluationContext& ctx) {
    AD_CORRECTNESS_CHECK(filter_.has_value());
    if (tmpl.tripleContainsBlankNode_[tripleIdx]) {
      return true;
    }
    resetIfVocabTooLarge();
    return filter_->insert(makeFullTripleKey(
        tmpl.preprocessedTriples_[tripleIdx], absoluteRow, ctx));
  }

  // Inserts a ground triple's full-triple `key` into the shared filter, so that
  // a later non-ground instantiation of the same triple is suppressed. The key
  // is canonicalized into `dedupVocab_` first, so it matches the keys built by
  // `makeFullTripleKey` (otherwise a ground triple with a local-vocab constant
  // would not suppress its non-ground duplicate). No-op for `none` mode.
  void seedGroundTriple(const DeduplicationKey& key) {
    if (filter_.has_value()) {
      filter_->insert(canonicalizeKey(key));
    }
  }

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
      std::optional<ad_utility::MemorySize> maxDedupVocabSize) {
    return maxDedupVocabSize
        .value_or(queryExecutionContext.getAllocator().amountMemoryLeft() / 4)
        .getBytes();
  }

  // Re-anchor a `LocalVocabIndex` into the `dedupVocab_`, so stored keys never
  // point into a freed block `LocalVocab`, and equal terms from different
  // blocks collapse.
  ValueId canonicalize(ValueId id) {
    if (id.getDatatype() != Datatype::LocalVocabIndex) return id;  // fast path
    const auto& entry = *id.getLocalVocabIndex();
    size_t sizeBefore = dedupVocab_.size();
    auto index = dedupVocab_.getIndexAndAddIfNotContained(entry);
    if (dedupVocab_.size() != sizeBefore) {  // a new string was actually added
      dedupVocabBytes_ += entry.toStringRepresentation().size();
    }
    return ValueId::makeFromLocalVocabIndex(index);
  }

  // Bound `dedupVocab_`s memory: once the accumulated string bytes reach the
  // threshold, either fail (`Global`, which must stay exact) or drop all dedup
  // state and start fresh (`BatchWise`). The filter's keys reference
  // `dedupVocab_`, so both are reset together.
  //
  // NOTE: The `BatchWise` reset makes deduplication approximate (triples seen
  // before a reset may be emitted again). `Global` is exact, so it fails
  // instead. `None` never reaches here (`isNew` asserts a filter exists).
  void resetIfVocabTooLarge() {
    if (dedupVocabBytes_ < maxDedupVocabBytes_) return;
    if (std::holds_alternative<DeduplicationMode::Global>(mode_.value_)) {
      AD_THROW(
          "CONSTRUCT export with `construct-deduplication = global` exceeded "
          "its "
          "memory budget: Reduce the result size, raise the memory limit, or "
          "switch `construct-deduplication` to a `batchwise:<N>` mode (bounded "
          "memory, approximate) or `none` (no deduplication).");
    }
    // Only `BatchWise` reaches here, so rebuilding a filter is always valid.
    filter_.emplace(mode_, queryExecutionContext_);
    dedupVocab_ = LocalVocab();
    dedupVocabBytes_ = 0;
  }

  // Canonicalize every position of a pre-built key into `dedupVocab_`.
  DeduplicationKey canonicalizeKey(DeduplicationKey key) {
    for (ValueId& id : key) {
      id = canonicalize(id);
    }
    return key;
  }
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
