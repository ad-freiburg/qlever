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

  // Builds the dedup structure for `mode`: a bounded LRU cache for `BatchWise`
  // (capacity = batch size) and an unbounded hash set for `Global`. `None` is a
  // precondition violation here; the caller must not construct a filter for it.
  static Filter makeFilter(const DeduplicationMode& mode,
                           const QueryExecutionContext& queryExecutionContext) {
    AD_CONTRACT_CHECK(
        std::holds_alternative<DeduplicationMode::None>(mode.value_));
    return std::visit(OverloadCallOperator{
                          [](const DeduplicationMode::None&) -> Filter {
                            // `None` is handled by the caller (no filter is
                            // created), so it must never reach
                            // `PerTripleFilter`.
                            AD_FAIL();
                          },
                          [&queryExecutionContext](
                              const DeduplicationMode::Global&) -> Filter {
                            return HashSetWithMemoryLimit<DeduplicationKey>{
                                queryExecutionContext.getAllocator()};
                          },
                          [](const DeduplicationMode::BatchWise& bw) -> Filter {
                            return LruDeduplicationCache{bw.batchSize_};
                          }},
                      mode.value_);
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
  ConstructDeduplicationState(
      const DeduplicationMode& mode,
      const QueryExecutionContext& queryExecutionContext)
      : filter_{makeFilter(mode, queryExecutionContext)} {}

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
  // owns every local-vocab entry referenced by a stored key
  LocalVocab dedupVocab_;

  // The single shared filter, or `nullopt` for `none` mode (never consulted).
  std::optional<PerTripleFilter> filter_;

  static std::optional<PerTripleFilter> makeFilter(
      const DeduplicationMode& mode,
      const QueryExecutionContext& queryExecutionContext) {
    if (std::holds_alternative<DeduplicationMode::None>(mode.value_)) {
      return std::nullopt;
    }
    return PerTripleFilter{mode, queryExecutionContext};
  }

  // Re-anchor a `LocalVocabIndex` into the `dedupVocab_`, so stored keys never
  // point into a freed block `LocalVocab`, and equal terms from different
  // blocks collapse.
  ValueId canonicalize(ValueId id) {
    if (id.getDatatype() != Datatype::LocalVocabIndex) return id;  // fast path
    return ValueId::makeFromLocalVocabIndex(
        dedupVocab_.getIndexAndAddIfNotContained(*id.getLocalVocabIndex()));
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
