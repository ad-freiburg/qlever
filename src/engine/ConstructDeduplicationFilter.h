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
// the row (see `makeFullTripleKey`). A single shared filter can collapse
// duplicates produced by different template triples (see
// `ConstructDeduplicationState`). Triples containing a blank node are never
// keyed: their per-row blank-node ids would make every key unique, so such
// triples bypass deduplication entirely.
using DeduplicationKey = std::array<ValueId, NUM_TRIPLE_POSITIONS>;

using LruDeduplicationCache =
    ad_utility::util::LRUCache<DeduplicationKey, std::monostate>;

// Per-template-triple dedup filter. `BatchWise` mode keeps only the last N
// unique keys in a bounded LRU cache; `Global` mode keeps every unique key in
// an unbounded hash set; `None` mode holds no structure at all (it is routed to
// the non-dedup code path and never deduplicates). The active alternative is
// chosen once from the mode at construction time.
class PerTripleFilter {
 public:
  explicit PerTripleFilter(const ad_utility::DeduplicationMode& mode)
      : filter_{makeFilter(mode)} {}

  // Returns true if `key` is new (not a duplicate), false otherwise.
  bool insert(const DeduplicationKey& key) {
    return std::visit(
        ad_utility::OverloadCallOperator{
            [](NoFilter) -> bool {
              // `DeduplicationMode::None` mode is routed to the non-dedup code
              // path, so its filter must never receive a key.
              AD_FAIL();
            },
            [&key](LruDeduplicationCache& lru) { return lru.insert(key); },
            [&key](ad_utility::HashSet<DeduplicationKey>& set) {
              return set.insert(key).second;
            }},
        filter_);
  }

 private:
  struct NoFilter {};
  using Filter = std::variant<NoFilter, LruDeduplicationCache,
                              ad_utility::HashSet<DeduplicationKey>>;

  Filter filter_;

  // Builds the dedup structure for `mode`: an empty `NoFilter` for `none`, a
  // bounded LRU cache for `BatchWise` (capacity = batch size), and an unbounded
  // hash set for `global`.
  static Filter makeFilter(const ad_utility::DeduplicationMode& mode) {
    return std::visit(
        ad_utility::OverloadCallOperator{
            [](const ad_utility::DeduplicationMode::None&) -> Filter {
              return NoFilter{};
            },
            [](const ad_utility::DeduplicationMode::Global&) -> Filter {
              return ad_utility::HashSet<DeduplicationKey>{};
            },
            [](const ad_utility::DeduplicationMode::BatchWise& bw) -> Filter {
              return LruDeduplicationCache{bw.batchSize_};
            }},
        mode.value_);
  }
};

// Constructs the *full-triple* deduplication key for the instantiation of
// `triple` at `absoluteRow`: the `ValueId` at each of the three positions
// (subject, predicate, object), taken from the constant's `dedupId_` or the
// variable's bound `ValueId` in the row. Must not be called for a triple that
// contains a blank node (those bypass deduplication entirely).
inline DeduplicationKey makeFullTripleKey(const PreprocessedTriple& triple,
                                          size_t absoluteRow,
                                          const BatchEvaluationContext& ctx) {
  DeduplicationKey key;
  for (size_t pos = 0; pos < NUM_TRIPLE_POSITIONS; ++pos) {
    key[pos] =
        std::visit(ad_utility::OverloadCallOperator{
                       [](const PrecomputedConstant& c) {
                         AD_CORRECTNESS_CHECK(!c.dedupId_.isUndefined());
                         return c.dedupId_;
                       },
                       [&ctx, absoluteRow](const PrecomputedVariable& v) {
                         return ctx.idTable_[absoluteRow][v.columnIndex_];
                       },
                       [](const PrecomputedBlankNode&) -> ValueId {
                         // Blank-node triples bypass deduplication, so their
                         // key is never built.
                         AD_FAIL();
                       }},
                   triple[pos]);
  }
  return key;
}

// Deduplication state for a whole CONSTRUCT clause. In every deduplicating mode
// there is one filter, shared by all template triples and keyed on the
// instantiated triple. This is what makes cross-template duplicates collapse
// (the same output triple produced by two different template triples is emitted
// once). The modes differ only in the backing structure of that single filter:
// - `DeduplicationMode::Global`: an unbounded hash set (exact deduplication).
// - `DeduplicationMode::BatchWise`: a bounded LRU cache (a bounded
// approximation that only remembers the most recent keys).
// - `DeduplicationMode::None`: no filter at all; the dedup code path is never
// entered.
// In all modes, a triple that contains a blank node is never deduplicated (its
// per-row blank-node id makes every instantiation distinct).
class ConstructDeduplicationState {
 public:
  ConstructDeduplicationState(const ad_utility::DeduplicationMode& mode)
      : filter_{makeFilter(mode)} {}

  // Returns true if the instantiation of template triple `tripleIdx` at
  // `absoluteRow` is new (should be emitted), false if it is a duplicate
  // (skip). Triples containing a blank node are always "new". `none` mode must
  // never reach here.
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
  // a later non-ground instantiation of the same triple is suppressed. No-op
  // for `none` mode.
  void seedGroundTriple(const DeduplicationKey& key) {
    if (filter_.has_value()) {
      filter_->insert(key);
    }
  }

 private:
  // The single shared filter, or `nullopt` for `none` mode (never consulted).
  std::optional<PerTripleFilter> filter_;

  static std::optional<PerTripleFilter> makeFilter(
      const ad_utility::DeduplicationMode& mode) {
    if (std::holds_alternative<ad_utility::DeduplicationMode::None>(
            mode.value_)) {
      return std::nullopt;
    }
    return PerTripleFilter{mode};
  }
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
