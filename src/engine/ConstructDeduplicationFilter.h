// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
#define QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <list>
#include <optional>
#include <variant>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "global/ValueId.h"
#include "util/ConstructDeduplicationMode.h"
#include "util/Exception.h"
#include "util/HashSet.h"
#include "util/OverloadCallOperator.h"

namespace qlever::constructExport {

// The deduplication key for a single template triple instantiation: the bound
// `ValueId`s at the triple's variable columns, in the order the variables
// appear in the triple (subject, then predicate, then object). Only variables
// are included; constants and blank nodes are deliberately excluded (constants
// are identical for every variable binding set, so they do not contribute to
// uniqueness; blank node identifiers are generated fresh per row, so including
// them would make every key unique, defeating deduplication). The variable's
// position within the triple is not encoded, because each template triple has
// its own filter (see `ConstructDeduplicationState`) and keys from different
// triples are never compared against each other.
using DeduplicationKey = absl::InlinedVector<ValueId, NUM_TRIPLE_POSITIONS>;

// Constructs the deduplication key for the result row at `absoluteRow` in
// `ctx` by reading the `ValueId` at each of the triple's `variableColumns`.
inline DeduplicationKey makeDeduplicationKey(
    const std::vector<size_t>& variableColumns, size_t absoluteRow,
    const BatchEvaluationContext& ctx) {
  DeduplicationKey key;
  key.reserve(variableColumns.size());
  for (size_t column : variableColumns) {
    key.push_back(ctx.idTable_[absoluteRow][column]);
  }
  return key;
}

// A set-like LRU cache for `DeduplicationKey`s. Tracks the N most recently
// inserted unique keys.
class LruDeduplicationCache {
 public:
  explicit LruDeduplicationCache(size_t capacity) : capacity_{capacity} {}

  // Returns true if `key` was not previously present and inserts it.
  // Returns false if `key` was already present (duplicate).
  // If the cache is full, the least recently used entry is evicted first.
  bool insert(const DeduplicationKey& key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
      // Key already present: move to front (most recently used).
      list_.splice(list_.begin(), list_, it->second);
      return false;
    }
    // Key is new: evict LRU entry if at capacity.
    if (map_.size() >= capacity_) {
      map_.erase(list_.back());
      list_.pop_back();
    }
    list_.push_front(key);
    map_.emplace(key, list_.begin());
    return true;
  }

 private:
  size_t capacity_;

  // Recency order of the keys: front = most recently used, back = least
  // recently used. A `std::list` is used because moving a node to the front
  // (on a hit) and popping the back (on eviction) are both O(1), and because
  // its iterators stay valid as other nodes are inserted, moved, or erased.
  std::list<DeduplicationKey> list_;

  // Maps each key to the iterator of its node in `list_`. The map answers
  // "is this key present?" in O(1); the stored iterator then tells us where
  // that key lives in `list_` so we can splice it to the front (on a hit)
  // without an O(N) search. The key is therefore deliberately stored twice
  // (once as a list element, once as a map key): that redundancy is what lets
  // lookup, recency update, and eviction all be O(1).
  absl::flat_hash_map<DeduplicationKey, std::list<DeduplicationKey>::iterator>
      map_;
};

// Per-template-triple dedup filter. `BatchWise` mode keeps only the last N
// unique keys in a bounded LRU cache; `global` mode keeps every unique key in
// an unbounded hash set; `none` mode holds no structure at all (it is routed to
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
              // `none` mode is routed to the non-dedup code path, so its filter
              // must never receive a key.
              AD_FAIL();
            },
            [&key](LruDeduplicationCache& lru) { return lru.insert(key); },
            [&key](ad_utility::HashSet<DeduplicationKey>& set) {
              return set.insert(key).second;
            }},
        filter_);
  }

 private:
  // `NoFilter` represents `none` mode (no dedup structure).
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
        mode.value);
  }
};

// Constructs the *full-triple* deduplication key for the instantiation of
// `triple` at `absoluteRow`: the `ValueId` at each of the three positions
// (subject, predicate, object), taken from the constant's `dedupId_` or the
// variable's bound `ValueId` in the row. Unlike `makeDeduplicationKey` (which
// only includes variable positions and is per-template-triple), this key
// identifies the whole instantiated triple, so it can be compared across all
// template triples in a single shared filter. Must not be called for a triple
// that contains a blank node (those bypass deduplication entirely).
inline DeduplicationKey makeFullTripleKey(const PreprocessedTriple& triple,
                                          size_t absoluteRow,
                                          const BatchEvaluationContext& ctx) {
  DeduplicationKey key;
  key.reserve(NUM_TRIPLE_POSITIONS);
  for (const PreprocessedTerm& term : triple) {
    key.push_back(std::visit(
        ad_utility::OverloadCallOperator{
            [](const PrecomputedConstant& c) -> ValueId { return c.dedupId_; },
            [&](const PrecomputedVariable& v) -> ValueId {
              return ctx.idTable_[absoluteRow][v.columnIndex_];
            },
            [](const PrecomputedBlankNode&) -> ValueId {
              // Blank-node triples bypass deduplication, so their key is never
              // built.
              AD_FAIL();
            }},
        term));
  }
  return key;
}

// Deduplication state for a whole CONSTRUCT clause. In every deduplicating mode
// there is exactly *one* filter, shared by all template triples and keyed on
// the *full* instantiated triple. This is what makes cross-template duplicates
// collapse (the same output triple produced by two different template triples
// is emitted once). The modes differ only in the backing structure of that
// single filter:
//   - `DeduplicationMode::Global`: an unbounded hash set (exact deduplication).
//   - `DeduplicationMode::BatchWise`: a bounded LRU cache (a bounded
//     approximation that only remembers the most recent keys).
//   - `DeduplicationMode::None`: no filter at all; the dedup code path is never
//     entered.
// In all modes, a triple that contains a blank node is never deduplicated (its
// per-row blank-node id makes every instantiation distinct).
class ConstructDeduplicationState {
 public:
  ConstructDeduplicationState(const ad_utility::DeduplicationMode& mode,
                              [[maybe_unused]] size_t numTriples)
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
            mode.value)) {
      return std::nullopt;
    }
    return PerTripleFilter{mode};
  }
};

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
