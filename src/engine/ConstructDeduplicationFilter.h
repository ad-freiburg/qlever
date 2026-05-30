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
              return LruDeduplicationCache{bw.batchSize};
            }},
        mode.value);
  }
};

// One `PerTripleFilter` per template triple.
using ConstructDeduplicationState = std::vector<PerTripleFilter>;

// Constructs the initial `ConstructDeduplicationState` for a given mode and
// number of template triples. Each `PerTripleFilter` is initialized with the
// mode so it can derive its LRU capacity and dispatch strategy internally.
inline ConstructDeduplicationState makeDeduplicationState(
    const ad_utility::DeduplicationMode& mode, size_t numTriples) {
  return ConstructDeduplicationState(numTriples, PerTripleFilter{mode});
}

}  // namespace qlever::constructExport

#endif  // QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
