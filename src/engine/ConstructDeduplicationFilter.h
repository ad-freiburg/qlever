// Copyright 2026 The QLever Authors, in particular:
// 2026 Marvin Stoetzel <marvin.stoetzel@email.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H
#define QLEVER_SRC_ENGINE_CONSTRUCTDEDUPLICATIONFILTER_H

#include <absl/container/flat_hash_map.h>

#include <array>
#include <list>
#include <vector>

#include "engine/ConstructBatchEvaluator.h"
#include "engine/ConstructTypes.h"
#include "global/ValueId.h"
#include "util/ConstructDeduplicationMode.h"
#include "util/HashSet.h"
#include "util/OverloadCallOperator.h"

namespace qlever::constructExport {

// The deduplication key for a single template triple instantiation. Each
// position corresponds to subject, predicate, or object. Variable positions
// hold the bound `ValueId` from the result row; constant and blank node
// positions hold `ValueId::makeUndefined()` as a sentinel (constants are
// identical in every row so they do not contribute to uniqueness; blank node
// identifiers are generated fresh per row so including them would make every
// key unique, defeating deduplication).
using DeduplicationKey = std::array<ValueId, NUM_TRIPLE_POSITIONS>;

// Constructs the deduplication key for the template triple at `tripleIdx` and
// the result row at `absoluteRow` in `ctx`. Variable positions (given by
// `variableColumns`) are filled with the corresponding `ValueId` from the
// IdTable; all other positions are filled with `ValueId::makeUndefined()`.
inline DeduplicationKey makeDeduplicationKey(
    const std::vector<size_t>& variableColumns, size_t absoluteRow,
    const BatchEvaluationContext& ctx) {
  DeduplicationKey key;
  key.fill(ValueId::makeUndefined());
  for (size_t i = 0; i < variableColumns.size(); ++i) {
    key[i] = ctx.idTable_[absoluteRow][variableColumns[i]];
  }
  return key;
}

// A set-like LRU cache for `DeduplicationKey`s. Tracks the N most recently
// inserted unique keys. Used as a fast-path guard in `global` mode and as the
// sole dedup structure in `lru` mode.
//
// Internal structure: a doubly-linked list maintains insertion/access order
// (front = most recently used, back = least recently used); an
// `absl::flat_hash_map` maps each key to its list iterator for O(1) lookup,
// insertion, and eviction.
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
  std::list<DeduplicationKey> list_;
  absl::flat_hash_map<DeduplicationKey, std::list<DeduplicationKey>::iterator>
      map_;
};

// Per-template-triple dedup filter. In `lru` mode this is just the LRU cache.
// In `global` mode the LRU cache acts as a fast-path guard in front of an
// unbounded hash set: a key that misses the LRU is looked up in the hash set
// before being declared new.
class PerTripleFilter {
 public:
  explicit PerTripleFilter(const ad_utility::DeduplicationMode& mode)
      : isGlobal_{std::holds_alternative<ad_utility::DeduplicationMode::Global>(
            mode.value)},
        lru_{lruCapacityFromMode(mode)} {}

  // Returns true if `key` is new (not a duplicate), false otherwise.
  // Dispatches to the appropriate implementation based on the mode passed at
  // construction time.
  bool insert(const DeduplicationKey& key) {
    return isGlobal_ ? insertGlobal(key) : insertLru(key);
  }

 private:
  bool isGlobal_;

  // Fast-path LRU cache. Always present.
  LruDeduplicationCache lru_;

  // Overflow hash set for `global` mode. Empty and unused in `lru` mode.
  ad_utility::HashSet<DeduplicationKey> globalSet_;

  // Returns the LRU capacity for a given mode.
  static size_t lruCapacityFromMode(const ad_utility::DeduplicationMode& mode) {
    constexpr size_t DEFAULT_LRU_CAPACITY = 1000;
    return std::visit(
        ad_utility::OverloadCallOperator{
            [](const ad_utility::DeduplicationMode::None&) -> size_t {
              // Unused, but LruDeduplicationCache requires capacity > 0.
              return 1;
            },
            [](const ad_utility::DeduplicationMode::Global&) -> size_t {
              return DEFAULT_LRU_CAPACITY;
            },
            [](const ad_utility::DeduplicationMode::BatchWise& bw) -> size_t {
              return bw.batchSize;
            }},
        mode.value);
  }

  bool insertLru(const DeduplicationKey& key) { return lru_.insert(key); }

  bool insertGlobal(const DeduplicationKey& key) {
    if (!lru_.insert(key)) return false;
    // LRU returned true (key is new to the cache), but the key may have been
    // previously evicted from the LRU and already exist in the global set.
    return globalSet_.insert(key).second;
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
