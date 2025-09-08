//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
#pragma once

#include "../../test/engine/ValuesForTesting.h"
#include "engine/LocalVocab.h"
#include "util/Cache.h"
#include "util/Synchronized.h"

// A simple thread-safe cache that associates query results with an explicit
// name.
class NamedQueryCache {
 public:
  // The cache value. It stores all the information required to construct a
  // proper `QueryExecutionTree` later on.
  struct Value {
    std::shared_ptr<const IdTable> result_;
    VariableToColumnMap varToColMap_;
    std::vector<ColumnIndex> resultSortedOn_;
    LocalVocab localVocab_;
  };

  // The `ValueSizeGetter` currently is a dummy, as we currently don't limit the
  // size of the explicit cache. In the future we could make the size more
  // accurate and also report statistics about named queries.
  struct ValueSizeGetter {
    ad_utility::MemorySize operator()(const Value&) const {
      return ad_utility::MemorySize::bytes(1);
    }
  };
  using Key = std::string;
  using Cache = ad_utility::LRUCache<Key, Value, ValueSizeGetter>;

 private:
  ad_utility::Synchronized<Cache> cache_;

 public:
  // Store an explicit query result with a given `key`. Previously stored
  // `value`s with the same `key` are overwritten.
  void store(const Key& key, Value value);

  // Erase the explicit query result the the given `key`
  void erase(const Key& key);

  // Clear the cache.
  void clear();

  // Get the number of cached queries.
  size_t numEntries() const;

  // Retrieve the query result that is associated with the `key`.
  // Throw an exception if the `key` doesn't exist.
  std::shared_ptr<const Value> get(const Key& key);

  // Retrieve the query result with the given `key` and convert it into an
  // explicit `ValuesForTesting` operation that can be used as part of a
  // `QueryExecutionTree`.
  std::shared_ptr<ExplicitIdTableOperation> getOperation(
      const Key& key, QueryExecutionContext* ctx);
};
