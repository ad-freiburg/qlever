//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
#pragma once

#include "engine/ValuesForTesting.h"
#include "util/Synchronized.h"

// A simple threadsafe cache that associates query results with an explicit
// name.
class NamedQueryCache {
 public:
  // The cache value. It stores all the information required to construct a
  // proper `QueryExecutionTree` later on.
  struct Value {
    IdTable result_;
    VariableToColumnMap varToColMap_;
    std::vector<ColumnIndex> resultSortedOn_;
  };
  using Key = std::string;
  using Cache = ad_utility::HashMap<std::string, Value>;

 private:
  ad_utility::Synchronized<Cache> cache_;

 public:
  // Store an explicit query result with a given `key`. Previously stored
  // `value`s with the same `key` are overwritten.
  void store(const Key& key, Value value);

  // Clear the cache.
  void clear();

  // Retrieve the query result that is associated with the `key`.
  // Throw an exception if the `key` doesn't exist.
  const Value& get(const Key& key) const;

  // Retrieve the query result with the given `key` and convert it into an
  // explicit `ValuesForTesting` operation that can be used as part of a
  // `QueryExecutionTree`.
  // TODO<joka921> This can be done more efficiently if we implement a dedicated
  // operation for this use case, `ValuesForTesting` currently incurs one
  // (unneeded) copy per query execution.
  std::shared_ptr<ValuesForTesting> getOperation(
      const Key& key, QueryExecutionContext* ctx) const;
};
