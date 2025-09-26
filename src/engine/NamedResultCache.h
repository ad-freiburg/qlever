// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H
#define QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H

#include "engine/ExplicitIdTableOperation.h"
#include "engine/LocalVocab.h"
#include "util/Cache.h"
#include "util/Synchronized.h"

// A simple thread-safe cache that caches query results with an explicit
// name.
class NamedResultCache {
 public:
  // The cached result. In addition to the `IdTable` of the result, also
  // store all the information required to construct a `QueryExecutionTree`.
  struct Value {
    std::shared_ptr<const IdTable> result_;
    VariableToColumnMap varToColMap_;
    std::vector<ColumnIndex> resultSortedOn_;
    LocalVocab localVocab_;
  };

  // The size of a cached result, which currently is just a dummy value of 1,
  //
  // TODO: Return the actual size of the cached result, or an approximation,
  // and have a limit on the total memory used by the cache.
  struct ValueSizeGetter {
    ad_utility::MemorySize operator()(const Value&) const {
      return ad_utility::MemorySize::bytes(1);
    }
  };

  // We use an LRU cache, where the key is the name of the cached result.
  using Key = std::string;
  using Cache = ad_utility::LRUCache<Key, Value, ValueSizeGetter>;

 private:
  ad_utility::Synchronized<Cache> cache_;

 public:
  // Store the given `result` under the given `name`. If a result with the same
  // name already exists, it is overwritten.
  void store(const Key& name, Value result);

  // Erase the result with the given `name` from the cache. If no such result
  // exists, do nothing.
  void erase(const Key& name);

  // Clear the cache.
  void clear();

  // Get the number of cached results.
  size_t numEntries() const;

  // Get a pointer to the cached result with the given `name`. If no such
  // result exists, throw an exception.
  std::shared_ptr<const Value> get(const Key& name);

  // Get a pointer to the cached result with the given `name`, and convert
  // it into an `ExplicitIdTableOperation` that can be used as part of a
  // `QueryExecutionTree`.
  std::shared_ptr<ExplicitIdTableOperation> getOperation(
      const Key& key, QueryExecutionContext* ctx);
};

#endif  // QLEVER_SRC_ENGINE_NAMEDQUERYCACHE_H
