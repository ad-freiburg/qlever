// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H
#define QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H

#include <boost/optional.hpp>

#include "engine/ExplicitIdTableOperation.h"
#include "engine/LocalVocab.h"
#include "engine/SpatialJoinCachedIndex.h"
#include "util/Cache.h"
#include "util/Serializer/Serializer.h"
#include "util/Synchronized.h"

// Forward declarations
class QueryExecutionContext;

// A simple thread-safe cache that caches query results with an explicit
// name.
class NamedResultCache {
 public:
  // The cached result. In addition to the `IdTable` of the result, also
  // store all the information required to construct a `QueryExecutionTree`.
  // The cache key of the root operation used to generate this result is kept to
  // be included in the cache key of operations using this result. Optionally, a
  // geometry index `cachedGeoIndex_` can be precomputed on a column of the
  // result table for spatial joins with a constant (right) child.
  struct Value {
    std::shared_ptr<const IdTable> result_;
    VariableToColumnMap varToColMap_;
    std::vector<ColumnIndex> resultSortedOn_;
    LocalVocab localVocab_;
    std::string cacheKey_;
    std::optional<SpatialJoinCachedIndex> cachedGeoIndex_;

    // The following two members (`Allocator` and `BlankNodeManager`) are only
    // used when reading a `Value` from a serializer.
    using Allocator = ad_utility::AllocatorWithLimit<Id>;
    std::optional<Allocator> allocatorForSerialization_{std::nullopt};
    boost::optional<ad_utility::BlankNodeManager&>
        blankNodeManagerForSerialization_{boost::none};
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
  // The `cache_` has a non-const `operator[]` which is non-const because it has
  // to update data structures for the `LRU` mechanism. That's why we
  // unfortunately have to use `mutable` here. We get threadsafety via the
  // `Synchronized` wrapper, and manually have to make sure that we logically
  // don't violate the constness.
  mutable ad_utility::Synchronized<Cache> cache_;

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
  std::shared_ptr<const Value> get(const Key& name) const;

  // Get a pointer to the cached result with the given `name`, and convert
  // it into an `ExplicitIdTableOperation` that can be used as part of a
  // `QueryExecutionTree`.
  std::shared_ptr<ExplicitIdTableOperation> getOperation(
      const Key& name, QueryExecutionContext* qec);

  // NOTE: The following two templated serialization functions are defined in
  // the `NamedResultCacheSerializer.h` header which has to be included by the
  // code that actually calls them to not get any undefined references.

  // Write the current contents of the result cache to the `serializer`.
  CPP_template(typename Serializer)(
      requires ad_utility::serialization::WriteSerializer<
          Serializer>) void writeToSerializer(Serializer& serializer) const;

  // Read the contents of the result cache from the `serializer`.
  // NOTE: This function has to be called after the index has been loaded, but
  // before any queries are executed, because of the deserialization of possible
  // blank nodes in the cache entries. In particular, if the serialized cache
  // contains a local blank node, and the `blankNodeManager` already has handed
  // out randomly allocated blank nodes, an `AD_CORRECTNESS_CHECK` will fail.
  CPP_template(typename Serializer)(
      requires ad_utility::serialization::ReadSerializer<
          Serializer>) void readFromSerializer(Serializer& serializer,
                                               Value::Allocator allocator,
                                               ad_utility::BlankNodeManager&
                                                   blankNodeManager);
};

#endif  // QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H
