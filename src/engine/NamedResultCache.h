// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#ifndef QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H
#define QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H

#include "engine/ExplicitIdTableOperation.h"
#include "engine/LocalVocab.h"
#include "engine/SpatialJoinCachedIndex.h"
#include "util/Cache.h"
#include "util/Serializer/SerializeHashMap.h"
#include "util/Serializer/SerializeOptional.h"
#include "util/Serializer/SerializeString.h"
#include "util/Serializer/SerializeVector.h"
#include "util/Serializer/Serializer.h"
#include "util/Serializer/TripleSerializer.h"
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
    // This allocator is only used during the `readFromDisk` member function.
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
  std::shared_ptr<const Value> get(const Key& name);

  // Get a pointer to the cached result with the given `name`, and convert
  // it into an `ExplicitIdTableOperation` that can be used as part of a
  // `QueryExecutionTree`.
  std::shared_ptr<ExplicitIdTableOperation> getOperation(
      const Key& name, QueryExecutionContext* qec);

  // _____________________________________________________________________________
  template <typename Serializer>
  void writeToSerializer(Serializer& serializer) const {
    auto lock = cache_.wlock();
    std::vector<std::pair<Key, std::shared_ptr<const Value>>> entries;
    for (const auto& key : lock->getAllNonpinnedKeys()) {
      entries.emplace_back(key, (*lock)[key]);
      AD_CORRECTNESS_CHECK(entries.back().second != nullptr);
    }

    // Serialize the number of entries
    serializer << entries.size();

    // Serialize each entry
    for (const auto& [key, value] : entries) {
      serializer << key;
      serializer << *value;
    }
  }

  // _____________________________________________________________________________
  template <typename Serializer>
  void readFromSerializer(Serializer& serializer, Value::Allocator allocator,
                          ad_utility::BlankNodeManager& blankNodeManager) {
    // Clear the cache first
    clear();

    // Deserialize the number of entries
    size_t numEntries;
    serializer >> numEntries;

    // Deserialize each entry and add to the cache
    for (size_t i = 0; i < numEntries; ++i) {
      // Deserialize the key
      Key key;
      serializer >> key;

      // Deserialize the value
      Value value;
      value.allocatorForSerialization_ = allocator;
      value.blankNodeManagerForSerialization_ = blankNodeManager;
      serializer >> value;

      // Use the store method to maintain consistency
      store(key, std::move(value));
    }
  }
};

// TODO<joka921> Move all this to a separate file, to make the includes more
// slim.
namespace ad_utility::serialization {

// Serialization for NamedResultCache::Value
// This serializes the complete Value including LocalVocab with proper ID
// remapping
AD_SERIALIZE_FUNCTION_WITH_CONSTRAINT(
    (ad_utility::SimilarTo<T, NamedResultCache::Value>)) {
  if constexpr (WriteSerializer<S>) {
    // Serialize the LocalVocab first (required for ID remapping)
    ad_utility::detail::serializeLocalVocab(serializer, arg.localVocab_);

    // Serialize the IdTable (uses the `serializeIds` helper which handles
    // LocalVocab IDs)
    serializer << arg.result_->numRows();
    serializer << arg.result_->numColumns();
    for (const auto& col : arg.result_->getColumns()) {
      ad_utility::detail::serializeIds(serializer, col);
    }

    // Serialize VariableToColumnMap manually (can't use HashMap serialization
    // because Variable is not default-constructible)
    serializer << arg.varToColMap_.size();
    for (const auto& [var, colInfo] : arg.varToColMap_) {
      serializer << var;
      serializer << colInfo;
    }

    // Serialize resultSortedOn (vector of ColumnIndex)
    serializer << arg.resultSortedOn_;

    // Serialize cacheKey (string)
    serializer << arg.cacheKey_;

    // Serialize cachedGeoIndex (optional)
    // Note: We cannot serialize the `optional` directly, because the geo index
    // has no default constructor for safety reasons.
    serializer << arg.cachedGeoIndex_.has_value();
    bool hasGeoIndex = arg.cachedGeoIndex_.has_value();
    serializer << hasGeoIndex;
    if (hasGeoIndex) {
      serializer << arg.cachedGeoIndex_.value();
    }
  } else {
    // Deserialize the LocalVocab and get the ID mapping
    AD_CORRECTNESS_CHECK(arg.blankNodeManagerForSerialization_.has_value());
    auto [localVocab, mapping] = ad_utility::detail::deserializeLocalVocab(
        serializer, &arg.blankNodeManagerForSerialization_.value());

    std::optional<NamedResultCache::Value::Allocator> dummyAllocator;
    const auto& allocator = [&]() -> const auto& {
      if (arg.allocatorForSerialization_.has_value()) {
        return arg.allocatorForSerialization_.value();
      } else {
        dummyAllocator = ad_utility::makeUnlimitedAllocator<Id>();
        return dummyAllocator.value();
      }
    }();

    // Deserialize the IdTable with ID mapping applied
    size_t numRows, numColumns;
    serializer >> numRows;
    serializer >> numColumns;

    IdTable idTable{numColumns, allocator};
    idTable.resize(numRows);
    for (decltype(auto) col : idTable.getColumns()) {
      ad_utility::detail::deserializeIds(serializer, mapping, col);
    }

    // Deserialize VariableToColumnMap manually
    size_t mapSize;
    serializer >> mapSize;
    VariableToColumnMap varToColMap;
    for (size_t i = 0; i < mapSize; ++i) {
      Variable var{"?dummy"};  // Variable needs a non-empty name
      serializer >> var;
      ColumnIndexAndTypeInfo colInfo{0, ColumnIndexAndTypeInfo::AlwaysDefined};
      serializer >> colInfo;
      varToColMap[std::move(var)] = colInfo;
    }

    // Deserialize resultSortedOn
    std::vector<ColumnIndex> resultSortedOn;
    serializer >> resultSortedOn;

    // Deserialize cacheKey
    std::string cacheKey;
    serializer >> cacheKey;

    // Deserialize cachedGeoIndex
    bool hasGeoIndex;
    serializer >> hasGeoIndex;
    std::optional<SpatialJoinCachedIndex> cachedGeoIndex;
    if (hasGeoIndex) {
      cachedGeoIndex.emplace(SpatialJoinCachedIndex::TagForSerialization{});
      serializer >> cachedGeoIndex.value();
    }

    // Construct the Value
    arg = NamedResultCache::Value{
        std::make_shared<const IdTable>(std::move(idTable)),
        std::move(varToColMap),
        std::move(resultSortedOn),
        std::move(localVocab),
        std::move(cacheKey),
        std::move(cachedGeoIndex)};
  }
}

}  // namespace ad_utility::serialization

#endif  // QLEVER_SRC_ENGINE_NAMEDRESULTCACHE_H
