// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2011-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "engine/Engine.h"
#include "engine/QueryPlanningCostFactors.h"
#include "engine/ResultTable.h"
#include "engine/RuntimeInformation.h"
#include "engine/SortPerformanceEstimator.h"
#include "global/Constants.h"
#include "global/Id.h"
#include "index/Index.h"
#include "util/Cache.h"
#include "util/ConcurrentCache.h"
#include "util/Log.h"
#include "util/Synchronized.h"
#include "util/http/websocket/QueryId.h"

using std::shared_ptr;
using std::string;
using std::vector;

class CacheValue {
 private:
  std::shared_ptr<const ResultTable> _resultTable;
  RuntimeInformation _runtimeInfo;

 public:
  explicit CacheValue(ResultTable resultTable, RuntimeInformation runtimeInfo)
      : _resultTable(
            std::make_shared<const ResultTable>(std::move(resultTable))),
        _runtimeInfo(std::move(runtimeInfo)) {}

  const shared_ptr<const ResultTable>& resultTable() const {
    return _resultTable;
  }

  const RuntimeInformation& runtimeInfo() const { return _runtimeInfo; }

  // Calculates the `MemorySize` taken up by an instance of `CacheValue`.
  struct SizeGetter {
    ad_utility::MemorySize operator()(const CacheValue& cacheValue) const {
      if (const auto& tablePtr = cacheValue._resultTable; tablePtr) {
        return ad_utility::MemorySize::bytes(tablePtr->size() *
                                             tablePtr->width() * sizeof(Id));
      } else {
        return 0_B;
      }
    }
  };
};

// Threadsafe LRU cache for (partial) query results, that
// checks on insertion, if the result is currently being computed
// by another query.
using ConcurrentLruCache = ad_utility::ConcurrentCache<
    ad_utility::LRUCache<string, CacheValue, CacheValue::SizeGetter>>;
using PinnedSizes =
    ad_utility::Synchronized<ad_utility::HashMap<std::string, size_t>,
                             std::shared_mutex>;
class QueryResultCache : public ConcurrentLruCache {
 private:
  PinnedSizes _pinnedSizes;

 public:
  virtual ~QueryResultCache() = default;
  void clearAll() override {
    // The _pinnedSizes are not part of the (otherwise threadsafe) _cache
    // and thus have to be manually locked.
    auto lock = _pinnedSizes.wlock();
    ConcurrentLruCache::clearAll();
    lock->clear();
  }
  // Inherit the constructor.
  using ConcurrentLruCache::ConcurrentLruCache;
  const PinnedSizes& pinnedSizes() const { return _pinnedSizes; }
  PinnedSizes& pinnedSizes() { return _pinnedSizes; }
  std::optional<size_t> getPinnedSize(const std::string& key) {
    auto rlock = _pinnedSizes.rlock();
    if (rlock->contains(key)) {
      return rlock->at(key);
    } else {
      return std::nullopt;
    }
  }
};

// Execution context for queries.
// Holds references to index and engine, implements caching.
class QueryExecutionContext {
 public:
  QueryExecutionContext(
      const Index& index, QueryResultCache* const cache,
      ad_utility::AllocatorWithLimit<Id> allocator,
      SortPerformanceEstimator sortPerformanceEstimator,
      std::function<void(std::string)> updateCallback =
          [](std::string) { /* No-op by default for testing */ },
      const bool pinSubtrees = false, const bool pinResult = false)
      : _pinSubtrees(pinSubtrees),
        _pinResult(pinResult),
        _index(index),
        _subtreeCache(cache),
        _allocator(std::move(allocator)),
        _costFactors(),
        _sortPerformanceEstimator(sortPerformanceEstimator),
        updateCallback_(std::move(updateCallback)) {}

  QueryResultCache& getQueryTreeCache() { return *_subtreeCache; }

  [[nodiscard]] const Index& getIndex() const { return _index; }

  void clearCacheUnpinnedOnly() { getQueryTreeCache().clearUnpinnedOnly(); }

  [[nodiscard]] const SortPerformanceEstimator& getSortPerformanceEstimator()
      const {
    return _sortPerformanceEstimator;
  }

  [[nodiscard]] double getCostFactor(const string& key) const {
    return _costFactors.getCostFactor(key);
  };

  const ad_utility::AllocatorWithLimit<Id>& getAllocator() {
    return _allocator;
  }

  /// Function that serializes the given RuntimeInformation to JSON and
  /// calls the updateCallback with this JSON string.
  /// This is used to broadcast updates of any query to a third party
  /// while it's still running.
  /// \param runtimeInformation The `RuntimeInformation` to serialize
  void signalQueryUpdate(const RuntimeInformation& runtimeInformation) const {
    updateCallback_(nlohmann::ordered_json(runtimeInformation).dump());
  }

  bool _pinSubtrees;
  bool _pinResult;

 private:
  const Index& _index;
  QueryResultCache* const _subtreeCache;
  // allocators are copied but hold shared state
  ad_utility::AllocatorWithLimit<Id> _allocator;
  QueryPlanningCostFactors _costFactors;
  SortPerformanceEstimator _sortPerformanceEstimator;
  std::function<void(std::string)> updateCallback_;
};
