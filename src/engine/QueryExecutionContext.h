// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2011-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <string>

#include "engine/QueryPlanningCostFactors.h"
#include "engine/Result.h"
#include "engine/RuntimeInformation.h"
#include "engine/SortPerformanceEstimator.h"
#include "global/Id.h"
#include "index/Index.h"
#include "util/Cache.h"
#include "util/ConcurrentCache.h"
#include "util/Synchronized.h"

class CacheValue {
 private:
  std::shared_ptr<Result> result_;
  RuntimeInformation runtimeInfo_;

 public:
  explicit CacheValue(Result result, RuntimeInformation runtimeInfo)
      : result_{std::make_shared<Result>(std::move(result))},
        runtimeInfo_{std::move(runtimeInfo)} {}

  CacheValue(CacheValue&&) = default;
  CacheValue(const CacheValue&) = delete;
  CacheValue& operator=(CacheValue&&) = default;
  CacheValue& operator=(const CacheValue&) = delete;

  const Result& resultTable() const noexcept { return *result_; }

  std::shared_ptr<const Result> resultTablePtr() const noexcept {
    return result_;
  }

  const RuntimeInformation& runtimeInfo() const noexcept {
    return runtimeInfo_;
  }

  static ad_utility::MemorySize getSize(const IdTable& idTable) {
    return ad_utility::MemorySize::bytes(idTable.size() * idTable.numColumns() *
                                         sizeof(Id));
  }

  // Calculates the `MemorySize` taken up by an instance of `CacheValue`.
  struct SizeGetter {
    ad_utility::MemorySize operator()(const CacheValue& cacheValue) const {
      if (const auto& resultPtr = cacheValue.result_; resultPtr) {
        return getSize(resultPtr->idTable());
      } else {
        return 0_B;
      }
    }
  };
};

// Threadsafe LRU cache for (partial) query results, that
// checks on insertion, if the result is currently being computed
// by another query.
using QueryResultCache = ad_utility::ConcurrentCache<
    ad_utility::LRUCache<string, CacheValue, CacheValue::SizeGetter>>;

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

  [[nodiscard]] double getCostFactor(const std::string& key) const {
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
