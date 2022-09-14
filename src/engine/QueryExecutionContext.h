// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author:
//   2011-2017 Björn Buchhold (buchhold@informatik.uni-freiburg.de)
//   2018-     Johannes Kalmbach (kalmbach@informatik.uni-freiburg.de)

#pragma once

#include <engine/Engine.h>
#include <engine/QueryPlanningCostFactors.h>
#include <engine/ResultTable.h>
#include <engine/RuntimeInformation.h>
#include <engine/SortPerformanceEstimator.h>
#include <global/Constants.h>
#include <index/Index.h>
#include <util/Cache.h>
#include <util/ConcurrentCache.h>
#include <util/Log.h>
#include <util/Synchronized.h>

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

using std::shared_ptr;
using std::string;
using std::vector;

struct CacheValue {
  explicit CacheValue(ad_utility::AllocatorWithLimit<Id> allocator)
      : _resultTable(std::make_shared<ResultTable>(std::move(allocator))),
        _runtimeInfo() {}
  std::shared_ptr<ResultTable> _resultTable;
  RuntimeInformation _runtimeInfo;
  [[nodiscard]] size_t size() const {
    return _resultTable ? _resultTable->size() * _resultTable->width() : 0;
  }
};

// Threadsafe LRU cache for (partial) query results, that
// checks on insertion, if the result is currently being computed
// by another query.
using ConcurrentLruCache =
    ad_utility::ConcurrentCache<ad_utility::LRUCache<string, CacheValue>>;
using PinnedSizes =
    ad_utility::Synchronized<ad_utility::HashMap<std::string, size_t>,
                             std::shared_mutex>;
class QueryResultCache : public ConcurrentLruCache {
 private:
  PinnedSizes _pinnedSizes;

 public:
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
  QueryExecutionContext(const Index& index, const Engine& engine,
                        QueryResultCache* const cache,
                        ad_utility::AllocatorWithLimit<Id> allocator,
                        SortPerformanceEstimator sortPerformanceEstimator,
                        const bool pinSubtrees = false,
                        const bool pinResult = false)
      : _pinSubtrees(pinSubtrees),
        _pinResult(pinResult),
        _index(index),
        _engine(engine),
        _subtreeCache(cache),
        _allocator(std::move(allocator)),
        _costFactors(),
        _sortPerformanceEstimator(sortPerformanceEstimator) {}

  QueryResultCache& getQueryTreeCache() { return *_subtreeCache; }

  [[nodiscard]] const Engine& getEngine() const { return _engine; }

  [[nodiscard]] const Index& getIndex() const { return _index; }

  void clearCacheUnpinnedOnly() { getQueryTreeCache().clearUnpinnedOnly(); }

  [[nodiscard]] const SortPerformanceEstimator& getSortPerformanceEstimator()
      const {
    return _sortPerformanceEstimator;
  }

  void readCostFactorsFromTSVFile(const string& fileName) {
    _costFactors.readFromFile(fileName);
  }

  [[nodiscard]] double getCostFactor(const string& key) const {
    return _costFactors.getCostFactor(key);
  };

  ad_utility::AllocatorWithLimit<Id> getAllocator() { return _allocator; }

  const bool _pinSubtrees;
  const bool _pinResult;

 private:
  const Index& _index;
  const Engine& _engine;
  QueryResultCache* const _subtreeCache;
  // allocators are copied but hold shared state
  ad_utility::AllocatorWithLimit<Id> _allocator;
  QueryPlanningCostFactors _costFactors;
  SortPerformanceEstimator _sortPerformanceEstimator;
};
