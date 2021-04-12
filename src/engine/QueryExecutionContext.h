// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "../global/Constants.h"
#include "../index/Index.h"
#include "../util/Cache.h"
#include "../util/ConcurrentCache.h"
#include "../util/Log.h"
#include "../util/Synchronized.h"
#include "./Engine.h"
#include "./ResultTable.h"
#include "QueryPlanningCostFactors.h"
#include "RuntimeInformation.h"

using std::shared_ptr;
using std::string;
using std::vector;

struct CacheValue {
  CacheValue(ad_utility::AllocatorWithLimit<Id> allocator)
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

// Execution context for queries.
// Holds references to index and engine, implements caching.
class QueryExecutionContext {
 public:
  QueryExecutionContext(const Index& index, const Engine& engine,
                        ConcurrentLruCache* const cache,
                        PinnedSizes* const pinnedSizes,
                        ad_utility::AllocatorWithLimit<Id> allocator,
                        const bool pinSubtrees = false,
                        const bool pinResult = false)
      : _pinSubtrees(pinSubtrees),
        _pinResult(pinResult),
        _index(index),
        _engine(engine),
        _subtreeCache(cache),
        _pinnedSizes(pinnedSizes),
        _alloc(std::move(allocator)),
        _costFactors() {}

  ConcurrentLruCache& getQueryTreeCache() { return *_subtreeCache; }

  PinnedSizes& getPinnedSizes() { return *_pinnedSizes; }

  const Engine& getEngine() const { return _engine; }

  const Index& getIndex() const { return _index; }

  void clearCacheUnpinnedOnly() { getQueryTreeCache().clearUnpinnedOnly(); }

  void readCostFactorsFromTSVFile(const string& fileName) {
    _costFactors.readFromFile(fileName);
  }

  float getCostFactor(const string& key) const {
    return _costFactors.getCostFactor(key);
  };

  ad_utility::AllocatorWithLimit<Id> getAllocator() { return _alloc; }

  const bool _pinSubtrees;
  const bool _pinResult;

 private:
  const Index& _index;
  const Engine& _engine;
  ConcurrentLruCache* const _subtreeCache;
  PinnedSizes* const _pinnedSizes;
  // allocators are copied but hold shared state
  ad_utility::AllocatorWithLimit<Id> _alloc;
  QueryPlanningCostFactors _costFactors;
};
