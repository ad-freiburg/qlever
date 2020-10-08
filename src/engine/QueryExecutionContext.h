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
#include "../util/CacheAdapter.h"
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
  CacheValue(ad_utility::LimitedAllocator<Id> alloc)
      : _resTable(std::make_shared<ResultTable>(std::move(alloc))),
        _runtimeInfo() {}
  std::shared_ptr<ResultTable> _resTable;
  RuntimeInformation _runtimeInfo;
  [[nodiscard]] size_t size() const {
    return _resTable ? _resTable->size() * _resTable->width() : 0;
  }
};

// TODO: In C++ 20 this can be a lambda with a default constructor
namespace detail {
struct finisher {
  template <class V>
  void operator()(V&& val) const {val._resTable->finish();}
};
}
using SubtreeCache = ad_utility::CacheAdapter<ad_utility::LRUCache<string, CacheValue>, detail::finisher>;
using PinnedSizes =
    ad_utility::Synchronized<ad_utility::HashMap<std::string, size_t>,
                             std::shared_mutex>;

// Execution context for queries.
// Holds references to index and engine, implements caching.
class QueryExecutionContext {
 public:
  QueryExecutionContext(const Index& index, const Engine& engine,
                        SubtreeCache* const cache,
                        PinnedSizes* const pinnedSizes,
                        ad_utility::LimitedAllocator<Id> alloc,
                        const bool pinSubtrees = false,
                        const bool pinResult = false)
      : _pinSubtrees(pinSubtrees),
        _pinResult(pinResult),
        _index(index),
        _engine(engine),
        _subtreeCache(cache),
        _pinnedSizes(pinnedSizes),
        _alloc(std::move(alloc)),
        _costFactors() {}

  SubtreeCache& getQueryTreeCache() { return *_subtreeCache; }

  PinnedSizes& getPinnedSizes() { return *_pinnedSizes; }

  const Engine& getEngine() const { return _engine; }

  const Index& getIndex() const { return _index; }

  void clearCache() { getQueryTreeCache().clear(); }

  void readCostFactorsFromTSVFile(const string& fileName) {
    _costFactors.readFromFile(fileName);
  }

  float getCostFactor(const string& key) const {
    return _costFactors.getCostFactor(key);
  };

  ad_utility::LimitedAllocator<Id> getAllocator() { return _alloc; }

  const bool _pinSubtrees;
  const bool _pinResult;

 private:
  const Index& _index;
  const Engine& _engine;
  SubtreeCache* const _subtreeCache;
  PinnedSizes* const _pinnedSizes;
  // allocators are copied but hold shared state
  ad_utility::LimitedAllocator<Id> _alloc;
  QueryPlanningCostFactors _costFactors;
};
