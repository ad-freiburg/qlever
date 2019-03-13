// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_set>
#include "../global/Constants.h"
#include "../index/Index.h"
#include "../util/LRUCache.h"
#include "../util/Log.h"
#include "./Engine.h"
#include "./ResultTable.h"
#include "./RuntimeSettings.h"
#include "QueryPlanningCostFactors.h"
#include "RuntimeInformation.h"
#include "ResultTable.h"

using std::shared_ptr;
using std::string;
using std::vector;

struct CacheValue {
  CacheValue() : _resTable(std::make_shared<ResultTable>()), _runtimeInfo() {}
  std::shared_ptr<ResultTable> _resTable;
  RuntimeInformation _runtimeInfo;
};

typedef ad_utility::LRUCache<string, CacheValue> SubtreeCache;

// Execution context for queries.
// Holds references to index and engine, implements caching.
class QueryExecutionContext {
 public:
  QueryExecutionContext(const Index& index, const Engine& engine, SubtreeCache* cache,
                        const RuntimeSettings& settings = RuntimeSettings())
      : _subtreeCache(cache),
        _index(index),
        _engine(engine),
        _costFactors(),
        _settings(settings) {}

  SubtreeCache& getQueryTreeCache() { return *_subtreeCache; }

  const Engine& getEngine() const { return _engine; }

  const Index& getIndex() const { return _index; }

  const RuntimeSettings& getSettings() const { return _settings; }

  void clearCache() { _subtreeCache->clear(); }

  void readCostFactorsFromTSVFile(const string& fileName) {
    _costFactors.readFromFile(fileName);
  }

  float getCostFactor(const string& key) const {
    return _costFactors.getCostFactor(key);
  };

  void registerComputationStart(shared_ptr<ResultTable> key){
    _startedComputationKeys.insert(key);
  }

  void registerComputationEnd(shared_ptr<ResultTable> key){
    if (_startedComputationKeys.count(key)) {
      _startedComputationKeys.erase(key);
    }
  }

  auto& getRunningComputations() { return _startedComputationKeys;}

 private:
  // cache keys of all computations that have been started
  // within this context. those are candidates for being cleaned up after
  // a timeout.
  // TODO: we should use a mutexed set here as soon as we are going
  // parallel within a query this should be a mutexed hashSet.
  std::unordered_set<std::shared_ptr<ResultTable>> _startedComputationKeys;
  SubtreeCache* _subtreeCache;
  const Index& _index;
  const Engine& _engine;
  QueryPlanningCostFactors _costFactors;
  RuntimeSettings _settings;
};
