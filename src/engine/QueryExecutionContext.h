// Copyright 2011, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <string>
#include <vector>
#include <memory>
#include "../util/LRUCache.h"
#include "../util/Log.h"
#include "./ResultTable.h"
#include "./Engine.h"
#include "../global/Constants.h"
#include "../index/Index.h"
#include "QueryPlanningCostFactors.h"

using std::string;
using std::vector;
using std::shared_ptr;

typedef ad_utility::LRUCache<string, ResultTable> SubtreeCache;

// Execution context for queries.
// Holds references to index and engine, implements caching.
class QueryExecutionContext {
public:

  QueryExecutionContext(const Index& index, const Engine& engine) :
      _subtreeCache(NOF_SUBTREES_TO_CACHE),
      _index(&index), _engine(&engine), _costFactors() {
  }

  SubtreeCache& getQueryTreeCache() {
    return _subtreeCache;
  }

  const Engine& getEngine() const {
    return *_engine;
  }

  const Index& getIndex() const {
    return *_index;
  }

  void clearCache() {
    _subtreeCache.clear();
  }

  void readCostFactorsFromTSVFile(const string& fileName) {
    _costFactors.readFromFile(fileName);
  }

  float getCostFactor(const string& key) const {
    return _costFactors.getCostFactor(key);
  };

 private:
  SubtreeCache _subtreeCache;
  const Index* _index;
  const Engine* _engine;
  QueryPlanningCostFactors _costFactors;
};

