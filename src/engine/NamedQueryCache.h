//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
#pragma once

#include "engine/ValuesForTesting.h"
#include "util/Cache.h"
#include "util/Synchronized.h"

class NamedQueryCache {
 public:
  struct Value {
    IdTable result_;
    VariableToColumnMap varToColMap_;
    std::vector<ColumnIndex> resultSortedOn_;
  };
  using Key = std::string;
  using Cache = ad_utility::HashMap<std::string, Value>;

 private:
  ad_utility::Synchronized<Cache> cache_;

 public:
  void store(const Key& key, Value value) {
    (*cache_.wlock()).insert_or_assign(key, std::move(value));
  }
  const Value& get(const Key& key) {
    auto l = cache_.wlock();
    auto it = l->find(key);
    // TODO<joka921> Proper error message.
    AD_CONTRACT_CHECK(it != l->end());
    return it->second;
  }

  std::shared_ptr<ValuesForTesting> getOperation(const Key& key,
                                                 QueryExecutionContext* ctx) {
    const auto& [table, map, sortedOn] = get(key);
    return std::make_shared<ValuesForTesting>(
        ctx, std::make_shared<IdTable>(table.clone()), map);
  }
};
