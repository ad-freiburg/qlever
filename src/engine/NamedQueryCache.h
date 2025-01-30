//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>
#pragma once

#include "engine/ValuesForTesting.h"
#include "util/Cache.h"
#include "util/Synchronized.h"

class NamedQueryCache {
  using Key = std::string;
  using Value = std::shared_ptr<ValuesForTesting>;
  using Cache =
      ad_utility::HashMap<std::string, std::shared_ptr<ValuesForTesting>>;

  ad_utility::Synchronized<Cache> cache_;

  void store(const Key& key, Value value) {
    (*cache_.wlock())[key] = std::move(value);
  }
  Value get(const Key& key) {
    auto l = cache_.wlock();
    auto it = l->find(key);
    // TODO<joka921> Proper error message.
    AD_CONTRACT_CHECK(it != l->end());
    return it->second;
  }
};
