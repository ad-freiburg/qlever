//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/NamedQueryCache.h"

// _____________________________________________________________________________
std::shared_ptr<ValuesForTesting> NamedQueryCache ::getOperation(
    const Key& key, QueryExecutionContext* ctx) const {
  const auto& [table, map, sortedOn] = get(key);
  // TODO<joka921> we should get rid of the copies for the IdTable (and
  // probably the other members) especially for larger results).
  return std::make_shared<ValuesForTesting>(ctx, table.clone(), map, sortedOn);
}

// _____________________________________________________________________________
auto NamedQueryCache::get(const Key& key) const -> const Value& {
  auto l = cache_.wlock();
  auto it = l->find(key);
  if (it == l->end()) {
    throw std::runtime_error{
        absl::StrCat("The named query with the name \"", key,
                     "\" was not pinned to the named query cache")};
  }
  return it->second;
}

// _____________________________________________________________________________
void NamedQueryCache::store(const Key& key, Value value) {
  (*cache_.wlock()).insert_or_assign(key, std::move(value));
}

// _____________________________________________________________________________
void NamedQueryCache::clear() { cache_.wlock()->clear(); }
