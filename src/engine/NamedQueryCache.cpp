//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/NamedQueryCache.h"

// _____________________________________________________________________________
std::shared_ptr<ValuesForTesting> NamedQueryCache ::getOperation(
    const Key& key, QueryExecutionContext* ctx) {
  const auto& ptr = get(key);
  const auto& [table, map, sortedOn] = *ptr;
  // TODO<joka921> Add a local vocab, and consider also passing a shared_ptr for
  // the local vocab.
  return std::make_shared<ValuesForTesting>(ctx, table, map, sortedOn);
}

// _____________________________________________________________________________
auto NamedQueryCache::get(const Key& key) -> std::shared_ptr<const Value> {
  auto l = cache_.wlock();
  if (!l->contains(key)) {
    throw std::runtime_error{
        absl::StrCat("The named query with the name \"", key,
                     "\" was not pinned to the named query cache")};
  }
  return (*l)[key];
}

// _____________________________________________________________________________
void NamedQueryCache::store(const Key& key, Value value) {
  // TODO<joka921> Check the overwrite semantics of the cache class.
  cache_.wlock()->insert(key, std::move(value));
}

// _____________________________________________________________________________
void NamedQueryCache::clear() { cache_.wlock()->clearAll(); }

// _____________________________________________________________________________
size_t NamedQueryCache::numEntries() const {
  return cache_.rlock()->numNonPinnedEntries();
}
