//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "engine/NamedQueryCache.h"

// _____________________________________________________________________________
std::shared_ptr<ExplicitIdTableOperation> NamedQueryCache ::getOperation(
    const Key& key, QueryExecutionContext* ctx) {
  const auto& ptr = get(key);
  const auto& [table, map, sortedOn, localVocab] = *ptr;
  auto res = std::make_shared<ExplicitIdTableOperation>(
      ctx, table, map, sortedOn, localVocab.clone());
  return res;
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
  auto lock = cache_.wlock();
  // The underlying cache throws on insert if the key is already present. We
  // therefore first call `erase`, which silently ignores keys that are not
  // present to avoid this behavior.
  lock->erase(key);
  lock->insert(key, std::move(value));
}

// _____________________________________________________________________________
void NamedQueryCache::erase(const Key& key) { cache_.wlock()->erase(key); }

// _____________________________________________________________________________
void NamedQueryCache::clear() { cache_.wlock()->clearAll(); }

// _____________________________________________________________________________
size_t NamedQueryCache::numEntries() const {
  return cache_.rlock()->numNonPinnedEntries();
}
