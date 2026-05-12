// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/NamedResultCache.h"

#include <algorithm>

#include "engine/NamedResultCacheSerializer.h"
#include "util/Serializer/FileSerializer.h"

// _____________________________________________________________________________
std::shared_ptr<ExplicitIdTableOperation> NamedResultCache::getOperation(
    const Key& name, QueryExecutionContext* qec) {
  const auto& result = get(name);
  const auto& [table, map, sortedOn, localVocab, cacheKey, geoIndex, allocator,
               blankNodeManager] = *result;
  auto resultAsOperation = std::make_shared<ExplicitIdTableOperation>(
      qec, table, map, sortedOn, localVocab.clone(), cacheKey);
  return resultAsOperation;
}

// _____________________________________________________________________________
auto NamedResultCache::get(const Key& name) const
    -> std::shared_ptr<const Value> {
  // Note: this function is `const`, but we need to use the (non-const) `wlock`
  // function on the `mutable` `cache_`, because the `operator[]` is not `const`
  // because it updates the LRU structures in the cache. However, logically it
  // doesn't change the contents of the cache and additionally (because of the
  // `wlock`) is threadsafe, this usage of `mutable` is okay.
  auto lock = cache_.wlock();
  if (!lock->contains(name)) {
    throw std::runtime_error{
        absl::StrCat("The cached result with name \"", name,
                     "\" is not contained in the named result cache.")};
  }
  return (*lock)[name];
}

// _____________________________________________________________________________
void NamedResultCache::store(const Key& name, Value result) {
  auto lock = cache_.wlock();
  // The underlying cache throws on insert if the key is already present. We
  // therefore first call `erase`, which silently ignores keys that are not
  // present to avoid this behavior.
  lock->erase(name);
  lock->insert(name, std::move(result));
}

// _____________________________________________________________________________
void NamedResultCache::erase(const Key& name) { cache_.wlock()->erase(name); }

// _____________________________________________________________________________
void NamedResultCache::clear() { cache_.wlock()->clearAll(); }

// _____________________________________________________________________________
size_t NamedResultCache::numEntries() const {
  return cache_.rlock()->numNonPinnedEntries();
}
