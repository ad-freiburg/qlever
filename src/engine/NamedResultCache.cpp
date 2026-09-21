// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/NamedResultCache.h"

#include "engine/NamedResultCacheSerializer.h"
#include "global/RuntimeParameters.h"

// _____________________________________________________________________________
std::shared_ptr<ExplicitIdTableOperation> NamedResultCache::getOperation(
    const Key& name, QueryExecutionContext* qec) {
  auto result = getIfContained(name);
  if (result == nullptr) {
    // Silencing this exception has to be requested explicitly, see
    // `RuntimeParameters::emptyResultInsteadOfExceptions_`.
    if (!getRuntimeParameter<
            &RuntimeParameters::emptyResultInsteadOfExceptions_>()) {
      throwNotContained(name);
    }
    return makeEmptyOperation(name, qec);
  }
  const auto& [table, map, sortedOn, localVocab, cacheKey, geoIndex, allocator,
               blankNodeManager] = *result;
  auto resultAsOperation = qec->makeShared<ExplicitIdTableOperation>(
      qec, table, map, sortedOn, localVocab.clone(), cacheKey);
  return resultAsOperation;
}

// _____________________________________________________________________________
auto NamedResultCache::get(const Key& name) const
    -> std::shared_ptr<const Value> {
  auto result = getIfContained(name);
  if (result == nullptr) {
    throwNotContained(name);
  }
  return result;
}

// _____________________________________________________________________________
auto NamedResultCache::getIfContained(const Key& name) const
    -> std::shared_ptr<const Value> {
  // Note: this function is `const`, but we need to use the (non-const) `wlock`
  // function on the `mutable` `cache_`, because the `operator[]` is not `const`
  // because it updates the LRU structures in the cache. However, logically it
  // doesn't change the contents of the cache and additionally (because of the
  // `wlock`) is threadsafe, this usage of `mutable` is okay.
  auto lock = cache_.wlock();
  if (!lock->contains(name)) {
    return nullptr;
  }
  return (*lock)[name];
}

// _____________________________________________________________________________
void NamedResultCache::throwNotContained(const Key& name) {
  throw std::runtime_error{
      absl::StrCat("The cached result with name \"", name,
                   "\" is not contained in the named result cache.")};
}

// _____________________________________________________________________________
std::shared_ptr<ExplicitIdTableOperation> NamedResultCache::makeEmptyOperation(
    const Key& name, QueryExecutionContext* qec) {
  // NOTE: The name is part of the cache key, because the empty result stands in
  // for the (currently missing) result with that name. Should that result
  // become available later, its own (different) cache key is used, so stale
  // empty results are never served from the query cache.
  return qec->makeShared<ExplicitIdTableOperation>(
      qec, std::make_shared<const IdTable>(0, qec->getAllocator()),
      VariableToColumnMap{}, std::vector<ColumnIndex>{}, LocalVocab{},
      absl::StrCat("Empty result for the missing named result \"", name, "\""));
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
