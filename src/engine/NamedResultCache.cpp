// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

#include "engine/NamedResultCache.h"

// _____________________________________________________________________________
std::shared_ptr<ExplicitIdTableOperation> NamedResultCache::getOperation(
    const Key& name, QueryExecutionContext* qec) {
  const auto& result = get(name);
  const auto& [table, map, sortedOn, localVocab] = *result;
  auto resultAsOperation = std::make_shared<ExplicitIdTableOperation>(
      qec, table, map, sortedOn, localVocab.clone());
  return resultAsOperation;
}

// _____________________________________________________________________________
auto NamedResultCache::get(const Key& name) -> std::shared_ptr<const Value> {
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
