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
  const auto& [table, map, sortedOn, localVocab, cacheKey, geoIndex] = *result;
  auto resultAsOperation = std::make_shared<ExplicitIdTableOperation>(
      qec, table, map, sortedOn, localVocab.clone(), cacheKey);
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
  auto cacheLock = cache_.wlock();
  auto keysLock = keys_.wlock();

  // The underlying cache throws on insert if the key is already present. We
  // therefore first call `erase`, which silently ignores keys that are not
  // present to avoid this behavior.
  bool wasPresent = cacheLock->contains(name);
  cacheLock->erase(name);
  cacheLock->insert(name, std::move(result));

  // Update the keys list
  if (!wasPresent) {
    keysLock->push_back(name);
  }
}

// _____________________________________________________________________________
void NamedResultCache::erase(const Key& name) {
  cache_.wlock()->erase(name);
  auto keysLock = keys_.wlock();
  keysLock->erase(std::remove(keysLock->begin(), keysLock->end(), name),
                  keysLock->end());
}

// _____________________________________________________________________________
void NamedResultCache::clear() {
  cache_.wlock()->clearAll();
  keys_.wlock()->clear();
}

// _____________________________________________________________________________
size_t NamedResultCache::numEntries() const {
  return cache_.rlock()->numNonPinnedEntries();
}

// _____________________________________________________________________________
void NamedResultCache::writeToDisk(const std::string& path) const {
  ad_utility::serialization::FileWriteSerializer serializer{path.c_str()};

  auto keysLock = keys_.rlock();

  // Collect entries that exist in the cache
  std::vector<std::pair<Key, std::shared_ptr<const Value>>> entries;
  for (const auto& key : *keysLock) {
    try {
      // get() acquires its own lock
      auto value = const_cast<NamedResultCache*>(this)->get(key);
      entries.emplace_back(key, value);
    } catch (...) {
      // Key was evicted from cache, skip it
      continue;
    }
  }

  // Serialize the number of entries
  serializer << entries.size();

  // Serialize each entry
  for (const auto& [key, value] : entries) {
    serializer << key;
    serializer << *value;
  }
}

// _____________________________________________________________________________
void NamedResultCache::readFromDisk(const std::string& path) {
  ad_utility::serialization::FileReadSerializer serializer{path.c_str()};

  // Clear the cache first
  clear();

  // Deserialize the number of entries
  size_t numEntries;
  serializer >> numEntries;

  // Deserialize each entry and add to the cache
  for (size_t i = 0; i < numEntries; ++i) {
    // Deserialize the key
    Key key;
    serializer >> key;

    // Deserialize the value
    Value value;
    serializer >> value;

    // Use the store method to maintain consistency
    store(key, std::move(value));
  }
}
