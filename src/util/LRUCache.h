// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <assert.h>
#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include "./HashMap.h"

using std::list;
using std::make_shared;
using std::pair;
using std::shared_ptr;

namespace ad_utility {
//! Associative array for almost arbitrary keys and values that acts as a cache.
//! Hash a fixed capacity and applies a least recently used (LRU) strategy
//! for removing elements once the capacity is exceeded.
//! Keys have to be proper keys for the underlying AccessMap and value types
//! have to provide a default constructor.
//! Currently capacity relates to numbers of elements. May be changed
//! to bytes and checking entry sizes in the future.
//! An AccessMap can be provided in order to achieve a matching other than
//! exact matching. An example application for that is using the LRUCache
//! as full-text-query cache where one might want to get the best fit of any
//! entry that can be used to filter the desired result from.
//!
//! This implementation provides thread safety for the cache itself and also
//! enforces read-only access for all operations which can't guarantee only
//! one thread getting write access. It uses shared_ptrs so as to ensure
//! that deletes do not free in-use memory.
template <class Key, class Value,
          class AccessMap = ad_utility::HashMap<
              Key, typename list<pair<Key, shared_ptr<const Value>>>::iterator>>
class LRUCache {
 private:
  typedef pair<Key, shared_ptr<const Value>> Entry;
  typedef pair<shared_ptr<Value>, shared_ptr<const Value>> EmplacePair;
  typedef list<Entry> EntryList;

 public:
  //! Typical constructor. A default value may be added in time.
  explicit LRUCache(size_t capacity)
      : _capacity(capacity), _data(), _accessMap(), _lock() {}

  // tryEmplace allows for race-free adding of items to the cache. Iff no item
  // in the cache is associated with the key a new item is created and
  // a writeable reference is returned as the first element of the result pair
  // and it is the callers responsibility to synchronize access when writing to
  // it. If there already was an item associated with the key the first element
  // is shared_ptr<Value>(nullptr) and the second provides read-only access.
  //
  // The second element of the pair always returns a valid read-only reference,
  // either to the just created element or an existing element.
  //
  // This needs to happen in a single operation because otherwise we may find
  // an item in the cash now but it may already be deleted when trying to
  // retrieve  it in another operation.
  template <class... Args>
  EmplacePair tryEmplace(const Key& key, Args&&... args) {
    std::lock_guard<std::mutex> lock(_lock);
    EmplacePair result;
    typename AccessMap::const_iterator mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Insert without taking mutex recursively
      shared_ptr<Value> emplaced =
          make_shared<Value>(std::forward<Args>(args)...);
      _data.emplace_front(key, emplaced);
      _accessMap[key] = _data.begin();
      if (_data.size() > _capacity) {
        // Remove the last element.
        // Since we are using shared_ptr this does not free the underlying
        // memory if it is still accessible through a previously returned
        // shared_ptr
        _accessMap.erase(_data.back().first);
        _data.pop_back();
      }
      assert(_data.size() <= _capacity);
      result = EmplacePair(emplaced, emplaced);
      return result;
    }
    // Move it to the front.
    typename EntryList::iterator listIt = mapIt->second;
    _data.splice(_data.begin(), _data, listIt);
    _accessMap[key] = _data.begin();
    result = EmplacePair(shared_ptr<Value>(nullptr), _data.begin()->second);
    return result;
  }

  //! Lookup a read-only value without creating a new one if non exists
  //! instead shared_ptr<const Value>(nullptr) is returned in that case
  shared_ptr<const Value> operator[](const Key& key) {
    std::lock_guard<std::mutex> lock(_lock);
    shared_ptr<const Value> result;
    typename AccessMap::const_iterator mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Returning a null pointer allows to easily check if the element
      // existed and crash misuses.
      result = shared_ptr<Value>(nullptr);
      return result;
    }

    // Move it to the front.
    typename EntryList::iterator listIt = mapIt->second;
    _data.splice(_data.begin(), _data, listIt);
    _accessMap[key] = _data.begin();
    result = _data.front().second;
    return result;
  }

  //! Insert a key value pair to the cache.
  void insert(const Key& key, Value value) {
    std::lock_guard<std::mutex> lock(_lock);
    _data.emplace_front(key, make_shared<const Value>(std::move(value)));
    _accessMap[key] = _data.begin();
    if (_data.size() > _capacity) {
      // Remove the last element.
      // Since we are using shared_ptr this does not free the underlying
      // memory if it is still accessible through a previously returned
      // shared_ptr
      _accessMap.erase(_data.back().first);
      _data.pop_back();
    }
    assert(_data.size() <= _capacity);
  }

  //! Set the capacity.
  void setCapacity(const size_t nofElements) {
    std::lock_guard<std::mutex> lock(_lock);
    _capacity = nofElements;
    while (_data.size() > _capacity) {
      // Remove the last element.
      // Since we are using shared_ptr this does not free the underlying
      // memory if it is still accessible through a previously returned
      // shared_ptr
      _accessMap.erase(_data.back().first);
      _data.pop_back();
    }
    assert(_data.size() <= _capacity);
  }

  //! Checks if there is an entry with the given key.
  bool contains(const Key& key) {
    std::lock_guard<std::mutex> lock(_lock);
    return _accessMap.count(key) > 0;
  }

  //! Clear the cache
  void clear() {
    std::lock_guard<std::mutex> lock(_lock);
    // Since we are using shared_ptr this does not free the underlying
    // memory if it is still accessible through a previously returned
    // shared_ptr
    _data.clear();
    _accessMap.clear();
  }

 private:
  size_t _capacity;
  EntryList _data;
  AccessMap _accessMap;
  // TODO(schnelle): It would be nice to use std::shared_mutex to only exclude
  // multiple writers.
  // Sadly there is currently no easy way to upgrade a shared_lock to an
  // exclusive lock. Thus using a shared_lock here would conflict with moving to
  // the front of the queue in operator[]
  std::mutex _lock;
};
}  // namespace ad_utility
