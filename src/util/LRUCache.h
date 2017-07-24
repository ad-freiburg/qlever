// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include "./HashMap.h"
#include <assert.h>
#include <list>
#include <memory>
#include <mutex>
#include <utility>

using std::list;
using std::pair;
using std::mutex;
using std::shared_ptr;
using std::make_shared;
using std::lock_guard;

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
//! This implementation provides thread safety through the use of locking and
//! only allowing access to cached values via read-only shared_ptr's which
//! also guarantee that memory is not freed while some thread still has access
//! to the cached data.
template <class Key, class Value,
          class AccessMap = ad_utility::HashMap<
              Key, typename list<pair<Key, shared_ptr<const Value>>>::iterator>>
class LRUCache {
private:
  typedef pair<Key, shared_ptr<const Value>> Entry;
  typedef list<Entry> EntryList;

public:
  //! Typical constructor. A default value may be added in time.
  explicit LRUCache(size_t capacity)
      : _capacity(capacity), _data(), _accessMap(), _lock() {}

  //! Lookup operator. If the value does not exist in the cache, a
  //! shared_ptr<const Value>(nullptr) is returned. Using a shared_ptr
  //! to const prevents any modification of cached values which may be
  //! shared across threads. The shared_ptr itself may however be copied
  //! as it uses thread safe primitives internally
  shared_ptr<const Value> operator[](const Key& key) {
    lock_guard<mutex> lock(_lock);
    typename AccessMap::const_iterator mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Returning a null pointer allows to easily check if the element
      // existed and crash misuses.
      return shared_ptr<const Value>(nullptr);
    }

    // Move it to the front.
    typename EntryList::iterator listIt = mapIt->second;
    _data.splice(_data.begin(), _data, listIt);
    _accessMap[key] = _data.begin();
    assert(_accessMap.find(key)->second == _data.begin());
    return _data.begin()->second;
  }

  //! Insert a key value pair to the cache.
  shared_ptr<const Value> insert(const Key& key, Value value) {
    lock_guard<mutex> lock(_lock);
    _data.push_front(Entry(key, make_shared<const Value>(std::move(value))));
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
    return _data.begin()->second;
  }

  //! Set the capacity.
  void setCapacity(const size_t nofElements) {
    lock_guard<mutex> lock(_lock);
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
    lock_guard<mutex> lock(_lock);
    return _accessMap.count(key) > 0;
  }

  //! Clear the cache
  void clear() {
    lock_guard<mutex> lock(_lock);
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
  // TODO(schnelle): Once we switch to C++17
  // this should be a shared_mutex
  mutex _lock;
};
}
