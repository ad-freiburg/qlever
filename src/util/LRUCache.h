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
//! This implementation provides thread safety for the cache itself but does
//! not enforce read-only access to the cached values. This is so that the
//! values may be used to block threads on their completion.
template <class Key, class Value,
          class AccessMap = ad_utility::HashMap<
              Key, typename list<pair<Key, shared_ptr<Value>>>::iterator>>
class LRUCache {
private:
  typedef pair<Key, shared_ptr<Value>> Entry;
  typedef list<Entry> EntryList;

public:
  //! Typical constructor. A default value may be added in time.
  explicit LRUCache(size_t capacity)
      : _capacity(capacity), _data(), _accessMap(), _lock() {}

  //! Get a value or default construct a new one.
  //! If the value does not exist in the cache, a
  //! a default Value is constructed and created is set to true. 
  //! otherwise created is set to false. Using a created paramter allows
  //! a threaded call to check if it needs to initialize the value in a
  //! race free way. If we didn't make that explicit a newly created value
  //! by another thread could not be distinguished from one created by the
  //! current thread.
  //! TODO(schnelle) add test
  shared_ptr<Value> getOrCreate(const Key &key, bool* created) {
    lock_guard<mutex> lock(_lock);
    typename AccessMap::const_iterator mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      *created = true;
      // Insert without taking mutex recursively
      _data.push_front(Entry(key, make_shared<Value>()));
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
    } else {
      *created = false;
      // Move it to the front.
      typename EntryList::iterator listIt = mapIt->second;
      _data.splice(_data.begin(), _data, listIt);
      _accessMap[key] = _data.begin();
    }
    assert(_accessMap.find(key)->second == _data.begin());
    return _data.begin()->second;
  }

  //! Lookup a read-only value without creating a new one if non exists
  //! instead shared_ptr<const Value>(nullptr) is returned in that case
  shared_ptr<const Value> operator[](const Key &key) {
    lock_guard<mutex> lock(_lock);
    typename AccessMap::const_iterator mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Returning a null pointer allows to easily check if the element
      // existed and crash misuses.
      return shared_ptr<Value>(nullptr);
    }

    // Move it to the front.
    typename EntryList::iterator listIt = mapIt->second;
    _data.splice(_data.begin(), _data, listIt);
    _accessMap[key] = _data.begin();
    assert(_accessMap.find(key)->second == _data.begin());
    return _data.begin()->second;
  }

  //! Insert a key value pair to the cache.
  void insert(const Key &key, Value value) {
    lock_guard<mutex> lock(_lock);
    _data.push_front(Entry(key, make_shared<Value>(std::move(value))));
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
  bool contains(const Key &key) {
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
