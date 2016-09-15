// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#pragma once

#include <assert.h>
#include <list>
#include <utility>
#include "./HashMap.h"

using std::list;
using std::pair;


namespace ad_utility {
//! Associative array for almost arbitrary keys and values that acts as a cache.
//! Hash a fixed capacity and applies a last recently used (LRU) strategy
//! for removing elements once the capacity is exceeded.
//! Keys have to be proper keys for the underlying AccessMap and value types
//! have to provide a default constructor.
//! Currently capacity relates to numbers of elements. May be changed
//! to bytes and checking entry sizes in the future.
//! An AccessMap can be provided in order to achieve a matching other than
//! exact matching. An example application for that is using the LRUCache
//! as full-text-query cache where one might want to get the best fit of any
//! entry that can be used to filter the desired result from.
template<class Key, class Value,
    class AccessMap = ad_utility::HashMap<Key,
        typename list<pair<Key, Value>>::iterator>>
class LRUCache {
  private:
    typedef pair<Key, Value> Entry;
    typedef list<Entry> EntryList;

  public:
    //! Typical constructor. A default value may be added in time.
    explicit LRUCache(size_t capacity) :
        _capacity(capacity), _data(), _accessMap() {
    }

    //! Lookup operator. Creates elements with default constructed
    //! values for keys that currently are not in the cache.
    Value& operator[](const Key& key) {
      typename AccessMap::const_iterator mapIt = _accessMap.find(key);
      if (mapIt == _accessMap.end()) {
        // Insert a pair with a default constructed value.
        insert(key, Value());
      } else {
        // Move it to the front.
        typename EntryList::iterator listIt = mapIt->second;
        _data.splice(_data.begin(), _data, listIt);
        _accessMap[key] = _data.begin();
      }
      assert(_accessMap.find(key)->second == _data.begin());
      return _data.begin()->second;
    }

    //! Insert a key value pair to the cache.
    void insert(const Key& key, const Value& value) {
      _data.push_front(Entry(key, value));
      _accessMap[key] = _data.begin();
      if (_data.size() > _capacity) {
        // Remove the last element.
        _accessMap.erase(_data.back().first);
        _data.pop_back();
      }
      assert(_data.size() <= _capacity);
    }


    //! Set the capacity.
    void setCapacity(const size_t nofElements) {
      _capacity = nofElements;
      while (_data.size() > _capacity) {
        // Remove the last element.
        _accessMap.erase(_data.back().first);
        _data.pop_back();
      }
      assert(_data.size() <= _capacity);
    }

    //! Checks if there is an entry with the given key.
    bool contains(const Key& key) {
      return _accessMap.count(key) > 0;
    }

    //! Clear the cache
    void clear() {
      _data.clear();
      _accessMap.clear();
    }

  private:
    size_t _capacity;
    EntryList _data;
    AccessMap _accessMap;
};
}
