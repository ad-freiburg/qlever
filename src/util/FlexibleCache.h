// Copyright 2020, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Niklas Schnelle (schnelle@informatik.uni-freiburg.de)
// Author: Johannes Kalmbach (johannes.kalmbach@gmail.com)

#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <utility>
#include <queue>
#include <map>
#include <algorithm>
#include "./HashMap.h"
#include "Exception.h"

namespace ad_utility {
using std::list;
using std::make_shared;
using std::pair;
using std::shared_ptr;


template<class Score, class Value, class Comp>
class SortedPQ {
  public:
  SortedPQ(Comp c) : mMap(makeComparator(c)) {}
  struct VT {
    Score mScore;
    Value mValue;
  };

  VT insert(Score s, Value v) {
    VT handle {std::move(s), std::move(v)};
    mMap.insert(handle);
    return handle;
  }

  VT pop() {
      auto VT = std::move(*mMap.begin());
      mMap.erase(mMap.begin());
      return VT;
  }

  VT updateKey(Score newKey, const VT& handle) {
    // TODO<joka921>: make this work without constructing a VT explicitly
    auto [beg, end] = mMap.equal_range(VT{handle.mScore, Value{}});
    auto it = std::find_if(beg, end, [&](const auto& a){return a.mValue == handle.mValue;});
    AD_CHECK(it != end);
    mMap.erase(it);
    return insert(newKey, handle.mValue);
  }

  [[nodiscard]] size_t size() const { return mMap.size();}
private:
  static auto makeComparator(Comp comp){
    return [comp](const auto& a, const auto& b) {return comp(a.mScore, b.mScore);};
  }
  std::multiset<VT, std::decay_t<decltype(makeComparator(std::declval<Comp>()))>> mMap;


};

template<class Score, class Value, class Comp>
class PQ {
public:

  struct PqNode {
    PqNode(Score s, Value&& v): mScore(s), mValue(std::move(v)) {}
    Score mScore;
    Value mValue;
  };
  using IntermPtr = shared_ptr<PqNode>;

  struct PqEntry {
    PqEntry(Score s, const IntermPtr& ptr) : mScore(s), mPtr(ptr){}
    Score mScore;
    IntermPtr mPtr;
  };
  static auto makeComparator(Comp comp){
    // std::priority_queue is a max_heap by default, this makes it a min_heap,
    // s.t. the two implementations behave similarly
    return [comp](const PqEntry& a, const PqEntry& b) {return !comp(a.mScore, b.mScore);};
  }

  PQ(Comp comp): _pq(makeComparator(comp)) {}

  std::priority_queue<PqEntry, std::vector<PqEntry>, std::decay_t<decltype(makeComparator(std::declval<Comp>()))>> _pq;

  shared_ptr<PqNode> insert(Score s, Value v) {
    auto handle = make_shared<PqNode>(s, std::move(v));
    auto entry = PqEntry(s, handle);
    _pq.emplace(std::move(entry));
    mSize++;
    return handle;
  }

  IntermPtr pop() {
    pruneChangedKeys();
    auto handle = std::move(_pq.top().mPtr);
    _pq.pop();
    mSize--;
    return handle;
  }

  void updateKey(const Score& newKey, shared_ptr<PqNode> ptr) {
    if (newKey == ptr->mScore) {
      return;
    }
    ptr->mScore = newKey;
    _pq.emplace(newKey, std::move(ptr));
  }

  /// The size of the internal std::priority_queue. Might be bigger than the actual size due to
  /// Multiple entries for the same value with "old" keys.
  size_t technicalSize() const { return _pq.size();}
  size_t size() const {return mSize;}

private:
  size_t mSize = 0;
  void pruneChangedKeys() {
    while(!_pq.empty()) {
      const auto& top = _pq.top();
      if (top.mScore == top.mPtr->mScore) {
        return;
      }
      // a mismatch means, that somebody has changed the key, and this value is no longer
      // valid.
      _pq.pop();
    }
  }
};
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
/*
template <class Key, class Value, typename Score, typename ScoreComparator>
class FlexibleCache {
 private:
  template <typename K, typename V>
  using MapType = ad_utility::HashMap<K, V>;

  using EntryValue = shared_ptr<const Value>;
  using EmplacedValue = shared_ptr<Value>;
  using Entry = pair<Key, EntryValue>;
  using EntryList = list<Entry>;

  using AccessMap = MapType<Key, typename EntryList::iterator>;
  using PinnedMap = MapType<Key, EntryValue>;

  using TryEmplaceResult = pair<EmplacedValue, EntryValue>;



 public:
  //! Typical constructor. A default value may be added in time.
  explicit LRUCache(size_t capacity)
      : _capacity(capacity), _data(), _pinnedMap(), _accessMap(), _lock() {}

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
  //
  // If the optional parameter pin is true the emplaced element will
  // never be thrown out of the cache unless removed explicitly.
  template <class... Args>
  TryEmplaceResult tryEmplace(const Key& key, Args&&... args) {
    std::lock_guard<std::mutex> lock(_lock);

    if (const auto pinnedIt = _pinnedMap.find(key);
        pinnedIt != _pinnedMap.end()) {
      return TryEmplaceResult(shared_ptr<Value>(nullptr), pinnedIt->second);
    }

    if (const auto mapIt = _accessMap.find(key); mapIt != _accessMap.end()) {
      typename EntryList::const_iterator listIt = mapIt->second;
      const EntryValue cached = listIt->second;
      // Move element to the front as it is now least recently used
      _data.splice(_data.begin(), _data, listIt);
      _accessMap[key] = _data.begin();
      return TryEmplaceResult(shared_ptr<Value>(nullptr), cached);
    }

    // Insert without taking mutex recursively
    EmplacedValue emplaced = make_shared<Value>(std::forward<Args>(args)...);

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
    return TryEmplaceResult(emplaced, emplaced);
  }

  // tryEmplacePinned acts like tryEmplace but if the pin paramater is true
  // also marks the emplaced element so that it can only be removed explicitly
  // and not dropped from the cache otherwise. If the element already was in the
  // cache it is also marked as pinned.
  template <class... Args>
  TryEmplaceResult tryEmplacePinned(const Key& key, Args&&... args) {
    std::lock_guard<std::mutex> lock(_lock);

    if (const auto pinnedIt = _pinnedMap.find(key);
        pinnedIt != _pinnedMap.end()) {
      return TryEmplaceResult(shared_ptr<Value>(nullptr), pinnedIt->second);
    }

    if (const auto mapIt = _accessMap.find(key); mapIt != _accessMap.end()) {
      typename EntryList::const_iterator listIt = mapIt->second;
      const EntryValue cached = listIt->second;
      // Move the element to the _pinnedMap and remove
      // unnecessary _accessMap entry
      _pinnedMap[key] = cached;
      _data.erase(listIt);
      _accessMap.erase(key);
      return TryEmplaceResult(shared_ptr<Value>(nullptr), cached);
    }

    // Insert without taking mutex recursively
    EmplacedValue emplaced = make_shared<Value>(std::forward<Args>(args)...);
    _pinnedMap[key] = emplaced;
    return TryEmplaceResult(emplaced, emplaced);
  }

  // Lookup a read-only value without creating a new one if non exists
  // instead shared_ptr<const Value>(nullptr) is returned in that case
  EntryValue operator[](const Key& key) {
    std::lock_guard<std::mutex> lock(_lock);
    if (const auto pinnedIt = _pinnedMap.find(key);
        pinnedIt != _pinnedMap.end()) {
      return pinnedIt->second;
    }

    const auto mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Returning a null pointer allows to easily check if the element
      // existed and crash misuses.
      return shared_ptr<Value>(nullptr);
    }

    // Move it to the front.
    typename EntryList::iterator listIt = mapIt->second;
    _data.splice(_data.begin(), _data, listIt);
    _accessMap[key] = _data.begin();
    return _data.front().second;
  }

  // Insert a key value pair to the cache.
  // TODO(schnelle) add pinned variant and check pinned
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
      // Remove elements from the back until we meet the capacity requirement
      _accessMap.erase(_data.back().first);
      _data.pop_back();
    }
    assert(_data.size() <= _capacity);
  }

  //! Checks if there is an entry with the given key.
  bool contains(const Key& key) {
    std::lock_guard<std::mutex> lock(_lock);
    return _pinnedMap.count(key) > 0 || _accessMap.count(key) > 0;
  }

  // Erase an item from the cache if it exists, do nothing otherwise. As this
  // erase is explicit do remove pinned elements as well.
  void erase(const Key& key) {
    std::lock_guard<std::mutex> lock(_lock);
    const auto pinnedIt = _pinnedMap.find(key);
    if (pinnedIt != _pinnedMap.end()) {
      _pinnedMap.erase(pinnedIt);
      return;
    }

    const auto mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Item already erased do nothing
      return;
    }
    const auto listIt = mapIt->second;
    _data.erase(listIt);
    _accessMap.erase(mapIt);
  }

  //! Clear the cache but leave pinned elements alone
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
  PinnedMap _pinnedMap;
  AccessMap _accessMap;
  // TODO(schnelle): It would be nice to use std::shared_mutex to only exclude
  // multiple writers.
  // Sadly there is currently no easy way to upgrade a shared_lock to an
  // exclusive lock. TUtfhus using a shared_lock here would conflict with moving to
  // the front of the queue in operator[]
  std::mutex _lock;

  FRIEND_TEST(LRUCacheTest, testTryEmplacePinnedExistingInternal);
};  // namespace ad_utility
}  // namespace ad_utility
 */
}  // namespace ad_utility
