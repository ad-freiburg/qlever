// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Niklas Schnelle (schnelle@informatik.uni-freiburg.de)

#pragma once

#include <assert.h>
#include <memory>
#include <mutex>
#include <utility>
#include "./HashMap.h"
#include "PriorityQueue.h"

namespace ad_utility {

using std::make_shared;
using std::pair;
using std::shared_ptr;

/**
 * Has operator()(const Value&)
 * If Value has a .size() Member function, this is called
 * Else, returns sizeof(Value)
 * @tparam Value
 */
template <typename Value>
struct DefaultSizeGetter {
  template <typename V, typename = void>
  struct HasSizeMember : std::false_type {};

  template <typename V>
  struct HasSizeMember<V, std::void_t<decltype(std::declval<V>().size())>>
      : std::true_type {};

  auto operator()([[maybe_unused]] const Value& value) const {
    if constexpr (HasSizeMember<Value>::value) {
      return value.size();
    } else {
      return sizeof(Value);
    }
  }
};

/**
 * @brief Associative array for almost arbitrary keys and values that acts as a
 * cache with fixed capacity.
 *
 * The strategy that is used to determine which element is removed once the
 * capacity is exceeded can be customized (see documentation of the template
 * parameters). Currently capacity relates to numbers of elements. May be
 * changed to bytes and checking entry sizes in the future. This implementation
 * provides thread safety for the cache itself and also enforces read-only
 * access for all operations which can't guarantee only one thread getting write
 * access. It uses shared_ptrs so as to ensure that deletes do not free in-use
 * memory.
 *
 * @tparam PriorityQueue Container template for a priority queue with an
 * updateKey method. The templates from PrioityQueue.h are suitable
 * @tparam Key The key type for lookup. Must be hashable TODO<joka921>::if
 * needed in future, add hash as optional template parameter
 * @tparam Value Value type. Must be default constructible
 * @tparam Score A type that is used to determine which element is deleted next
 * from the cache (the lowest element according to ScoreComparator)
 * @tparam ScoreComparator function of (Score, Score) -> bool that returns true
 * if the first argument is to be deleted before the second
 * @tparam AccessUpdater function (Score, Value) -> Score. Each time a value is
 * accessed, its previous score and the value are used to calculate a new score.
 * @tparam ScoreCalculator function Value -> Score to determine the Score of a a
 * newly inserted entry
 * @tparam EntrySizeGetter function Value -> size_t to determine the actual
 * "size" of a value for statistics
 */
template <template <typename Sc, typename Val, typename Comp>
          class PriorityQueue,
          class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          typename EntrySizeGetter = DefaultSizeGetter<Value>>
class FlexibleCache {
 private:
  template <typename K, typename V>
  using MapType = ad_utility::HashMap<K, V>;

  using EntryValue = shared_ptr<const Value>;

  class Entry {
    Key mKey;
    EntryValue mValue;

   public:
    Entry(Key k, EntryValue v) : mKey(std::move(k)), mValue(std::move(v)) {}

    Entry() = default;

    const Key& key() const { return mKey; }

    Key& key() { return mKey; }

    const EntryValue& value() const { return mValue; }

    EntryValue& value() { return mValue; }
  };

  using EmplacedValue = shared_ptr<Value>;
  // using Entry = pair<Key, EntryValue>;
  using EntryList = PriorityQueue<Score, Entry, ScoreComparator>;

  using AccessMap = MapType<Key, typename EntryList::Handle>;
  using PinnedMap = MapType<Key, EntryValue>;

  using TryEmplaceResult = pair<EmplacedValue, EntryValue>;

  using EntrySizeType = std::invoke_result_t<EntrySizeGetter, const Value&>;

 public:
  //! Typical constructor. A default value may be added in time.
  explicit FlexibleCache(size_t capacity, ScoreComparator scoreComparator,
                         AccessUpdater accessUpdater,
                         ScoreCalculator scoreCalculator,
                         EntrySizeGetter entrySizeGetter = EntrySizeGetter())
      : _capacity(capacity),
        _data(scoreComparator),
        _accessUpdater(accessUpdater),
        _scoreCalculator(scoreCalculator),
        _entrySizeGetter(entrySizeGetter) {}

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
  // an item in the cache now but it may already be deleted when trying to
  // retrieve it in another operation.
  //
  // If the optional parameter pin is true the emplaced element will
  // never be thrown out of the cache unless removed explicitly.
  template <class... Args>
  TryEmplaceResult tryEmplace(const Key& key, Args&&... args) {
    std::lock_guard lock(_lock);

    if (const auto pinnedIt = _pinnedMap.find(key);
        pinnedIt != _pinnedMap.end()) {
      return TryEmplaceResult(shared_ptr<Value>(nullptr), pinnedIt->second);
    }

    if (const auto mapIt = _accessMap.find(key); mapIt != _accessMap.end()) {
      auto& handle = mapIt->second;
      // Move element to the front as it is now least recently used
      // The handle changes. In the current implementation, only irrelevant
      // parts of the handles should change, but we still do the update
      // TODO<joka921> : measure if this is performance critical.
      _data.updateKey(_accessUpdater(handle.score(), handle.value()), &handle);
      return TryEmplaceResult(EmplacedValue(nullptr),
                              _accessMap[key].value().value());
    }

    // Insert without taking mutex recursively
    EmplacedValue emplaced = make_shared<Value>(std::forward<Args>(args)...);

    _accessMap[key] =
        _data.insert(_scoreCalculator(*emplaced), Entry(key, emplaced));
    if (_data.size() > _capacity) {
      // Remove the last element.
      // Since we are using shared_ptr this does not free the underlying
      // memory if it is still accessible through a previously returned
      // shared_ptr

      _accessMap.erase(_data.pop().value().key());
    }
    assert(_data.size() <= _capacity);
    return TryEmplaceResult(emplaced, emplaced);
  }

  // tryEmplacePinned acts like tryEmplace but pins the emplaced element.
  // This means that it can only be removed explicitly
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
      auto handle = mapIt->second;
      const EntryValue cached = handle.value().value();
      // Move the element to the _pinnedMap and remove
      // unnecessary _accessMap entry
      _pinnedMap[key] = cached;
      // We have already copied the value to variable "cached", so invalidating
      // the handle is fine
      _data.erase(std::move(handle));
      _accessMap.erase(key);
      return TryEmplaceResult(shared_ptr<Value>(nullptr), std::move(cached));
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
    auto& handle = mapIt->second;
    _data.updateKey(_accessUpdater(handle.score(), handle.value()), &handle);
    return _accessMap[key].value().value();
  }

  // Insert a key value pair to the cache.
  // TODO(schnelle) add pinned variant and check pinned
  void insert(const Key& key, Value value) {
    std::lock_guard<std::mutex> lock(_lock);
    Score s = _scoreCalculator(value);
    auto handle = _data.insert(
        std::move(s), Entry(key, make_shared<const Value>(std::move(value))));
    _accessMap[key] = handle;
    // todo<joka921> : codeDuplication
    if (_data.size() > _capacity) {
      // Remove the last element.
      // Since we are using shared_ptr this does not free the underlying
      // memory if it is still accessible through a previously returned
      // shared_ptr

      _accessMap.erase(_data.pop().value().key());
    }
    assert(_data.size() <= _capacity);
  }

  //! Set the capacity.
  void setCapacity(const size_t nofElements) {
    std::lock_guard<std::mutex> lock(_lock);
    _capacity = nofElements;
    while (_data.size() > _capacity) {
      // Remove elements from the back until we meet the capacity requirement
      // TODO<joka921> : code duplication
      _accessMap.erase(_data.pop().value().key());
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
    _data.erase(std::move(mapIt->second));
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

  /// Clear the cache AND the pinned elements
  void clearAll() {
    std::lock_guard<std::mutex> lock(_lock);
    // Since we are using shared_ptr this does not free the underlying
    // memory if it is still accessible through a previously returned
    // shared_ptr
    _data.clear();
    _pinnedMap.clear();
    _accessMap.clear();
  }

  /// return the total size of the pinned elements
  [[nodiscard]] EntrySizeType pinnedSize() const {
    std::lock_guard lock(_lock);
    return std::accumulate(
        _pinnedMap.begin(), _pinnedMap.end(), EntrySizeType{},
        [this](const EntrySizeType& x, const auto& el) {
          // elements of the pinned Map are shared_ptrs
          return x +
                 (el.second ? _entrySizeGetter(*el.second) : EntrySizeType{});
        });
  }

  /// return the total size of the cached elements
  [[nodiscard]] EntrySizeType cachedSize() const {
    std::lock_guard lock(_lock);
    return std::accumulate(
        _accessMap.begin(), _accessMap.end(), EntrySizeType{},
        [this](const EntrySizeType& x, const auto& el) {
          // elements of the pinned Map are shared_ptrs
          return x + _entrySizeGetter(*el.second.value().value());
        });
  }

  /// return the number of cached elements
  [[nodiscard]] size_t numCachedElements() const { return _accessMap.size(); }

  /// return the number of pinned elements
  [[nodiscard]] size_t numPinnedElements() const { return _pinnedMap.size(); }

 private:
  size_t _capacity;
  EntryList _data;
  AccessUpdater _accessUpdater;
  ScoreCalculator _scoreCalculator;
  EntrySizeGetter _entrySizeGetter;
  PinnedMap _pinnedMap;
  AccessMap _accessMap;
  // TODO(schnelle): It would be nice to use std::shared_mutex to only exclude
  // multiple writers.
  // Sadly there is currently no easy way to upgrade a shared_lock to an
  // exclusive lock. Also using a shared_lock here would conflict with moving
  // to the front of the queue in operator[]
  mutable std::mutex _lock;

  FRIEND_TEST(FlexibleLRUCacheTest, testTryEmplacePinnedExistingInternal);
  FRIEND_TEST(LRUCacheTest, testTryEmplacePinnedExistingInternal);
};

// Partial instantiation of FlexibleCache using the heap-based priority queue
// from ad_utility::HeapBasedPQ
template <class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          typename EntrySizeGetter = DefaultSizeGetter<Value>>
using HeapBasedCache =
    ad_utility::FlexibleCache<HeapBasedPQ, Key, Value, Score, ScoreComparator,
                              AccessUpdater, ScoreCalculator, EntrySizeGetter>;

// Partial instantiation of FlexibleCache using the tree-based priority queue
// from ad_utility::TreeBasedPQ
template <class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          typename EntrySizeGetter = DefaultSizeGetter<Value>>
using TreeBasedCache =
    ad_utility::FlexibleCache<TreeBasedPQ, Key, Value, Score, ScoreComparator,
                              AccessUpdater, ScoreCalculator, EntrySizeGetter>;

namespace detail {
// helper types used to implement an LRU cache using the FlexibleCache
// the score is calculated as the access time
// the correct time type used as a score
using TimePoint = std::decay_t<decltype(std::chrono::steady_clock::now())>;
struct timeAsScore {
  template <typename T>
  TimePoint operator()([[maybe_unused]] const T& v) {
    return std::chrono::steady_clock::now();
  }
};

struct timeUpdater {
  template <typename T, typename U>
  TimePoint operator()([[maybe_unused]] const T& v,
                       [[maybe_unused]] const U& u) {
    return std::chrono::steady_clock::now();
  }
};
}  // namespace detail

/// A LRU cache using the HeapBasedCache
template <typename Key, typename Value,
          typename EntrySizeGetter = DefaultSizeGetter<Value>>
class HeapBasedLRUCache
    : public HeapBasedCache<Key, Value, detail::TimePoint, std::less<>,
                            detail::timeUpdater, detail::timeAsScore,
                            EntrySizeGetter> {
  using Base =
      HeapBasedCache<Key, Value, detail::TimePoint, std::less<>,
                     detail::timeUpdater, detail::timeAsScore, EntrySizeGetter>;

 public:
  explicit HeapBasedLRUCache(size_t capacity)
      : Base(capacity, std::less<>(), detail::timeUpdater{},
             detail::timeAsScore{}, EntrySizeGetter{}) {}
};

/// A LRU cache using the TreeBasedCache
template <typename Key, typename Value,
          typename EntrySizeGetter = DefaultSizeGetter<Value>>
class TreeBasedLRUCache
    : public ad_utility::TreeBasedCache<Key, Value, detail::TimePoint,
                                        std::less<>, detail::timeUpdater,
                                        detail::timeAsScore, EntrySizeGetter> {
  using Base = ad_utility::TreeBasedCache<Key, Value, detail::TimePoint,
                                          std::less<>, detail::timeUpdater,
                                          detail::timeAsScore, EntrySizeGetter>;

 public:
  explicit TreeBasedLRUCache(size_t capacity)
      : Base(capacity, std::less<>(), detail::timeUpdater{},
             detail::timeAsScore{}, EntrySizeGetter{}) {}
};

/// typedef for the simple name LRUCache that is fixed to one of the possible
/// implementations at compiletime
#ifdef _QLEVER_USE_TREE_BASED_CACHE
template <typename Key, typename Value,
          typename EntrySizeGetter = DefaultSizeGetter<Value>>
using LRUCache = TreeBasedLRUCache<Key, Value, EntrySizeGetter>;
#else
template <typename Key, typename Value,
          typename EntrySizeGetter = DefaultSizeGetter<Value>>
using LRUCache = HeapBasedLRUCache<Key, Value, EntrySizeGetter>;
#endif

}  // namespace ad_utility
