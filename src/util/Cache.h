// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Niklas Schnelle (schnelle@informatik.uni-freiburg.de)
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#pragma once

#include <assert.h>
#include <limits>
#include <memory>
#include <mutex>
#include <utility>
#include "./HashMap.h"
#include "PriorityQueue.h"

namespace ad_utility {

using std::make_shared;
using std::pair;
using std::shared_ptr;

static constexpr auto size_t_max = std::numeric_limits<size_t>::max();

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
 * parameters). The capacity can be limited wrt the number of elements as well
 * as their actual size (which is determined by a configurable callable).
 * THIS IMPLEMENTATION IS NO LONGER THREADSAFE. It used to be. If you require
 * thread-safety, we suggest wrapping it in an ad_utility::Synchronized
 * This implementation uses shared_ptrs so as to ensure that deletes do not
 * free in-use memory.
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
 public:
  using key_type = Key;
  using value_type = Value;

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
  explicit FlexibleCache(size_t capacityNumElements, size_t capacitySize,
                         size_t maxSizeSingleEl,
                         ScoreComparator scoreComparator,
                         AccessUpdater accessUpdater,
                         ScoreCalculator scoreCalculator,
                         EntrySizeGetter entrySizeGetter = EntrySizeGetter())
      : _capacityNumElements(capacityNumElements),
        _capacitySize(capacitySize),
        _maxSizeSingleEl(maxSizeSingleEl),
        _data(scoreComparator),
        _accessUpdater(accessUpdater),
        _scoreCalculator(scoreCalculator),
        _entrySizeGetter(entrySizeGetter) {}

  // Look up a read-only value without creating it.
  // If the key does not exist, nullptr is returned
  EntryValue operator[](const Key& key) {
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

  /// Insert a key-value pair to the cache. Throws an exception if the key is
  /// already present.
  /// If the value is too big for the cache, nothing happens.
  EntryValue insert(const Key& key, Value value) {
    auto ptr = make_shared<Value>(std::move(value));
    return insert(key, std::move(ptr));
  }

  // Insert a pinned key-value pair to the cache. Throws an exception if the key
  // is already present.
  /// If the value is too big for the cache, an exception is thrown.
  EntryValue insertPinned(const Key& key, Value value) {
    auto ptr = make_shared<Value>(std::move(value));
    return insertPinned(key, std::move(ptr));
  }

  /// Insert a key-value pair to the cache. Throws an exception if the key is
  /// already present. Overload for shared_ptr of value.
  /// If the value is too big for the cache, nothing happens.
  EntryValue insert(const Key& key, shared_ptr<Value> valPtr) {
    if (contains(key)) {
      throw std::runtime_error(
          "Trying to insert a cache key which was already present");
    }

    // ignore elements that are too big
    if (_entrySizeGetter(*valPtr) > _maxSizeSingleEl) {
      return {};
    }
    Score s = _scoreCalculator(*valPtr);
    _totalSizeNonPinned += _entrySizeGetter(*valPtr);
    auto handle = _data.insert(std::move(s), Entry(key, std::move(valPtr)));
    _accessMap[key] = handle;
    shrinkToFit();
    return handle.value().value();
  }

  /// Insert a pinned key-value pair to the cache. Throws an exception if the
  /// key is already present. Overload for shared_ptr<Value>
  /// If the value is too big for the cache, an exception is thrown.
  EntryValue insertPinned(const Key& key, shared_ptr<Value> valPtr) {
    if (contains(key)) {
      throw std::runtime_error(
          "Trying to insert a cache key which was already present");
    }

    // throw if an element is too big for this cache, and can
    if (_entrySizeGetter(*valPtr) > _maxSizeSingleEl) {
      throw std::runtime_error(
          "Trying to pin an element to the cache that is bigger than the "
          "maximum size for a single element in the cache");
    }
    _pinnedMap[key] = valPtr;
    _totalSizePinned += _entrySizeGetter(*valPtr);
    shrinkToFit();
    return valPtr;
  }

  //! Set the capacity.
  void setCapacity(const size_t nofElements) {
    _capacityNumElements = nofElements;
    shrinkToFit();
  }

  //! Checks if there is an entry with the given key.
  bool contains(const Key& key) const {
    return _pinnedMap.count(key) > 0 || _accessMap.count(key) > 0;
  }

  bool containsPinnedIncludingUpgrade(const Key& key) {
    if (_pinnedMap.count(key) > 0) {
      return true;
    }
    if (!_accessMap.count(key)) {
      return false;
    }
    auto handle = _accessMap[key];
    const EntryValue cached = handle.value().value();
    // Move the element to the _pinnedMap and remove
    // unnecessary _accessMap entry
    _pinnedMap[key] = cached;
    // We have already copied the value to variable "cached", so invalidating
    // the handle is fine
    _data.erase(std::move(handle));
    _accessMap.erase(key);
    auto sz = _entrySizeGetter(*cached);
    _totalSizeNonPinned -= sz;
    _totalSizePinned += sz;
    return true;
  }

  // Erase an item from the cache if it exists, do nothing otherwise. As this
  // erase is explicit do remove pinned elements as well.
  void erase(const Key& key) {
    const auto pinnedIt = _pinnedMap.find(key);
    if (pinnedIt != _pinnedMap.end()) {
      _totalSizePinned -= _entrySizeGetter(*pinnedIt->second);
      _pinnedMap.erase(pinnedIt);
      return;
    }

    const auto mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Item already erased do nothing
      return;
    }
    _totalSizeNonPinned -= _entrySizeGetter(*mapIt->second);
    _data.erase(std::move(mapIt->second));
    _accessMap.erase(mapIt);
  }

  //! Clear the cache but leave pinned elements alone
  void clear() {
    // Since we are using shared_ptr this does not free the underlying
    // memory if it is still accessible through a previously returned
    // shared_ptr
    _data.clear();
    _accessMap.clear();
    _totalSizeNonPinned = 0;
  }

  /// Clear the cache AND the pinned elements
  void clearAll() {
    // Since we are using shared_ptr this does not free the underlying
    // memory if it is still accessible through a previously returned
    // shared_ptr
    _data.clear();
    _pinnedMap.clear();
    _accessMap.clear();
    _totalSizeNonPinned = 0;
    _totalSizePinned = 0;
  }

  /// return the total size of the pinned elements
  [[nodiscard]] EntrySizeType pinnedSize() const {
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
    return std::accumulate(
        _accessMap.begin(), _accessMap.end(), EntrySizeType{},
        [this](const EntrySizeType& x, const auto& el) {
          // elements of the accessMap are shared_ptrs
          return x + _entrySizeGetter(*el.second.value().value());
        });
  }

  /// return the number of cached elements
  [[nodiscard]] size_t numCachedElements() const { return _accessMap.size(); }

  /// return the number of pinned elements
  [[nodiscard]] size_t numPinnedElements() const { return _pinnedMap.size(); }

 private:
  size_t _capacityNumElements;
  size_t _capacitySize;
  size_t _maxSizeSingleEl;
  size_t _totalSizeNonPinned =
      0;  // the size in terms of the EntrySizeGetter, NOT Number of elements
  size_t _totalSizePinned = 0;

  EntryList _data;
  AccessUpdater _accessUpdater;
  ScoreCalculator _scoreCalculator;
  EntrySizeGetter _entrySizeGetter;
  PinnedMap _pinnedMap;
  AccessMap _accessMap;

  FRIEND_TEST(FlexibleLRUCacheTest, testTryEmplacePinnedExistingInternal);
  FRIEND_TEST(LRUCacheTest, testTryEmplacePinnedExistingInternal);

  void shrinkToFit() {
    while (!_data.empty() &&
           (_data.size() + _pinnedMap.size() > _capacityNumElements ||
            _totalSizeNonPinned + _totalSizePinned > _capacitySize)) {
      // Remove elements from the back until we meet the capacity requirement
      // TODO<joka921> : code duplication

      auto handle = _data.pop();
      _totalSizeNonPinned -= _entrySizeGetter(*handle.value().value());
      _accessMap.erase(handle.value().key());
    }
    assert(_data.empty() ||
           _data.size() + _pinnedMap.size() <= _capacityNumElements);
  }
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
                       [[maybe_unused]] const U& u) const {
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
  explicit HeapBasedLRUCache(size_t capacityNumEls,
                             size_t capacitySize = size_t_max,
                             size_t maxSizeSingleEl = size_t_max)
      : Base(capacityNumEls, capacitySize, maxSizeSingleEl, std::less<>(),
             detail::timeUpdater{}, detail::timeAsScore{}, EntrySizeGetter{}) {}
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
