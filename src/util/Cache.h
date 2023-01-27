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
 * The strategy that is used to determine which entry is removed once the
 * capacity is exceeded can be customized (see documentation of the template
 * parameters). The capacity can be limited wrt the number of entries as well
 * as their actual size (which can be configured with a callable of type
 * ValueSizeGetter in the constructor below). NOTE: THIS IMPLEMENTATION IS NO
 * LONGER THREADSAFE. It used to be. If you require thread-safety, you should
 * wrap the FlexibleCache object within ad_utility::Synchronized; see class
 * ConcurrentCache. This implementation uses shared_ptr pointers to ensure that
 * delete operations do not free memory that is still in use.
 * @tparam PriorityQueue Container template for a priority queue with an
 * updateKey method. The templates from PrioityQueue.h are suitable
 * @tparam Key The key type for lookup. Must be hashable TODO<joka921>::if
 * needed in future, add hash as optional template parameter
 * @tparam Value Value type. Must be default constructible
 * @tparam Score A type that is used to determine which entry is deleted next
 * from the cache (the lowest entry according to ScoreComparator)
 * @tparam ScoreComparator function of (Score, Score) -> bool that returns true
 * if the first argument is to be deleted before the second
 * @tparam AccessUpdater function (Score, Value) -> Score. Each time a value is
 * accessed, its previous score and the value are used to calculate a new score.
 * @tparam ScoreCalculator function Value -> Score to determine the Score of a a
 * newly inserted entry
 * @tparam ValueSizeGetter function Value -> size_t to determine the actual
 * "size" of a value for statistics
 */
template <template <typename Sc, typename Val, typename Comp>
          class PriorityQueue,
          class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          typename ValueSizeGetter = DefaultSizeGetter<Value>>
class FlexibleCache {
 public:
  // For easier interaction with the STL, which often uses key_type and
  // value_type .
  using key_type = Key;
  using value_type = Value;

 private:
  template <typename K, typename V>
  using MapType = ad_utility::HashMap<K, V>;

  using ValuePtr = shared_ptr<const Value>;

  class Entry {
    Key mKey;
    ValuePtr mValue;

   public:
    Entry(Key k, ValuePtr v) : mKey(std::move(k)), mValue(std::move(v)) {}

    Entry() = default;

    const Key& key() const { return mKey; }

    Key& key() { return mKey; }

    const ValuePtr& value() const { return mValue; }

    ValuePtr& value() { return mValue; }
  };

  using EmplacedValue = shared_ptr<Value>;
  // using Entry = pair<Key, ValuePtr>;
  using EntryList = PriorityQueue<Score, Entry, ScoreComparator>;

  using AccessMap = MapType<Key, typename EntryList::Handle>;
  using PinnedMap = MapType<Key, ValuePtr>;

  using TryEmplaceResult = pair<EmplacedValue, ValuePtr>;

  using EntrySizeType = std::invoke_result_t<ValueSizeGetter, const Value&>;

 public:
  //! Typical constructor. A default value may be added in time.
  explicit FlexibleCache(size_t maxNumEntries, size_t maxSize,
                         size_t maxSizeSingleEntry,
                         ScoreComparator scoreComparator,
                         AccessUpdater accessUpdater,
                         ScoreCalculator scoreCalculator,
                         ValueSizeGetter valueSizeGetter = ValueSizeGetter())
      : _maxNumEntries(maxNumEntries),
        _maxSize(maxSize),
        _maxSizeSingleEntry(maxSizeSingleEntry),
        _entries(scoreComparator),
        _accessUpdater(accessUpdater),
        _scoreCalculator(scoreCalculator),
        _valueSizeGetter(valueSizeGetter) {}

  // Look up a read-only value without creating it.
  // If the key does not exist, nullptr is returned
  ValuePtr operator[](const Key& key) {
    if (const auto pinnedIt = _pinnedMap.find(key);
        pinnedIt != _pinnedMap.end()) {
      return pinnedIt->second;
    }

    const auto mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Returning a null pointer allows to easily check if the entry
      // existed and crash misuses.
      return shared_ptr<Value>(nullptr);
    }

    // Move it to the front.
    auto& handle = mapIt->second;
    _entries.updateKey(_accessUpdater(handle.score(), handle.value()), &handle);
    return _accessMap[key].value().value();
  }

  /// Insert a key-value pair to the cache. Throws an exception if the key is
  /// already present. If the value is too big for the cache, nothing happens.
  ValuePtr insert(const Key& key, Value value) {
    auto ptr = make_shared<Value>(std::move(value));
    return insert(key, std::move(ptr));
  }

  // Insert a pinned key-value pair to the cache. Throws an exception if the key
  // is already present. If the value is too big for the cache, an exception is
  // thrown.
  ValuePtr insertPinned(const Key& key, Value value) {
    auto ptr = make_shared<Value>(std::move(value));
    return insertPinned(key, std::move(ptr));
  }

  /// Insert a key-value pair to the cache. Throws an exception if the key is
  /// already present. Overload for shared_ptr of value. If the value is too big
  /// for the cache, nothing happens.
  ValuePtr insert(const Key& key, shared_ptr<Value> valPtr) {
    if (contains(key)) {
      throw std::runtime_error(
          "Trying to insert a cache key which was already present");
    }

    // Ignore entries that are too big.
    auto sizeOfNewEntry = _valueSizeGetter(*valPtr);
    if (sizeOfNewEntry > _maxSizeSingleEntry) {
      return {};
    }
    if (!makeRoomIfFits(sizeOfNewEntry)) {
      return {};
    }
    Score s = _scoreCalculator(*valPtr);
    _totalSizeNonPinned += _valueSizeGetter(*valPtr);
    auto handle = _entries.insert(std::move(s), Entry(key, std::move(valPtr)));
    _accessMap[key] = handle;
    // The first value is the value part of the key-value pair in the priority
    // queue (where the key is a score and the value is a cache entry). The
    // second value is the value part of the key-value pair in the cache (where
    // the key is the cache key and the value is the payload = in our case, a
    // result).
    return handle.value().value();
  }

  /// Insert a pinned key-value pair to the cache. Throws an exception if the
  /// key is already present. Overload for shared_ptr<Value>. If the value is
  /// too big for the cache, an exception is thrown.
  ValuePtr insertPinned(const Key& key, shared_ptr<Value> valPtr) {
    if (contains(key)) {
      throw std::runtime_error(
          "Trying to insert a cache key which was already present");
    }

    // Throw if an entry is too big for this cache.
    auto sizeOfNewEntry = _valueSizeGetter(*valPtr);
    if (sizeOfNewEntry > _maxSizeSingleEntry) {
      throw std::runtime_error(
          "Trying to pin an entry to the cache that is bigger than the "
          "maximum size for a single entry in the cache");
    }
    // Make room for the new entry.
    makeRoomIfFits(sizeOfNewEntry);
    _pinnedMap[key] = valPtr;
    _totalSizePinned += _valueSizeGetter(*valPtr);
    return valPtr;
  }

  //! Set or change the maximum number of entries
  void setMaxNumEntries(const size_t maxNumEntries) {
    _maxNumEntries = maxNumEntries;
    makeRoomIfFits(0);
  }

  //! Set or change the maximum total size of the cache
  void setMaxSize(const size_t maxSize) {
    _maxSize = maxSize;
    makeRoomIfFits(0);
  }

  //! Set or change the maximum size of a single Entry
  void setMaxSizeSingleEntry(const size_t maxSizeSingleEntry) {
    _maxSizeSingleEntry = maxSizeSingleEntry;
    // We currently do not delete entries that are now too big
    // after the update.
    // TODO<joka921>:: implement this functionality
  }

  //! Checks if there is an entry with the given key.
  bool contains(const Key& key) const {
    return containsPinned(key) || containsNonPinned(key);
  }

  bool containsPinned(const Key& key) const { return _pinnedMap.contains(key); }

  bool containsNonPinned(const Key& key) const {
    return _accessMap.contains(key) && !containsPinned(key);
  }

  //! Checks if there is an entry with the given key. If the entry exists and
  //! was not cachedPinned, it is cachedPinned after the call.
  bool containsAndMakePinnedIfExists(const Key& key) {
    if (_pinnedMap.contains(key)) {
      return true;
    }
    if (!_accessMap.contains(key)) {
      return false;
    }
    auto handle = _accessMap[key];
    const ValuePtr valuePtr = handle.value().value();

    // adapt the sizes of the pinned and non-pinned part of the cache
    auto sz = _valueSizeGetter(*valuePtr);
    _totalSizeNonPinned -= sz;
    _totalSizePinned += sz;
    // Move the entry to the _pinnedMap and remove it from the non-pinned data
    // structures
    _pinnedMap[key] = std::move(valuePtr);
    _entries.erase(std::move(handle));
    _accessMap.erase(key);
    return true;
  }

  //! Erase an entry from the cache if it exists, do nothing otherwise. This
  //! also removes pinned entries.
  void erase(const Key& key) {
    const auto pinnedIt = _pinnedMap.find(key);
    if (pinnedIt != _pinnedMap.end()) {
      _totalSizePinned -= _valueSizeGetter(*pinnedIt->second);
      _pinnedMap.erase(pinnedIt);
      return;
    }

    const auto mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Item already erased do nothing
      return;
    }
    // the entry exists in the non-pinned part of the cache, erase it.
    _totalSizeNonPinned -= _valueSizeGetter(*mapIt->second);
    _entries.erase(std::move(mapIt->second));
    _accessMap.erase(mapIt);
  }

  //! Clear the cache but leave pinned entries alone
  void clearUnpinnedOnly() {
    // Since we are using shared_ptr this does not free the underlying
    // memory if it is still accessible through a previously returned
    // shared_ptr
    _entries.clear();
    _accessMap.clear();
    _totalSizeNonPinned = 0;
  }

  /// Clear the cache AND the pinned entries
  void clearAll() {
    // Since we are using shared_ptr this does not free the underlying
    // memory if it is still accessible through a previously returned
    // shared_ptr
    _entries.clear();
    _pinnedMap.clear();
    _accessMap.clear();
    _totalSizeNonPinned = 0;
    _totalSizePinned = 0;
  }

  /// Return the total size of the pinned entries
  [[nodiscard]] EntrySizeType pinnedSize() const {
    return std::accumulate(
        _pinnedMap.begin(), _pinnedMap.end(), EntrySizeType{},
        [this](const EntrySizeType& x, const auto& el) {
          // entries of the pinned Map are shared_ptrs
          return x +
                 (el.second ? _valueSizeGetter(*el.second) : EntrySizeType{});
        });
  }

  /// Return the total size of the non-pinned cache entries
  [[nodiscard]] EntrySizeType nonPinnedSize() const {
    return std::accumulate(
        _accessMap.begin(), _accessMap.end(), EntrySizeType{},
        [this](const EntrySizeType& x, const auto& el) {
          // entries of the accessMap are shared_ptrs
          return x + _valueSizeGetter(*el.second.value().value());
        });
  }

  /// Return the number of non-pinned cache entries
  [[nodiscard]] size_t numNonPinnedEntries() const { return _accessMap.size(); }

  /// Return the number of pinned entries
  [[nodiscard]] size_t numPinnedEntries() const { return _pinnedMap.size(); }

  // Delete cache entries from the non-pinned area, until an element of size
  // `sizeToMakeRoomFor` can be inserted into the cache.
  // The special case `sizeToMakeRoomFor == 0`  means, that we do not need to
  // insert a new element, but just want to shrink back the cache to its allowed
  // size, for example after a change of capacity
  //
  // Returns: true iff if the procedure succeeded. It may fail if
  // `sizeToMakeRoomFor` is larger than the _maxSize - _totalSizePinned;
  bool makeRoomIfFits(size_t sizeToMakeRoomFor) {
    if (_maxSize - _totalSizePinned < sizeToMakeRoomFor) {
      return false;
    }

    // Used to distinguish between > and >= below.
    const size_t needToAddNewElement = sizeToMakeRoomFor ? 1 : 0;

    while (!_entries.empty() &&
           (_entries.size() + _pinnedMap.size() + needToAddNewElement >
                _maxNumEntries ||
            _totalSizeNonPinned + _totalSizePinned + sizeToMakeRoomFor >
                _maxSize)) {
      // Remove entries from the back until we meet the capacity and size
      // requirements
      removeOneEntry();
    }

    // Note that the pinned entries alone can exceed the capacity of the cache.
    assert(_entries.empty() ||
           _entries.size() + _pinnedMap.size() <= _maxNumEntries);
    return true;
  }

  // Delete entries of a total size of at least `sizeToMakeRoomFor` from the
  // cache. If this is not possible, the cache is cleared (only unpinned
  // elements), and false is returned. This possibly results in some freed
  // space, but less than requested.
  bool makeRoomAsMuchAsPossible(size_t sizeToMakeRoomFor) {
    if (sizeToMakeRoomFor > _totalSizeNonPinned) {
      clearUnpinnedOnly();
      return false;
    }
    size_t targetSize = _totalSizeNonPinned - sizeToMakeRoomFor;
    while (!_entries.empty() && _totalSizeNonPinned > targetSize) {
      removeOneEntry();
    }
    return true;
  }

 private:
  // Removes the entry with the smallest score from the cache.
  // Precondition: The cache must not be empty.
  void removeOneEntry() {
    AD_CONTRACT_CHECK(!_entries.empty());
    auto handle = _entries.pop();
    _totalSizeNonPinned -= _valueSizeGetter(*handle.value().value());
    _accessMap.erase(handle.value().key());
  }
  size_t _maxNumEntries;
  size_t _maxSize;
  size_t _maxSizeSingleEntry;
  size_t _totalSizeNonPinned =
      0;  // the size in terms of the ValueSizeGetter, NOT Number of entries
  size_t _totalSizePinned = 0;

  EntryList _entries;
  AccessUpdater _accessUpdater;
  ScoreCalculator _scoreCalculator;
  ValueSizeGetter _valueSizeGetter;
  PinnedMap _pinnedMap;
  AccessMap _accessMap;
};

// Partial instantiation of FlexibleCache using the heap-based priority queue
// from ad_utility::HeapBasedPQ
template <class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          typename ValueSizeGetter = DefaultSizeGetter<Value>>
using HeapBasedCache =
    ad_utility::FlexibleCache<HeapBasedPQ, Key, Value, Score, ScoreComparator,
                              AccessUpdater, ScoreCalculator, ValueSizeGetter>;

// Partial instantiation of FlexibleCache using the tree-based priority queue
// from ad_utility::TreeBasedPQ
template <class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          typename ValueSizeGetter = DefaultSizeGetter<Value>>
using TreeBasedCache =
    ad_utility::FlexibleCache<TreeBasedPQ, Key, Value, Score, ScoreComparator,
                              AccessUpdater, ScoreCalculator, ValueSizeGetter>;

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
          typename ValueSizeGetter = DefaultSizeGetter<Value>>
class HeapBasedLRUCache
    : public HeapBasedCache<Key, Value, detail::TimePoint, std::less<>,
                            detail::timeUpdater, detail::timeAsScore,
                            ValueSizeGetter> {
  using Base =
      HeapBasedCache<Key, Value, detail::TimePoint, std::less<>,
                     detail::timeUpdater, detail::timeAsScore, ValueSizeGetter>;

 public:
  explicit HeapBasedLRUCache(size_t capacityNumEls = size_t_max,
                             size_t capacitySize = size_t_max,
                             size_t maxSizeSingleEl = size_t_max)
      : Base(capacityNumEls, capacitySize, maxSizeSingleEl, std::less<>(),
             detail::timeUpdater{}, detail::timeAsScore{}, ValueSizeGetter{}) {}
};

/// A LRU cache using the TreeBasedCache
template <typename Key, typename Value,
          typename ValueSizeGetter = DefaultSizeGetter<Value>>
class TreeBasedLRUCache
    : public ad_utility::TreeBasedCache<Key, Value, detail::TimePoint,
                                        std::less<>, detail::timeUpdater,
                                        detail::timeAsScore, ValueSizeGetter> {
  using Base = ad_utility::TreeBasedCache<Key, Value, detail::TimePoint,
                                          std::less<>, detail::timeUpdater,
                                          detail::timeAsScore, ValueSizeGetter>;

 public:
  explicit TreeBasedLRUCache(size_t capacity)
      : Base(capacity, std::less<>(), detail::timeUpdater{},
             detail::timeAsScore{}, ValueSizeGetter{}) {}
};

/// typedef for the simple name LRUCache that is fixed to one of the possible
/// implementations at compiletime
#ifdef _QLEVER_USE_TREE_BASED_CACHE
template <typename Key, typename Value,
          typename ValueSizeGetter = DefaultSizeGetter<Value>>
using LRUCache = TreeBasedLRUCache<Key, Value, ValueSizeGetter>;
#else
template <typename Key, typename Value,
          typename ValueSizeGetter = DefaultSizeGetter<Value>>
using LRUCache = HeapBasedLRUCache<Key, Value, ValueSizeGetter>;
#endif

}  // namespace ad_utility
