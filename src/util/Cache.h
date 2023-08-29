// Copyright 2011, University of Freiburg, Chair of Algorithms and Data
// Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
// Author: Niklas Schnelle (schnelle@informatik.uni-freiburg.de)
// Author: Johannes Kalmbach (kalmbacj@informatik.uni-freiburg.de)

#pragma once

#include <assert.h>

#include <concepts>
#include <limits>
#include <memory>
#include <mutex>
#include <type_traits>
#include <utility>

#include "./HashMap.h"
#include "PriorityQueue.h"
#include "util/ConstexprUtils.h"
#include "util/MemorySize/MemorySize.h"
#include "util/TypeTraits.h"

namespace ad_utility {

using std::make_shared;
using std::pair;
using std::shared_ptr;
using namespace ad_utility::memory_literals;

static constexpr auto size_t_max = std::numeric_limits<size_t>::max();

/*
Concept for `ValueSizeGetter`. Must be default initializable, regular invocable
with the given value type as const l-value reference and return a `MemorySize`.
*/
template <typename T, typename ValueType>
concept ValueSizeGetter =
    std::default_initializable<T> &&
    std::regular_invocable<T, const ValueType&> &&
    ad_utility::isSimilar<std::invoke_result_t<T, const ValueType&>,
                          MemorySize>;

/*
 @brief Associative array for almost arbitrary keys and values that acts as a
 cache with fixed memory capacity.

 The strategy that is used to determine which entry is removed once the capacity
 is exceeded can be customized (see documentation of the template parameters).
 NOTE: THIS IMPLEMENTATION IS NO LONGER THREADSAFE. It used to be. If you
 require thread-safety, you should wrap the FlexibleCache object within
 ad_utility::Synchronized; see class ConcurrentCache. This implementation uses
 shared_ptr pointers to ensure that delete operations do not free memory that is
 still in use.
 @tparam PriorityQueue Container template for a priority queue with an
 updateKey method. The templates from PrioityQueue.h are suitable
 @tparam Key The key type for lookup. Must be hashable TODO<joka921>::if
 needed in future, add hash as optional template parameter
 @tparam Value Value type. Must be default constructible.
 @tparam Score A type that is used to determine which entry is deleted next
 from the cache (the lowest entry according to ScoreComparator)
 @tparam ScoreComparator function of (Score, Score) -> bool that returns true
 if the first argument is to be deleted before the second
 @tparam AccessUpdater function (Score, Value) -> Score. Each time a value is
 accessed, its previous score and the value are used to calculate a new score.
 @tparam ScoreCalculator function Value -> Score to determine the Score of a a
 newly inserted entry
 @tparam ValueSizeGetter function Value -> MemorySize to determine the actual
 size of a value for statistics
 */
template <template <typename Sc, typename Val, typename Comp>
          class PriorityQueue,
          class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          ValueSizeGetter<Value> ValueSizeGetter>
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

 public:
  //! Typical constructor. A default value may be added in time.
  explicit FlexibleCache(size_t maxNumEntries, MemorySize maxSize,
                         MemorySize maxSizeSingleEntry,
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
    // TODO Use the normal arithmetical operators, once they are implemented.
    _totalSizeNonPinned = MemorySize::bytes(
        _totalSizeNonPinned.getBytes() + _valueSizeGetter(*valPtr).getBytes());
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
    // TODO Use the normal arithmetical operators, once they are implemented.
    _totalSizePinned = MemorySize::bytes(_totalSizePinned.getBytes() +
                                         _valueSizeGetter(*valPtr).getBytes());
    return valPtr;
  }

  //! Set or change the maximum number of entries
  void setMaxNumEntries(const size_t maxNumEntries) {
    _maxNumEntries = maxNumEntries;
    makeRoomIfFits(0_B);
  }

  //! Set or change the maximum total size of the cache
  void setMaxSize(const MemorySize maxSize) {
    _maxSize = maxSize;
    makeRoomIfFits(0_B);
  }

  //! Set or change the maximum size of a single Entry
  void setMaxSizeSingleEntry(const MemorySize maxSizeSingleEntry) {
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
    // TODO Use the normal arithmetical operators, once they are implemented.
    _totalSizeNonPinned =
        MemorySize::bytes(_totalSizeNonPinned.getBytes() - sz.getBytes());
    _totalSizePinned =
        MemorySize::bytes(_totalSizePinned.getBytes() + sz.getBytes());
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
      // TODO Use the normal arithmetical operators, once they are implemented.
      _totalSizePinned =
          MemorySize::bytes(_totalSizePinned.getBytes() -
                            _valueSizeGetter(*pinnedIt->second).getBytes());
      _pinnedMap.erase(pinnedIt);
      return;
    }

    const auto mapIt = _accessMap.find(key);
    if (mapIt == _accessMap.end()) {
      // Item already erased do nothing
      return;
    }
    // the entry exists in the non-pinned part of the cache, erase it.
    _totalSizeNonPinned =
        MemorySize::bytes(_totalSizeNonPinned.getBytes() -
                          _valueSizeGetter(*mapIt->second).getBytes());
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
    _totalSizeNonPinned = 0_B;
  }

  /// Clear the cache AND the pinned entries
  void clearAll() {
    // Since we are using shared_ptr this does not free the underlying
    // memory if it is still accessible through a previously returned
    // shared_ptr
    _entries.clear();
    _pinnedMap.clear();
    _accessMap.clear();
    _totalSizeNonPinned = 0_B;
    _totalSizePinned = 0_B;
  }

  /// Return the total size of the pinned entries
  [[nodiscard]] MemorySize pinnedSize() const {
    // TODO Use the normal arithmetical operators, once they are implemented.
    return MemorySize::bytes(std::accumulate(
        _pinnedMap.begin(), _pinnedMap.end(), 0UL,
        [this](const size_t& x, const auto& el) {
          // entries of the pinned Map are shared_ptrs
          return x +
                 (el.second ? _valueSizeGetter(*el.second).getBytes() : 0UL);
        }));
  }

  /// Return the total size of the non-pinned cache entries
  [[nodiscard]] MemorySize nonPinnedSize() const {
    // TODO Use the normal arithmetical operators, once they are implemented.
    return MemorySize::bytes(std::accumulate(
        _accessMap.begin(), _accessMap.end(), 0UL,
        [this](const size_t& x, const auto& el) {
          // entries of the accessMap are shared_ptrs
          return x + _valueSizeGetter(*el.second.value().value()).getBytes();
        }));
  }

  /// Return the number of non-pinned cache entries
  [[nodiscard]] size_t numNonPinnedEntries() const { return _accessMap.size(); }

  /// Return the number of pinned entries
  [[nodiscard]] size_t numPinnedEntries() const { return _pinnedMap.size(); }

  // Delete cache entries from the non-pinned area, until an element of size
  // `sizeToMakeRoomFor` can be inserted into the cache.
  // The special case `sizeToMakeRoomFor == 0_B`  means, that we do not need to
  // insert a new element, but just want to shrink back the cache to its allowed
  // size, for example after a change of capacity
  //
  // Returns: true iff if the procedure succeeded. It may fail if
  // `sizeToMakeRoomFor` is larger than the _maxSize - _totalSizePinned;
  bool makeRoomIfFits(MemorySize sizeToMakeRoomFor) {
    // TODO Use the normal arithmetical operators, once they are implemented.
    if (MemorySize::bytes(_maxSize.getBytes() - _totalSizePinned.getBytes()) <
        sizeToMakeRoomFor) {
      return false;
    }

    // Used to distinguish between > and >= below.
    const size_t needToAddNewElement =
        sizeToMakeRoomFor.getBytes() != 0 ? 1 : 0;

    // TODO Use the normal arithmetical operators, once they are implemented.
    while (!_entries.empty() &&
           (_entries.size() + _pinnedMap.size() + needToAddNewElement >
                _maxNumEntries ||
            MemorySize::bytes(_totalSizeNonPinned.getBytes() +
                              _totalSizePinned.getBytes() +
                              sizeToMakeRoomFor.getBytes()) > _maxSize)) {
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
  bool makeRoomAsMuchAsPossible(MemorySize sizeToMakeRoomFor) {
    if (sizeToMakeRoomFor > _totalSizeNonPinned) {
      clearUnpinnedOnly();
      return false;
    }
    // TODO Use the normal arithmetical operators, once they are implemented.
    MemorySize targetSize = MemorySize::bytes(_totalSizeNonPinned.getBytes() -
                                              sizeToMakeRoomFor.getBytes());
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
    // TODO Use the normal arithmetical operators, once they are implemented.
    _totalSizeNonPinned =
        MemorySize::bytes(_totalSizeNonPinned.getBytes() -
                          _valueSizeGetter(*handle.value().value()).getBytes());
    _accessMap.erase(handle.value().key());
  }
  size_t _maxNumEntries;
  MemorySize _maxSize;
  MemorySize _maxSizeSingleEntry;
  MemorySize _totalSizeNonPinned;  // the size in terms of the ValueSizeGetter,
                                   // NOT Number of entries
  MemorySize _totalSizePinned;

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
          ValueSizeGetter<Value> ValueSizeGetter>
using HeapBasedCache =
    ad_utility::FlexibleCache<HeapBasedPQ, Key, Value, Score, ScoreComparator,
                              AccessUpdater, ScoreCalculator, ValueSizeGetter>;

// Partial instantiation of FlexibleCache using the tree-based priority queue
// from ad_utility::TreeBasedPQ
template <class Key, class Value, typename Score, typename ScoreComparator,
          typename AccessUpdater, typename ScoreCalculator,
          ValueSizeGetter<Value> ValueSizeGetter>
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
template <typename Key, typename Value, ValueSizeGetter<Value> ValueSizeGetter>
class HeapBasedLRUCache
    : public HeapBasedCache<Key, Value, detail::TimePoint, std::less<>,
                            detail::timeUpdater, detail::timeAsScore,
                            ValueSizeGetter> {
  using Base =
      HeapBasedCache<Key, Value, detail::TimePoint, std::less<>,
                     detail::timeUpdater, detail::timeAsScore, ValueSizeGetter>;

 public:
  explicit HeapBasedLRUCache(
      size_t capacityNumEls = size_t_max,
      MemorySize capacitySize = MemorySize::bytes(size_t_max),
      MemorySize maxSizeSingleEl = MemorySize::bytes(size_t_max))
      : Base(capacityNumEls, capacitySize, maxSizeSingleEl, std::less<>(),
             detail::timeUpdater{}, detail::timeAsScore{}, ValueSizeGetter{}) {}
};

/// A LRU cache using the TreeBasedCache
template <typename Key, typename Value, ValueSizeGetter<Value> ValueSizeGetter>
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
template <typename Key, typename Value, ValueSizeGetter<Value> ValueSizeGetter>
using LRUCache = TreeBasedLRUCache<Key, Value, ValueSizeGetter>;
#else
template <typename Key, typename Value, ValueSizeGetter<Value> ValueSizeGetter>
using LRUCache = HeapBasedLRUCache<Key, Value, ValueSizeGetter>;
#endif

}  // namespace ad_utility
