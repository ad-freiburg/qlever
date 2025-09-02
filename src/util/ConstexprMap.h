//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_CONSTEXPRMAP_H
#define QLEVER_CONSTEXPRMAP_H

#include <stdexcept>

#include "backports/algorithm.h"

namespace ad_utility {

/// A const and constexpr map from `Key`s to `Value`s.
template <typename Key, typename Value, size_t numEntries>
class ConstexprMap {
  using Pair = std::pair<Key, Value>;
  using Arr = std::array<Pair, numEntries>;

 private:
  // TODO make const
  Arr _values;

 public:
  // Create from an Array of key-value pairs. The keys have to be unique.
  explicit constexpr ConstexprMap(Arr values) : _values{std::move(values)} {
    std::sort(_values.begin(), _values.end(), compare);
    if (std::unique(_values.begin(), _values.end(),
                    [](const Pair& a, const Pair& b) {
                      return a.first == b.first;
                    }) != _values.end()) {
      throw std::runtime_error{
          "ConstexprMap requires that all the keys are unique"};
    }
  }

  // If `key` is in the map, return an iterator to the corresponding `(Key,
  // Value)` pair. Else return `end()`.
  constexpr typename Arr::const_iterator find(const Key& key) const {
    auto lb = std::lower_bound(
        _values.begin(), _values.end(), key,
        [](const Pair& a, const Key& b) { return a.first < b; });
    if (lb == _values.end() || lb->first != key) {
      return _values.end();
    }
    return lb;
  }

  // Return true iff the key is contained.
  constexpr bool contains(const Key& key) const {
    return find(key) != _values.end();
  }

  // Return the value for the given key (throws an exception if key does not
  // exist).
  constexpr const Value& at(const Key& key) const {
    auto it = find(key);
    if (it == _values.end()) {
      throw std::out_of_range{"Key was not found in map"};
    }
    return it->second;
  }

 private:
  static constexpr auto compare = [](const Pair& a, const Pair& b) {
    return a.first < b.first;
  };
};

}  // namespace ad_utility

#endif  // QLEVER_CONSTEXPRMAP_H
