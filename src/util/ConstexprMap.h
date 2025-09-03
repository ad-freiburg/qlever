//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_CONSTEXPRMAP_H
#define QLEVER_CONSTEXPRMAP_H

#include <boost/hana/tuple.hpp>
#include <stdexcept>

#include "backports/algorithm.h"

namespace ad_utility {

// The type used to store `key, value` pairs inside the `ConstexprMap` below.
// We use `boost::hana::pair` because it is `constexpr` in C++17.
template <typename Key, typename Value>
using ConstexprMapPair = boost::hana::pair<Key, Value>;

/// A const and constexpr map from `Key`s to `Value`s.
template <typename Key, typename Value, size_t numEntries>
class ConstexprMap {
 public:
  using Pair = ConstexprMapPair<Key, Value>;
  using Arr = std::array<Pair, numEntries>;

 private:
  // TODO make const
  Arr _values;

 public:
  // Create from an Array of key-value pairs. The keys have to be unique.
  explicit constexpr ConstexprMap(Arr values) : _values{std::move(values)} {
    ql::ranges::sort(_values, compare);
    if (::ranges::adjacent_find(_values, std::equal_to<>{},
                                boost::hana::first) != _values.end()) {
      throw std::runtime_error{
          "ConstexprMap requires that all the keys are unique"};
    }
  }

  // If `key` is in the map, return an iterator to the corresponding `(Key,
  // Value)` pair. Else return `end()`.
  constexpr typename Arr::const_iterator find(const Key& key) const {
    auto lb = ql::ranges::lower_bound(_values, key, std::less<>{},
                                      boost::hana::first);
    if (lb == _values.end() || boost::hana::first(*lb) != key) {
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
    return boost::hana::second(*it);
  }

 private:
  static constexpr auto compare = [](const Pair& a, const Pair& b) {
    return boost::hana::first(a) < boost::hana::first(b);
  };
};

}  // namespace ad_utility

#endif  // QLEVER_CONSTEXPRMAP_H
