//  Copyright 2021, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_CONSTEXPRMAP_H
#define QLEVER_CONSTEXPRMAP_H

#include <stdexcept>
#include <utility>

#include "backports/algorithm.h"

namespace ad_utility {

// The type used to store `key, value` pairs inside the `ConstexprMap` below.
// We cannot use `std::pair` or `std::tuple`, because they are not constexpr in
// C++17. We also cannot use `boost::hana::pair` as it has a strange bug in
// C++17 on GCC 8.3 (Wrong behavior in constexpr mode, correct behavior if the
// `constexpr` keyword is removed.
template <typename Key, typename Value>
struct ConstexprMapPair {
  Key key_;
  Value value_;
  constexpr ConstexprMapPair(Key key, Value value)
      : key_{std::move(key)}, value_{std::move(value)} {}
};

/// A const and constexpr map from `Key`s to `Value`s.
template <typename Key, typename Value, size_t numEntries>
class ConstexprMap {
 public:
  using Pair = ConstexprMapPair<Key, Value>;
  using Arr = std::array<Pair, numEntries>;

  // A lambda to obtain the `key_` from a `pair`.
  // Note: We cannot simply use the pointer-to-member `&Pair::key_`, because
  // then the `ConstexprMap` for some reason is not `constexpr` anymore on
  // `GCC 8.3`.
  static constexpr auto getKey = [](const auto& pair) -> const auto& {
    return pair.key_;
  };

 private:
  // TODO make const
  Arr _values;

 public:
  // Create from an Array of key-value pairs. The keys have to be unique.
  explicit constexpr ConstexprMap(Arr values) : _values{std::move(values)} {
    ql::ranges::sort(_values, compare);
    if (::ranges::adjacent_find(_values, std::equal_to<>{}, &Pair::key_) !=
        _values.end()) {
      throw std::runtime_error{
          "ConstexprMap requires that all the keys are unique"};
    }
  }

  // If `key` is in the map, return an iterator to the corresponding `(Key,
  // Value)` pair. Else return `end()`.
  constexpr typename Arr::const_iterator find(const Key& key) const {
    auto lb = ql::ranges::lower_bound(_values, key, std::less<>{}, &Pair::key_);
    if (lb == _values.end() || lb->key_ != key) {
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
    return it->value_;
  }

 private:
  static constexpr auto compare = [](const Pair& a, const Pair& b) {
    return a.key_ < b.key_;
  };
};

}  // namespace ad_utility

#endif  // QLEVER_CONSTEXPRMAP_H
