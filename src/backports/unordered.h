// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_BACKPORTS_UNORDERED_H
#define QLEVER_SRC_BACKPORTS_UNORDERED_H

// This file defines the class templates that all the hash maps and hash sets
// of QLever are based on (see `util/HashMap.h`, `util/HashSet.h`, and
// `util/ContainersWithAllocator.h`), as well as helpers that work for all of
// them (`ql::erase_if`, `ql::heterogeneous`, and the functions in the
// `ql::unordered` namespace).
//
// If available, the class templates are the open-addressing hash containers
// from `boost::unordered`, which are fast and (unlike the `absl` containers)
// exception safe. The flat containers (`boost::unordered_flat_map` and
// `boost::unordered_flat_set`) exist since Boost 1.81, the node containers
// (`boost::unordered_node_map` and `boost::unordered_node_set`) since Boost
// 1.82. Otherwise, we use (thin wrappers around) `std::unordered_map` and
// `std::unordered_set` (in particular for the Boost 1.71 that is used by the
// `CPP17 libQLever` CI workflow). The Boost containers can be disabled by
// defining `QLEVER_NO_BOOST_UNORDERED`, which is useful to test the fallback.

#include <boost/version.hpp>
#include <cstddef>
#include <functional>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "backports/algorithm.h"

#if BOOST_VERSION >= 108100 && !defined(QLEVER_NO_BOOST_UNORDERED)
#define QLEVER_HAS_BOOST_UNORDERED_FLAT
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>
#endif

#if BOOST_VERSION >= 108200 && !defined(QLEVER_NO_BOOST_UNORDERED)
#define QLEVER_HAS_BOOST_UNORDERED_NODE
#include <boost/unordered/unordered_node_map.hpp>
#include <boost/unordered/unordered_node_set.hpp>
#endif

namespace ql {

namespace backports {
// The fallbacks for the Boost containers: `std::unordered_map` and
// `std::unordered_set` with a `contains` member function also in C++17. The
// lookup functions deliberately only take the `key_type`, also in C++20 (where
// the `std` containers support heterogeneous lookups with transparent hash and
// equality functions), so that a heterogeneous lookup that doesn't compile in
// C++17 is already detected in C++20. Use the functions from the
// `ql::unordered` namespace below for heterogeneous lookups.
template <typename Base>
class StdUnorderedContainer : public Base {
 public:
  using Base::Base;
  using typename Base::const_iterator;
  using typename Base::iterator;
  using typename Base::key_type;
  using typename Base::size_type;

  iterator find(const key_type& key) { return Base::find(key); }
  const_iterator find(const key_type& key) const { return Base::find(key); }
  size_type count(const key_type& key) const { return Base::count(key); }
  bool contains(const key_type& key) const {
    return Base::find(key) != Base::end();
  }
};

template <typename... Ts>
class StdUnorderedMap
    : public StdUnorderedContainer<std::unordered_map<Ts...>> {
  using StdUnorderedContainer<std::unordered_map<Ts...>>::StdUnorderedContainer;
};

template <typename... Ts>
class StdUnorderedSet
    : public StdUnorderedContainer<std::unordered_set<Ts...>> {
  using StdUnorderedContainer<std::unordered_set<Ts...>>::StdUnorderedContainer;
};
}  // namespace backports

// The class templates for hash maps and hash sets. The elements of the `Node`
// containers never move, the elements of the `Flat` containers may move when
// the container grows.
#ifdef QLEVER_HAS_BOOST_UNORDERED_FLAT
template <typename... Ts>
using FlatHashMapTemplate = boost::unordered_flat_map<Ts...>;
template <typename... Ts>
using FlatHashSetTemplate = boost::unordered_flat_set<Ts...>;
#else
template <typename... Ts>
using FlatHashMapTemplate = backports::StdUnorderedMap<Ts...>;
template <typename... Ts>
using FlatHashSetTemplate = backports::StdUnorderedSet<Ts...>;
#endif

#ifdef QLEVER_HAS_BOOST_UNORDERED_NODE
template <typename... Ts>
using NodeHashMapTemplate = boost::unordered_node_map<Ts...>;
template <typename... Ts>
using NodeHashSetTemplate = boost::unordered_node_set<Ts...>;
#else
template <typename... Ts>
using NodeHashMapTemplate = backports::StdUnorderedMap<Ts...>;
template <typename... Ts>
using NodeHashSetTemplate = backports::StdUnorderedSet<Ts...>;
#endif

// Make `ql::erase_if` work for all the above hash containers. In C++20 mode,
// `ql::erase_if` already is `std::erase_if`, which covers the
// `std::unordered_...` containers.
namespace backports {
template <typename Container, typename Pred>
typename Container::size_type eraseIfFromUnorderedContainer(Container& c,
                                                            Pred& pred) {
  auto oldSize = c.size();
  for (auto it = c.begin(); it != c.end();) {
    if (pred(*it)) {
      it = c.erase(it);
    } else {
      ++it;
    }
  }
  return oldSize - c.size();
}

template <typename K, typename V, typename H, typename E, typename A,
          typename Pred>
typename std::unordered_map<K, V, H, E, A>::size_type erase_if(
    std::unordered_map<K, V, H, E, A>& c, Pred pred) {
  return eraseIfFromUnorderedContainer(c, pred);
}

template <typename K, typename H, typename E, typename A, typename Pred>
typename std::unordered_set<K, H, E, A>::size_type erase_if(
    std::unordered_set<K, H, E, A>& c, Pred pred) {
  return eraseIfFromUnorderedContainer(c, pred);
}
}  // namespace backports

#ifdef QLEVER_CPP_17
using backports::erase_if;
#endif
#ifdef QLEVER_HAS_BOOST_UNORDERED_FLAT
using boost::unordered::erase_if;
#endif

namespace unordered::detail {
template <typename T, typename = void>
struct IsTransparent : std::false_type {};
template <typename T>
struct IsTransparent<T, std::void_t<typename T::is_transparent>>
    : std::true_type {};

template <typename Hash, typename KeyEqual>
using IsTransparentHashAndEq =
    std::bool_constant<IsTransparent<Hash>::value &&
                       IsTransparent<KeyEqual>::value>;

// The Boost containers support heterogeneous versions of all the functions in
// the `ql::unordered` namespace below since Boost 1.82 (Boost 1.81 only has
// heterogeneous lookups, but no heterogeneous insertions), if their hash and
// equality functions are transparent. Other containers (in particular the
// `std::unordered_...` containers in C++17) are treated as not supporting
// heterogeneous lookups.
template <typename T>
struct SupportsHeterogeneousLookup : std::false_type {};
#ifdef QLEVER_HAS_BOOST_UNORDERED_NODE
template <typename K, typename V, typename H, typename E, typename A>
struct SupportsHeterogeneousLookup<boost::unordered_flat_map<K, V, H, E, A>>
    : IsTransparentHashAndEq<H, E> {};
template <typename K, typename H, typename E, typename A>
struct SupportsHeterogeneousLookup<boost::unordered_flat_set<K, H, E, A>>
    : IsTransparentHashAndEq<H, E> {};
template <typename K, typename V, typename H, typename E, typename A>
struct SupportsHeterogeneousLookup<boost::unordered_node_map<K, V, H, E, A>>
    : IsTransparentHashAndEq<H, E> {};
template <typename K, typename H, typename E, typename A>
struct SupportsHeterogeneousLookup<boost::unordered_node_set<K, H, E, A>>
    : IsTransparentHashAndEq<H, E> {};
#endif
}  // namespace unordered::detail

// True iff all the functions in the `ql::unordered` namespace below can
// directly be called with a key of a type that is different from the
// `key_type` of the `Container` (e.g. a `std::string_view` for a `std::string`
// key) without materializing a `key_type`. This requires a Boost container
// with transparent hash and equality functions (the defaults from `absl` are
// transparent for strings), see above.
template <typename Container>
constexpr bool supportsHeterogeneousLookup =
    unordered::detail::SupportsHeterogeneousLookup<Container>::value;

// Return `function(key, container)`. If the `container` doesn't support
// heterogeneous lookups (see above), the `key` is first converted to the
// `key_type` of the `container`. This avoids the materialization of the `key`
// whenever possible. For example, for a hash map with `std::string` keys,
// `ql::heterogeneous(stringView, map, [](auto&& key, auto& map) { return
// map.find(AD_FWD(key)); })` materializes a `std::string` only if `map` cannot
// directly be searched for a `std::string_view`.
template <typename Key, typename Container, typename Function>
decltype(auto) heterogeneous(Key&& key, Container& container,
                             Function&& function) {
  using C = std::remove_const_t<Container>;
  using KeyType = typename C::key_type;
  if constexpr (supportsHeterogeneousLookup<C> ||
                std::is_same_v<std::decay_t<Key>, KeyType>) {
    return std::invoke(std::forward<Function>(function), std::forward<Key>(key),
                       container);
  } else {
    return std::invoke(std::forward<Function>(function),
                       KeyType(std::forward<Key>(key)), container);
  }
}

// The member functions of the hash containers that take a key, as free
// functions that take the container as the first argument, and avoid the
// materialization of the key whenever possible (see `ql::heterogeneous`).
namespace unordered {

// Return `container.find(key)`.
template <typename Container, typename Key>
auto find(Container& container, Key&& key) {
  return heterogeneous(
      std::forward<Key>(key), container,
      [](auto&& k, auto& c) { return c.find(std::forward<decltype(k)>(k)); });
}

// Return true iff the `container` contains the `key`. (We don't use the
// `contains` member function, as it doesn't exist for the `std::unordered_...`
// containers in C++17.)
template <typename Container, typename Key>
bool contains(const Container& container, Key&& key) {
  return heterogeneous(std::forward<Key>(key), container,
                       [](auto&& k, const auto& c) {
                         return c.find(std::forward<decltype(k)>(k)) != c.end();
                       });
}

// Return `container.count(key)`.
template <typename Container, typename Key>
size_t count(const Container& container, Key&& key) {
  return heterogeneous(std::forward<Key>(key), container,
                       [](auto&& k, const auto& c) {
                         return c.count(std::forward<decltype(k)>(k));
                       });
}

// Return `container.at(key)`.
template <typename Container, typename Key>
decltype(auto) at(Container& container, Key&& key) {
  return heterogeneous(std::forward<Key>(key), container,
                       [](auto&& k, auto& c) -> decltype(auto) {
                         return c.at(std::forward<decltype(k)>(k));
                       });
}

// Return `container.erase(key)` (the number of erased elements).
template <typename Container, typename Key>
size_t erase(Container& container, Key&& key) {
  return heterogeneous(
      std::forward<Key>(key), container,
      [](auto&& k, auto& c) { return c.erase(std::forward<decltype(k)>(k)); });
}

// Return `set.insert(key)`.
template <typename Set, typename Key>
auto insert(Set& set, Key&& key) {
  return heterogeneous(std::forward<Key>(key), set, [](auto&& k, auto& s) {
    return s.insert(std::forward<decltype(k)>(k));
  });
}

// Return `map.try_emplace(key, args...)`.
template <typename Map, typename Key, typename... Args>
auto try_emplace(Map& map, Key&& key, Args&&... args) {
  return heterogeneous(std::forward<Key>(key), map,
                       [&args...](auto&& k, auto& m) {
                         return m.try_emplace(std::forward<decltype(k)>(k),
                                              std::forward<Args>(args)...);
                       });
}

// Return `map.insert_or_assign(key, value)`.
template <typename Map, typename Key, typename Value>
auto insert_or_assign(Map& map, Key&& key, Value&& value) {
  return heterogeneous(std::forward<Key>(key), map,
                       [&value](auto&& k, auto& m) {
                         return m.insert_or_assign(std::forward<decltype(k)>(k),
                                                   std::forward<Value>(value));
                       });
}

// Return `map[key]`.
template <typename Map, typename Key>
decltype(auto) subscript(Map& map, Key&& key) {
  return heterogeneous(std::forward<Key>(key), map,
                       [](auto&& k, auto& m) -> decltype(auto) {
                         return m[std::forward<decltype(k)>(k)];
                       });
}

}  // namespace unordered
}  // namespace ql

// Make the Boost hash sets hashable with `absl::Hash` (found via ADL), in the
// same way as the `absl` and the `std` hash sets. This is required for example
// for an `ad_utility::HashSet<ad_utility::HashSet<T>>`.
#ifdef QLEVER_HAS_BOOST_UNORDERED_FLAT
namespace boost::unordered {
template <typename H, typename T, typename Hash, typename Eq, typename Alloc>
H AbslHashValue(H h, const unordered_flat_set<T, Hash, Eq, Alloc>& set) {
  return H::combine(H::combine_unordered(std::move(h), set.begin(), set.end()),
                    set.size());
}
#ifdef QLEVER_HAS_BOOST_UNORDERED_NODE
template <typename H, typename T, typename Hash, typename Eq, typename Alloc>
H AbslHashValue(H h, const unordered_node_set<T, Hash, Eq, Alloc>& set) {
  return H::combine(H::combine_unordered(std::move(h), set.begin(), set.end()),
                    set.size());
}
#endif
}  // namespace boost::unordered
#endif

#endif  // QLEVER_SRC_BACKPORTS_UNORDERED_H
