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
// `util/ContainersWithAllocator.h`), as well as `ql::erase_if` for all of them.
//
// If available, the class templates are the open-addressing hash containers
// from `boost::unordered`, which are fast and (unlike the `absl` containers)
// exception safe. The flat containers (`boost::unordered_flat_map` and
// `boost::unordered_flat_set`) exist since Boost 1.81, the node containers
// (`boost::unordered_node_map` and `boost::unordered_node_set`) since Boost
// 1.82. Otherwise, we use `std::unordered_map` and `std::unordered_set` (in
// particular for the Boost 1.71 that is used by the `CPP17 libQLever` CI
// workflow). The Boost containers can be disabled by defining
// `QLEVER_NO_BOOST_UNORDERED`, which is useful to test the fallback.
//
// All the class templates support heterogeneous lookups and insertions (e.g.
// `map.find(stringView)`, `map.try_emplace(stringView, value)`, or
// `map[stringView]` for a map with `std::string` keys), if the hash and
// equality functions are transparent (which the defaults from `absl` are for
// strings). The Boost containers support this natively since Boost 1.82. For
// all other containers (the `std` containers, and the flat containers from
// Boost 1.81, which only support heterogeneous lookups, but no heterogeneous
// insertions), we use the `HeterogeneousContainer` wrapper below, which
// converts the key to the `key_type` before calling the respective member
// function of the underlying container.

#include <boost/version.hpp>
#include <initializer_list>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "backports/algorithm.h"
#include "backports/concepts.h"
#include "backports/type_traits.h"
#include "util/Forward.h"

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
namespace detail {
// True iff `T` (a hash or equality function) is transparent.
template <typename T>
CPP_requires(HasIsTransparent,
             requires()(ql::type_identity<typename T::is_transparent>{}));
template <typename T>
CPP_concept IsTransparent = CPP_requires_ref(HasIsTransparent, T);
}  // namespace detail

// A hash container (`std::unordered_map`, `std::unordered_set`, or the flat
// containers from Boost 1.81) with heterogeneous versions of all the member
// functions that take a key (and a `contains` member function, which the `std`
// containers don't have in C++17). If the hash and equality functions of the
// container are transparent, a key that has a different type than the
// `key_type` is converted to the `key_type`, before it is passed to the
// respective member function of the `Base` container. Otherwise, the key is
// passed on unchanged, so a lookup with a key that is not implicitly
// convertible to the `key_type` doesn't compile, just as for the Boost
// containers with non-transparent hash and equality functions.
template <typename Base>
class HeterogeneousContainer : public Base {
 public:
  using typename Base::const_iterator;
  using typename Base::hasher;
  using typename Base::iterator;
  using typename Base::key_equal;
  using typename Base::key_type;
  using typename Base::size_type;
  using typename Base::value_type;

 private:
  static constexpr bool isTransparent =
      detail::IsTransparent<hasher> && detail::IsTransparent<key_equal>;

  // True iff a `Key` has to be converted to the `key_type`, see above.
  template <typename Key>
  static constexpr bool needsConversion =
      isTransparent && !std::is_same_v<ql::remove_cvref_t<Key>, key_type> &&
      !std::is_convertible_v<Key, const_iterator> &&
      std::is_constructible_v<key_type, Key>;

 public:
  using Base::Base;

  // The member functions that take a key. The default template arguments
  // (`Key = key_type`) are needed for keys that are given as a braced
  // initializer list (e.g. `map.find({a, b})` for a map with `std::pair`
  // keys): a type cannot be deduced from a braced initializer list, so without
  // the default, such a call wouldn't compile, whereas it does compile for the
  // Boost containers (which have non-template overloads for `const key_type&`).
  // With the default, the braced initializer list initializes a `key_type`.
  template <typename Key = key_type>
  iterator find(Key&& key) {
    return Base::find(toKey(AD_FWD(key)));
  }
  template <typename Key = key_type>
  const_iterator find(Key&& key) const {
    return Base::find(toKey(AD_FWD(key)));
  }
  template <typename Key = key_type>
  size_type count(Key&& key) const {
    return Base::count(toKey(AD_FWD(key)));
  }
  template <typename Key = key_type>
  bool contains(Key&& key) const {
    return find(AD_FWD(key)) != Base::end();
  }
  template <typename Key = key_type>
  decltype(auto) at(Key&& key) {
    return Base::at(toKey(AD_FWD(key)));
  }
  template <typename Key = key_type>
  decltype(auto) at(Key&& key) const {
    return Base::at(toKey(AD_FWD(key)));
  }
  template <typename Key = key_type>
  decltype(auto) operator[](Key&& key) {
    return Base::operator[](toKey(AD_FWD(key)));
  }
  template <typename Key = key_type, typename... Args>
  decltype(auto) try_emplace(Key&& key, Args&&... args) {
    return Base::try_emplace(toKey(AD_FWD(key)), AD_FWD(args)...);
  }
  template <typename Key = key_type, typename Value>
  decltype(auto) insert_or_assign(Key&& key, Value&& value) {
    return Base::insert_or_assign(toKey(AD_FWD(key)), AD_FWD(value));
  }

  // Erase the element with the given key, or the element that the given
  // iterator points to (an iterator is never converted, see
  // `needsConversion`).
  template <typename KeyOrIterator = key_type>
  decltype(auto) erase(KeyOrIterator&& keyOrIterator) {
    return Base::erase(toKey(AD_FWD(keyOrIterator)));
  }
  template <typename It>
  decltype(auto) erase(It first, It last) {
    return Base::erase(first, last);
  }

  // Insert a `value`. For a set, the `value_type` is the `key_type`, so
  // `set.insert(value)` takes a key, which is converted to the `key_type` if
  // required, just as for the other member functions above (e.g.
  // `set.insert(stringView)` for a set of `std::string`s). For a map, the
  // `value_type` is a `std::pair<const key_type, mapped_type>`, so
  // `map.insert(value)` never takes a bare key, and the `value` is passed on
  // unchanged (heterogeneous insertions into a map are done via `try_emplace`,
  // `insert_or_assign`, or `operator[]`). For the default template argument,
  // see the comment above `find`.
  template <typename Value = value_type>
  decltype(auto) insert(Value&& value) {
    if constexpr (std::is_same_v<key_type, value_type>) {
      return Base::insert(toKey(AD_FWD(value)));
    } else {
      return Base::insert(AD_FWD(value));
    }
  }
  // The `insert` overloads with an iterator hint or an iterator range.
  template <typename T, typename U>
  decltype(auto) insert(T&& t, U&& u) {
    return Base::insert(AD_FWD(t), AD_FWD(u));
  }
  void insert(std::initializer_list<value_type> list) { Base::insert(list); }

 private:
  // Convert the `key` to the `key_type` if `needsConversion` (see above),
  // otherwise return it unchanged.
  template <typename Key>
  static decltype(auto) toKey(Key&& key) {
    if constexpr (needsConversion<Key>) {
      return key_type(AD_FWD(key));
    } else {
      return AD_FWD(key);
    }
  }
};

// The fallbacks based on the STL, which are used when the Boost containers are
// not available. (These are classes and not aliases, s.t.
// `ad_utility::isInstantiation` works for them.)
template <typename... Ts>
class StdUnorderedMap
    : public HeterogeneousContainer<std::unordered_map<Ts...>> {
  using HeterogeneousContainer<
      std::unordered_map<Ts...>>::HeterogeneousContainer;
};

template <typename... Ts>
class StdUnorderedSet
    : public HeterogeneousContainer<std::unordered_set<Ts...>> {
  using HeterogeneousContainer<
      std::unordered_set<Ts...>>::HeterogeneousContainer;
};

#if defined(QLEVER_HAS_BOOST_UNORDERED_FLAT) && \
    !defined(QLEVER_HAS_BOOST_UNORDERED_NODE)
// The flat containers from Boost 1.81, with heterogeneous insertions.
template <typename... Ts>
class BoostFlatMap
    : public HeterogeneousContainer<boost::unordered_flat_map<Ts...>> {
  using HeterogeneousContainer<
      boost::unordered_flat_map<Ts...>>::HeterogeneousContainer;
};

template <typename... Ts>
class BoostFlatSet
    : public HeterogeneousContainer<boost::unordered_flat_set<Ts...>> {
  using HeterogeneousContainer<
      boost::unordered_flat_set<Ts...>>::HeterogeneousContainer;
};
#endif
}  // namespace backports

// The class templates for hash maps and hash sets. The elements of the `Node`
// containers never move, the elements of the `Flat` containers may move when
// the container grows.
#ifdef QLEVER_HAS_BOOST_UNORDERED_NODE
template <typename... Ts>
using FlatHashMapTemplate = boost::unordered_flat_map<Ts...>;
template <typename... Ts>
using FlatHashSetTemplate = boost::unordered_flat_set<Ts...>;
template <typename... Ts>
using NodeHashMapTemplate = boost::unordered_node_map<Ts...>;
template <typename... Ts>
using NodeHashSetTemplate = boost::unordered_node_set<Ts...>;
#else
#ifdef QLEVER_HAS_BOOST_UNORDERED_FLAT
template <typename... Ts>
using FlatHashMapTemplate = backports::BoostFlatMap<Ts...>;
template <typename... Ts>
using FlatHashSetTemplate = backports::BoostFlatSet<Ts...>;
#else
template <typename... Ts>
using FlatHashMapTemplate = backports::StdUnorderedMap<Ts...>;
template <typename... Ts>
using FlatHashSetTemplate = backports::StdUnorderedSet<Ts...>;
#endif
template <typename... Ts>
using NodeHashMapTemplate = backports::StdUnorderedMap<Ts...>;
template <typename... Ts>
using NodeHashSetTemplate = backports::StdUnorderedSet<Ts...>;
#endif

// Make `ql::erase_if` work for all the above hash containers. In C++20 mode,
// `ql::erase_if` already is `std::erase_if`, which covers the
// `std::unordered_...` containers.
namespace backports {
// Erase all elements `x` with `pred(x)` from the `container` and return the
// number of erased elements. This is only used for the `std::unordered_...`
// containers, for which `container.erase(it)` invalidates only the iterators to
// the erased element, and returns an iterator to the next element (this is
// also how `std::erase_if` is specified for them in C++20). The Boost
// containers have their own `erase_if`.
template <typename Container, typename Pred>
typename Container::size_type eraseIfFromUnorderedContainer(
    Container& container, Pred& pred) {
  auto oldSize = container.size();
  for (auto it = container.begin(); it != container.end();) {
    if (pred(*it)) {
      it = container.erase(it);
    } else {
      ++it;
    }
  }
  return oldSize - container.size();
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
