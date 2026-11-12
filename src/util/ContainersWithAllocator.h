// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the LICENSE file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H
#define QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H

#include <absl/container/hash_container_defaults.h>

#include <deque>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "backports/unordered.h"
#include "util/Allocator.h"

// This file contains, in the `qlm` namespace (roughly `QLever memory`),
// aliases for containers from the standard library (and for hash containers
// from Boost, see NOTE 3) with their allocator rebound to `qlever::Allocator`.
// This is similar to the `std::pmr` namespace for polymorphic allocators.
//
// NOTE 1: The namespace name `qlm` intentionally has three letters, same as
// `std`, so e.g. changing a `std::vector` to a `qlm::vector` will not trigger a
// reformatting.
//
// NOTE 2: The older aliases `ad_utility::VectorWithMemoryLimit`,
// `HashMapWithMemoryLimit` and `HashSetWithMemoryLimit` serve the same purpose
// for a vector, a hash map and a hash set. `VectorWithMemoryLimit` is a class
// (not a plain alias) because a `std::vector` with a non-default-constructible
// allocator does not work with `ql::ranges` on libc++ (see the comment in
// `util/VectorWithMemoryLimit.h`). The same caveat applies to `qlm::vector`,
// so prefer `VectorWithMemoryLimit` for a vector that is passed to `ql::ranges`
// algorithms or views.
//
// NOTE 3: The hash containers `flat_hash_map`, `flat_hash_set`,
// `node_hash_map`, and `node_hash_set` are the `boost::unordered_flat_...` and
// `boost::unordered_node_...` containers if available (see
// `backports/unordered.h`), and `std::unordered_map` and `std::unordered_set`
// else (the `absl` containers are not exception safe, but `qlever::Allocator`
// throws when the memory limit is exceeded). The `node_...` containers
// guarantee pointer stability of their elements, the `flat_...` containers
// don't. In all cases, the hash function defaults to `absl::Hash`.
namespace qlm {

template <typename T>
using Allocator = qlever::Allocator<T>;

template <typename T, typename... Args>
std::shared_ptr<T> makeShared(const Allocator<T>& allocator, Args&&... args) {
  return std::allocate_shared<T>(allocator, std::forward<Args>(args)...);
}

template <typename T>
using vector = std::vector<T, Allocator<T>>;

template <typename T>
using deque = std::deque<T, Allocator<T>>;

template <typename T>
using list = std::list<T, Allocator<T>>;

template <typename T, typename Compare = std::less<T>>
using set = std::set<T, Compare, Allocator<T>>;

template <typename Key, typename T, typename Compare = std::less<Key>>
using map = std::map<Key, T, Compare, Allocator<std::pair<const Key, T>>>;

#ifdef QLEVER_HAS_BOOST_UNORDERED
template <typename T, typename Hash = absl::DefaultHashContainerHash<T>,
          typename KeyEqual = absl::DefaultHashContainerEq<T>>
using flat_hash_set =
    boost::unordered_flat_set<T, Hash, KeyEqual, Allocator<T>>;

template <typename Key, typename T,
          typename Hash = absl::DefaultHashContainerHash<Key>,
          typename KeyEqual = absl::DefaultHashContainerEq<Key>>
using flat_hash_map =
    boost::unordered_flat_map<Key, T, Hash, KeyEqual,
                              Allocator<std::pair<const Key, T>>>;

template <typename T, typename Hash = absl::DefaultHashContainerHash<T>,
          typename KeyEqual = absl::DefaultHashContainerEq<T>>
using node_hash_set =
    boost::unordered_node_set<T, Hash, KeyEqual, Allocator<T>>;

template <typename Key, typename T,
          typename Hash = absl::DefaultHashContainerHash<Key>,
          typename KeyEqual = absl::DefaultHashContainerEq<Key>>
using node_hash_map =
    boost::unordered_node_map<Key, T, Hash, KeyEqual,
                              Allocator<std::pair<const Key, T>>>;
#else
template <typename T, typename Hash = absl::DefaultHashContainerHash<T>,
          typename KeyEqual = absl::DefaultHashContainerEq<T>>
using flat_hash_set = std::unordered_set<T, Hash, KeyEqual, Allocator<T>>;

template <typename Key, typename T,
          typename Hash = absl::DefaultHashContainerHash<Key>,
          typename KeyEqual = absl::DefaultHashContainerEq<Key>>
using flat_hash_map = std::unordered_map<Key, T, Hash, KeyEqual,
                                         Allocator<std::pair<const Key, T>>>;

template <typename T, typename Hash = absl::DefaultHashContainerHash<T>,
          typename KeyEqual = absl::DefaultHashContainerEq<T>>
using node_hash_set = flat_hash_set<T, Hash, KeyEqual>;

template <typename Key, typename T,
          typename Hash = absl::DefaultHashContainerHash<Key>,
          typename KeyEqual = absl::DefaultHashContainerEq<Key>>
using node_hash_map = flat_hash_map<Key, T, Hash, KeyEqual>;
#endif

}  // namespace qlm

#endif  // QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H
