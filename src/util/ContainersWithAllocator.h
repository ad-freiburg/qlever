// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the LICENSE file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H
#define QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H

#include <deque>
#include <list>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "util/Allocator.h"

// This file contains, in the `qlm` namespace (roughly `QLever memory`),
// aliases for containers from the standard library with their allocator rebound
// to `qlever::Allocator`. This is similar to the `std::pmr` namespace for
// polymorphic allocators.
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
namespace qlm {

template <typename T>
using Allocator = qlever::Allocator<T>;

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

template <typename T, typename Hash = std::hash<T>,
          typename KeyEqual = std::equal_to<T>>
using unordered_set = std::unordered_set<T, Hash, KeyEqual, Allocator<T>>;

template <typename Key, typename T, typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
using unordered_map = std::unordered_map<Key, T, Hash, KeyEqual,
                                         Allocator<std::pair<const Key, T>>>;

}  // namespace qlm

#endif  // QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H
