// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_ALLOCATORTYPES_H
#define QLEVER_SRC_UTIL_ALLOCATORTYPES_H

#include <deque>
#include <forward_list>
#include <list>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "util/AllocatorPmr.h"

// Short, consistent aliases for standard containers parameterized with
// QLever's `ql::pmr::memory_resource`-based `ad_utility::PmrAllocator<T>`
// (see `util/AllocatorPmr.h`). Spelling out
// `std::vector<T, ad_utility::PmrAllocator<T>>` (etc.) at every use is
// repetitive and error-prone; prefer these aliases instead.
namespace qlever {

template <typename T>
using PmrAllocator = ad_utility::PmrAllocator<T>;

template <typename T>
using vector = std::vector<T, PmrAllocator<T>>;

template <typename T>
using deque = std::deque<T, PmrAllocator<T>>;

template <typename T>
using list = std::list<T, PmrAllocator<T>>;

template <typename T>
using forward_list = std::forward_list<T, PmrAllocator<T>>;

template <typename T, typename Compare = std::less<T>>
using set = std::set<T, Compare, PmrAllocator<T>>;

template <typename T, typename Compare = std::less<T>>
using multiset = std::multiset<T, Compare, PmrAllocator<T>>;

template <typename Key, typename T, typename Compare = std::less<Key>>
using map = std::map<Key, T, Compare, PmrAllocator<std::pair<const Key, T>>>;

template <typename Key, typename T, typename Compare = std::less<Key>>
using multimap =
    std::multimap<Key, T, Compare, PmrAllocator<std::pair<const Key, T>>>;

template <typename T, typename Hash = std::hash<T>,
          typename KeyEqual = std::equal_to<T>>
using unordered_set = std::unordered_set<T, Hash, KeyEqual, PmrAllocator<T>>;

template <typename T, typename Hash = std::hash<T>,
          typename KeyEqual = std::equal_to<T>>
using unordered_multiset =
    std::unordered_multiset<T, Hash, KeyEqual, PmrAllocator<T>>;

template <typename Key, typename T, typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
using unordered_map =
    std::unordered_map<Key, T, Hash, KeyEqual,
                       PmrAllocator<std::pair<const Key, T>>>;

template <typename Key, typename T, typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
using unordered_multimap =
    std::unordered_multimap<Key, T, Hash, KeyEqual,
                            PmrAllocator<std::pair<const Key, T>>>;

template <typename CharT, typename Traits = std::char_traits<CharT>>
using basic_string = std::basic_string<CharT, Traits, PmrAllocator<CharT>>;

using string = basic_string<char>;

}  // namespace qlever

#endif  // QLEVER_SRC_UTIL_ALLOCATORTYPES_H
