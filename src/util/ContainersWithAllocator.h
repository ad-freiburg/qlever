// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the LICENSE file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H
#define QLEVER_SRC_UTIL_CONTAINERSWITHALLOCATOR_H

#include <deque>
#include <initializer_list>
#include <iterator>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "backports/concepts.h"
#include "backports/keywords.h"
#include "backports/type_traits.h"
#include "util/Allocator.h"
#include "util/Forward.h"
#include "util/TypeTraits.h"

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
// `util/VectorWithMemoryLimit.h`). `qlm::vector` forwards its constructors the
// same way (see below), so the caveat does not apply to it either. Unlike
// `VectorWithMemoryLimit`, `qlm::vector` is copyable.
namespace qlm {

template <typename T>
using Allocator = qlever::Allocator<T>;

template <typename T, typename... Args>
std::shared_ptr<T> makeShared(const Allocator<T>& allocator, Args&&... args) {
  return std::allocate_shared<T>(allocator, std::forward<Args>(args)...);
}

namespace detail {
// Return true iff the last of the `Args` is convertible to `Alloc` and `Base`
// is constructible from the `Args`. The constructibility is only checked if the
// last argument is an allocator: For other arguments, the check instantiates
// the default allocator argument of the constructors of `std::vector`, which is
// a hard error for an allocator that is not default-constructible (e.g. for
// Clang 16 with libstdc++ 12).
template <typename Base, typename Alloc, typename... Args>
constexpr bool isConstructibleWithAllocatorImpl() {
  if constexpr (sizeof...(Args) == 0) {
    return false;
  } else if constexpr (!std::is_convertible_v<ad_utility::Last<Args...>,
                                              Alloc>) {
    return false;
  } else {
    return std::is_constructible_v<Base, Args&&...>;
  }
}
template <typename Base, typename Alloc, typename... Args>
inline constexpr bool isConstructibleWithAllocator =
    isConstructibleWithAllocatorImpl<Base, Alloc, Args...>();
}  // namespace detail

// A `std::vector` with a `qlever::Allocator`. This is a class (not an alias) so
// that it can have deduction guides, which allow for example
// `... | ::ranges::to<qlm::vector>(allocator)` without spelling out the
// element type.
template <typename T>
class vector : public std::vector<T, Allocator<T>> {
 public:
  using Base = std::vector<T, Allocator<T>>;

  // Forward all constructors of `Base` that take an allocator as their last
  // argument (the allocator is not default-constructible, so there are no
  // others). We deliberately don't write `using Base::Base`: Since C++23, the
  // inherited constructors take part in class template argument deduction,
  // where they would deduce the element type from the type of the allocator
  // instead of from the range or iterators (e.g. on GCC 15).
  CPP_template(typename... Args)(
      requires(sizeof...(Args) > 0) &&
      CPP_NOT(concepts::derived_from<
              ql::remove_cvref_t<ad_utility::First<Args...>>, Base>) &&
      detail::isConstructibleWithAllocator<Base, Allocator<T>, Args...>)
  QL_EXPLICIT(sizeof...(Args) == 1)
  vector(Args&&... args) : Base(AD_FWD(args)...) {}

  // The `initializer_list` constructor is not covered by the generic mechanism
  // above, because a braced list has no type that could be deduced.
  vector(std::initializer_list<T> init, const Allocator<T>& alloc)
      : Base(init, alloc) {}
};

// Deduce the element type from an iterator pair and an allocator (which may be
// an allocator for a different element type that is convertible to the
// allocator of the deduced element type).
template <typename It, typename A,
          typename V = typename std::iterator_traits<It>::value_type,
          typename = std::enable_if_t<std::is_convertible_v<A, Allocator<V>>>>
vector(It, It, A) -> vector<V>;

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
