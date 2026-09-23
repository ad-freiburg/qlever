// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Robin Textor-Falconi <textorr@informatik.uni-freiburg.de>, UFR
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.
//
// Derived from Boost.Sort, files
// `boost/sort/block_indirect_sort/blk_detail/parallel_sort.hpp` and
// `boost/sort/common/pivot.hpp`:
// Copyright (c) 2010, 2015, 2016 Francisco Jose Tapia (fjtapia@gmail.com)
// Distributed under the Boost Software License, Version 1.0. (See the
// accompanying file `LICENSE_1_0.txt` or copy at
// http://www.boost.org/LICENSE_1_0.txt)

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_PARALLELSORT_H
#define QLEVER_SRC_UTIL_BLOCKSORT_PARALLELSORT_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <algorithm>
#include <bit>
#include <boost/asio/awaitable.hpp>
#include <boost/sort/pdqsort/pdqsort.hpp>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iterator>
#include <utility>

#include "backports/algorithm.h"
#include "util/Exception.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// A parallel quicksort, used for the initial sort of the parts, and on its own
// for few threads. A port of `boost::sort::blk_detail::parallel_sort`.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Sort the three elements and return the middle one, Boost's `mid3`.
//
// NOTE: This and `movePivotToFront` use `ql::ranges::iter_swap` instead of
// Boost's `std::swap(*it1, *it2)`, which doesn't compile for proxy iterators
// like those of `IdTable`. The other parts of Boost.Sort that are used here
// (e.g. `pdqsort`) work for `IdTable` as they are.
template <typename Iterator, typename Compare>
Iterator median3(Iterator it1, Iterator it2, Iterator it3, const Compare& cmp) {
  if (cmp(*it2, *it1)) {
    ql::ranges::iter_swap(it2, it1);
  }
  if (cmp(*it3, *it2)) {
    ql::ranges::iter_swap(it3, it2);
    if (cmp(*it2, *it1)) {
      ql::ranges::iter_swap(it2, it1);
    }
  }
  return it2;
}

// Move the median of nine elements of `[first, last)` (at least nine) to
// `first`, Boost's `pivot9`.
template <typename Iterator, typename Compare>
void movePivotToFront(Iterator first, Iterator last, const Compare& cmp) {
  size_t step = static_cast<size_t>(last - first) / 8;
  Iterator pivot = median3(
      median3(first + 1, first + step, first + 2 * step, cmp),
      median3(first + 3 * step, first + 4 * step, first + 5 * step, cmp),
      median3(first + 6 * step, first + 7 * step, last - 1, cmp), cmp);
  ql::ranges::iter_swap(first, pivot);
}

// The number of elements that one task sorts on its own; smaller for bigger
// elements, like in Boost.
template <typename Value>
constexpr size_t maxElementsPerTask() {
  auto bitsOfSize = static_cast<uint32_t>(std::bit_width(sizeof(Value))) / 2;
  return size_t{1} << (18 - std::min(bitsOfSize, uint32_t{5}));
}

// Whether reversing `[first, last)` would sort it.
template <typename Iterator, typename Compare>
bool isDescending(Iterator first, Iterator last, const Compare& cmp) {
  return ql::ranges::is_sorted(
      first, last,
      [&cmp](const auto& a, const auto& b) -> bool { return cmp(b, a); });
}

// Boost's `divide_sort`: partition `[first, last)`, spawn the second half into
// `group` and recurse into the first one, until `level` reaches zero or a part
// has fewer than `maxElementsPerTask` elements. All recursive calls spawn into
// the `group` of `parallelSort`, which is awaited only after all of them.
template <typename State, typename Iterator>
void divideSort(State& state, Iterator first, Iterator last, uint32_t level,
                TaskGroup& group) {
  using Value = typename State::Value;
  constexpr size_t maxPerTask = maxElementsPerTask<Value>();
  const auto& cmp = state.cmp_;
  if (ql::ranges::is_sorted(first, last, cmp)) {
    return;
  }
  size_t numElements = static_cast<size_t>(last - first);
  if (level == 0 || numElements < maxPerTask) {
    boost::sort::pdqsort(first, last, cmp);
    return;
  }

  // Partition around the median of nine. The pivot is a copy, because `*first`
  // may be a proxy.
  movePivotToFront(first, last, cmp);
  const Value pivot = *first;
  Iterator cFirst = first + 1;
  Iterator cLast = last - 1;
  while (cmp(*cFirst, pivot)) {
    ++cFirst;
  }
  while (cmp(pivot, *cLast)) {
    --cLast;
  }
  while (cFirst < cLast) {
    ql::ranges::iter_swap(cFirst, cLast);
    ++cFirst;
    --cLast;
    while (cmp(*cFirst, pivot)) {
      ++cFirst;
    }
    while (cmp(pivot, *cLast)) {
      --cLast;
    }
  }
  ql::ranges::iter_swap(first, cLast);

  group.spawnFunction([&state, cFirst, last, level, &group]() {
    divideSort(state, cFirst, last, level - 1, group);
  });
  if (state.hasError()) {
    return;
  }
  divideSort(state, first, cLast, level - 1, group);
}

// Sort `[first, last)` with a parallel quicksort, Boost's `parallel_sort`.
template <typename State, typename Iterator>
net::awaitable<void> parallelSort(State& state, Iterator first, Iterator last) {
  using Value = typename State::Value;
  const auto& cmp = state.cmp_;
  AD_CORRECTNESS_CHECK(last >= first);
  size_t numElements = static_cast<size_t>(last - first);

  // Cheap special cases: already sorted, or sorted in reverse.
  if (ql::ranges::is_sorted(first, last, cmp)) {
    co_return;
  }
  if (isDescending(first, last, cmp)) {
    ql::ranges::reverse(first, last);
    co_return;
  }

  constexpr size_t maxPerTask = maxElementsPerTask<Value>();
  if (numElements < maxPerTask) {
    boost::sort::pdqsort(first, last, cmp);
    co_return;
  }
  // The maximal recursion depth, with some slack for uneven splits.
  auto level =
      static_cast<uint32_t>((std::bit_width(numElements / maxPerTask) * 3) / 2);

  TaskGroup group = state.makeTaskGroup();
  // `join()` is awaited below, see LIFETIME at `TaskGroup`.
  try {
    if (!state.hasError()) {
      divideSort(state, first, last, level, group);
    }
  } catch (...) {
    state.storeError(std::current_exception());
  }
  co_await group.join();
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_PARALLELSORT_H
