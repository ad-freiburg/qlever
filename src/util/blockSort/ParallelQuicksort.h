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

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_PARALLELQUICKSORT_H
#define QLEVER_SRC_UTIL_BLOCKSORT_PARALLELQUICKSORT_H

#ifndef QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#include <algorithm>
#include <bit>
#include <boost/asio/awaitable.hpp>
#include <boost/sort/pdqsort/pdqsort.hpp>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <utility>

#include "backports/algorithm.h"
#include "util/blockSort/SortState.h"
#include "util/blockSort/TaskGroup.h"

// A parallel quicksort whose tasks run on an executor. A port of
// `boost::sort::blk_detail::parallel_sort`.
namespace ad_utility::blockSort::detail {

namespace net = boost::asio;

// Sort the three elements and return the middle one, Boost's `mid3`.
//
// NOTE: This and `movePivotToFront` use `ql::ranges::iter_swap` instead of
// Boost's `std::swap(*it1, *it2)`, which doesn't compile for proxy iterators
// like those of `IdTable`. The other parts of Boost.Sort that are used here
// (e.g. `pdqsort`) work for `IdTable` as they are.
template <typename Iterator, typename Compare>
[[nodiscard]] Iterator median3(Iterator it1, Iterator it2, Iterator it3,
                               const Compare& cmp) {
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

// Move the median of nine elements of `[first, last)` (at least 16) to
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

// The default number of elements that one task sorts on its own; smaller for
// bigger elements, like in Boost.
template <typename Value>
constexpr size_t maxElementsPerTask() {
  auto bitsOfSize = static_cast<uint32_t>(std::bit_width(sizeof(Value))) / 2;
  return size_t{1} << (18 - std::min(bitsOfSize, uint32_t{5}));
}

// Whether reversing `[first, last)` would sort it.
template <typename Iterator, typename Compare>
[[nodiscard]] bool isDescending(Iterator first, Iterator last,
                                const Compare& cmp) {
  return ql::ranges::is_sorted(
      first, last,
      [&cmp](const auto& a, const auto& b) -> bool { return cmp(b, a); });
}

// Partition `[first, last)` around the pivot at `*first` (a Hoare partition,
// like Boost): afterwards, `[first, leftEnd)` holds elements that are at most
// the pivot, the pivot is at `leftEnd`, and `[rightBegin, last)` holds elements
// that are at least the pivot. Elements equal to the pivot may end up on both
// sides. `ql::ranges::partition` was measured to be a few percent slower for
// distinct elements.
//
// NOTE: The scans need no bounds checks: the pivot at `first` stops the scan
// from the right, and the median of nine (see `movePivotToFront`) leaves
// another element that is at least the pivot, which stops the scan from the
// left. After a swap, the swapped elements stop the scans.
template <typename Iterator, typename Compare>
[[nodiscard]] std::pair<Iterator, Iterator> hoarePartition(Iterator first,
                                                           Iterator last,
                                                           const Compare& cmp) {
  // A copy, because `*first` may be a proxy.
  const typename std::iterator_traits<Iterator>::value_type pivot = *first;
  Iterator leftEnd = last - 1;
  Iterator rightBegin = first + 1;
  while (cmp(*rightBegin, pivot)) {
    ++rightBegin;
  }
  while (cmp(pivot, *leftEnd)) {
    --leftEnd;
  }
  while (rightBegin < leftEnd) {
    ql::ranges::iter_swap(rightBegin, leftEnd);
    ++rightBegin;
    --leftEnd;
    while (cmp(*rightBegin, pivot)) {
      ++rightBegin;
    }
    while (cmp(pivot, *leftEnd)) {
      --leftEnd;
    }
  }
  ql::ranges::iter_swap(first, leftEnd);
  return {leftEnd, rightBegin};
}

// Boost's `divide_sort`: partition `[first, last)` and sort the two parts
// concurrently, until `level` reaches zero or a part has fewer than
// `maxElementsPerTask_` elements, which is then sorted by a single task.
template <typename State, typename Iterator>
net::awaitable<void> parallelQuicksortImpl(State& state, Iterator first,
                                           Iterator last, uint32_t level) {
  const auto& cmp = state.cmp_;
  if (ql::ranges::is_sorted(first, last, cmp)) {
    co_return;
  }
  size_t numElements = static_cast<size_t>(last - first);
  if (level == 0 || numElements < state.maxElementsPerTask_) {
    boost::sort::pdqsort(first, last, cmp);
    co_return;
  }

  movePivotToFront(first, last, cmp);
  auto [leftEnd, rightBegin] = hoarePartition(first, last, cmp);
  co_await state.runConcurrently(
      parallelQuicksortImpl(state, first, leftEnd, level - 1),
      parallelQuicksortImpl(state, rightBegin, last, level - 1));
}

// Sort `[first, last)` with a parallel quicksort, Boost's `parallel_sort`.
template <typename State, typename Iterator>
net::awaitable<void> parallelQuicksort(State& state, Iterator first,
                                       Iterator last) {
  const auto& cmp = state.cmp_;
  // Cheap special cases: already sorted, or sorted in reverse.
  if (ql::ranges::is_sorted(first, last, cmp)) {
    co_return;
  }
  if (isDescending(first, last, cmp)) {
    ql::ranges::reverse(first, last);
    co_return;
  }
  // The maximal recursion depth, with some slack for uneven splits.
  size_t numElements = static_cast<size_t>(last - first);
  auto level = static_cast<uint32_t>(
      (std::bit_width(numElements / state.maxElementsPerTask_) * 3) / 2);
  co_await parallelQuicksortImpl(state, first, last, level);
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_REDUCED_FEATURE_SET_FOR_CPP17

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_PARALLELQUICKSORT_H
