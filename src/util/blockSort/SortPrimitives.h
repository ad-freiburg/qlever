// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_UTIL_BLOCKSORT_SORTPRIMITIVES_H
#define QLEVER_SRC_UTIL_BLOCKSORT_SORTPRIMITIVES_H

#include <cstddef>
#include <iterator>
#include <type_traits>

#include "backports/algorithm.h"
#include "util/blockSort/BoostSortHeaders.h"

// Replacements for the primitives of `boost::sort` that also work for proxy
// iterators like those of the column-major `IdTable`. Boost swaps elements with
// `std::swap(*it1, *it2)`, which requires true `Value&` references; here
// `ql::ranges::iter_swap` is used instead. The scratch buffers are plain
// arrays and don't need this.
namespace ad_utility::blockSort::detail {

// Whether `Iterator` hands out true references.
template <typename Iterator>
constexpr bool hasTrueReferences() {
  using Traits = std::iterator_traits<Iterator>;
  return std::is_same_v<typename Traits::reference,
                        typename Traits::value_type&>;
}

// Sort `[first, last)` in the calling thread: `boost::sort::pdqsort` if
// possible, `ql::ranges::sort` for proxy iterators.
template <typename Iterator, typename Compare>
void sortSequentially(Iterator first, Iterator last, const Compare& cmp) {
  if constexpr (hasTrueReferences<Iterator>()) {
    boost::sort::pdqsort(first, last, cmp);
  } else {
    ql::ranges::sort(first, last, cmp);
  }
}

// Sort the three elements and return the middle one, Boost's `mid3`.
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
  size_t step = static_cast<size_t>(last - first) >> 3;
  Iterator pivot = median3(
      median3(first + 1, first + step, first + 2 * step, cmp),
      median3(first + 3 * step, first + 4 * step, first + 5 * step, cmp),
      median3(first + 6 * step, first + 7 * step, last - 1, cmp), cmp);
  ql::ranges::iter_swap(first, pivot);
}

}  // namespace ad_utility::blockSort::detail

#endif  // QLEVER_SRC_UTIL_BLOCKSORT_SORTPRIMITIVES_H
