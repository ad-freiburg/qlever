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

// The primitives of the block indirect sort (see
// `util/blockSort/BlockIndirectSort.h`) that have to work for *proxy*
// iterators, too.
//
// `boost::sort` exchanges elements with `std::swap(*it1, *it2)` and
// `Value tmp = std::move(*it)`, both of which require an iterator that hands
// out true `Value&` references. The iterators of QLever's column-major
// `IdTable` hand out proxy row references instead (a row of a column-major
// table is not an object, see `engine/idTable/IdTableRow.h`), for which the
// equivalent operations are the customization points `ql::ranges::iter_swap`
// and `ql::ranges::iter_move`. Everything that Boost would do to an element
// *through* such an iterator therefore has to be routed through this file.
//
// NOTE: The primitives that Boost applies to the scratch buffers (which are
// ordinary arrays of `Value`) are unaffected and are used directly.
namespace ad_utility::blockSort::detail {

// Whether `Iterator` hands out true references, so that the primitives of
// `boost::sort` can be applied to its elements directly.
template <typename Iterator>
constexpr bool hasTrueReferences() {
  using Traits = std::iterator_traits<Iterator>;
  return std::is_same_v<typename Traits::reference,
                        typename Traits::value_type&>;
}

// Sort `[first, last)` with a single thread. This is `boost::sort::pdqsort`,
// which is faster than anything in the standard library, wherever it compiles,
// and `ql::ranges::sort` (which goes through `iter_swap` and `iter_move`) for
// proxy iterators, see the comment at the top of this file.
template <typename Iterator, typename Compare>
void sortSequentially(Iterator first, Iterator last, const Compare& cmp) {
  if constexpr (hasTrueReferences<Iterator>()) {
    boost::sort::pdqsort(first, last, cmp);
  } else {
    ql::ranges::sort(first, last, cmp);
  }
}

// Return the iterator to the median of the three elements, ordering the three
// of them on the way. A port of `boost::sort::common::mid3`.
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

// Move the median of nine of the elements of `[first, last)` to `first`, which
// is the pivot that `divideSort` (see `util/blockSort/ParallelSort.h`)
// partitions around. A port of `boost::sort::common::pivot9`, which the range
// has to have at least nine elements for.
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
