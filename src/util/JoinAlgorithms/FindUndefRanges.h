//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_JOINALGORITHMS_FINDUNDEFRANGES_H
#define QLEVER_SRC_UTIL_JOINALGORITHMS_FINDUNDEFRANGES_H

#include "backports/algorithm.h"
#include "global/Id.h"
#include "util/Generator.h"

namespace ad_utility {
// The following functions `findSmallerUndefRanges...` have the following in
// common: For a single `row` of IDs find all the iterators in the sorted range
// `[begin, end)` that are lexicographically smaller than `row`, are compatible
// with `row` (meaning that on each position they have either the same value, or
// one of them is UNDEF), and contain at least one UNDEF entry. They all have to
// following preconditions (checked via `assert()` macros):
//   - The `row` must have the same number of entries than the elements in the
//     range `[begin, end)`
//   - The range `[begin, end)` must be lexicographically sorted.
// The resulting rows are returned via a generator of iterators. The boolean
// argument `outOfOrderElementFound` is set to true if the `row` contains at
// least one UNDEF entry, and one of the compatible rows from `[begin, end)`
// contains at least one UNDEF entry in a column in which `row` is defined.
// It is in general not possible for a zipper-style join algorithm to determine
// the correct position in the sorted output for such an element, so we have to
// keep track of this information. For example (5, UNDEF) and (UNDEF, 3) are
// compatible, but the result is (5, 3) which is not adjacent to any of the two
// inputs.
// -- End of comment for all functions --

// This function has the additional precondition that none of the entries of
// `row` may be UNDEF. This function runs in `O(2^C * log(N) + R)` where C
// is the number of columns, N is the size of the range `[begin, end)` and `R`
// is the number of matching elements.
// TODO<joka921> This can be optimized when we also know which columns of
// `[begin, end)` can possibly contain UNDEF values.
CPP_template(typename R,
             typename It)(requires std::random_access_iterator<It>)  //
    auto findSmallerUndefRangesForRowsWithoutUndef(
        const R& row, It begin, It end,
        [[maybe_unused]] bool& resultMightBeUnsorted)
        -> cppcoro::generator<It> {
  using Row = typename std::iterator_traits<It>::value_type;
  assert(row.size() == (*begin).size());
  assert(
      ql::ranges::is_sorted(begin, end, ql::ranges::lexicographical_compare));
  assert((ql::ranges::all_of(row,
                             [](Id id) { return id != Id::makeUndefined(); })));
  size_t numJoinColumns = row.size();
  // TODO<joka921> This can be done without copying.
  Row rowLower = row;

  size_t upperBound = 1UL << row.size();
  for (size_t i = 0; i < upperBound - 1; ++i) {
    for (size_t j = 0; j < numJoinColumns; ++j) {
      rowLower[j] = (i >> (numJoinColumns - j - 1)) & 1
                        ? row[j]
                        : ValueId::makeUndefined();
    }

    auto [begOfUndef, endOfUndef] = std::equal_range(
        begin, end, rowLower, ql::ranges::lexicographical_compare);
    for (auto it = begOfUndef; it != endOfUndef; ++it) {
      co_yield it;
    }
  }
}

// This function has the additional precondition, that the `row` contains
// UNDEF values in all the last `numLastUndefined` columns and no UNDEF values
// in the remaining columns. This function runs in `O(2^C * log(N) + R)` where C
// is the number of  defined columns (`numColumns - numLastUndefined`), N is the
// size of the range `[begin, end)` and `R` is the number of matching elements.

// TODO<joka921> We could also implement a version that is optimized on the
// [begin, end] range not having UNDEF values in some of the columns
CPP_template(typename It)(requires std::random_access_iterator<It>)  //
    auto findSmallerUndefRangesForRowsWithUndefInLastColumns(
        const auto& row, const size_t numLastUndefined, It begin, It end,
        bool& resultMightBeUnsorted) -> cppcoro::generator<It> {
  using Row = typename std::iterator_traits<It>::value_type;
  const size_t numJoinColumns = row.size();
  assert(row.size() == (*begin).size());
  assert(numJoinColumns >= numLastUndefined);
  assert(
      ql::ranges::is_sorted(begin, end, ql::ranges::lexicographical_compare));
  const size_t numDefinedColumns = numJoinColumns - numLastUndefined;
  for (size_t i = 0; i < numDefinedColumns; ++i) {
    assert(row[i] != Id::makeUndefined());
  }
  for (size_t i = numDefinedColumns; i < numJoinColumns; ++i) {
    assert(row[i] == Id::makeUndefined());
  }

  // If every entry in the row is UNDEF, then it is the smallest possible
  // row, and there can be no smaller row.
  if (numLastUndefined == 0) {
    co_return;
  }
  Row rowLower = row;

  size_t upperBound = 1u << numDefinedColumns;
  for (size_t permutationCounter = 0; permutationCounter < upperBound - 1;
       ++permutationCounter) {
    for (size_t colIdx = 0; colIdx < numDefinedColumns; ++colIdx) {
      rowLower[colIdx] =
          (permutationCounter >> (numDefinedColumns - colIdx - 1)) & 1
              ? row[colIdx]
              : ValueId::makeUndefined();
    }

    auto begOfUndef = std::lower_bound(begin, end, rowLower,
                                       ql::ranges::lexicographical_compare);
    rowLower[numDefinedColumns - 1] =
        Id::fromBits(rowLower[numDefinedColumns - 1].getBits() + 1);
    auto endOfUndef = std::lower_bound(begin, end, rowLower,
                                       ql::ranges::lexicographical_compare);
    for (; begOfUndef != endOfUndef; ++begOfUndef) {
      resultMightBeUnsorted = true;
      co_yield begOfUndef;
    }
  }
}

// This function has no additional preconditions, but runs in `O((end - begin) *
// numColumns)`.
CPP_template(typename It)(requires std::random_access_iterator<It>)  //
    auto findSmallerUndefRangesArbitrary(const auto& row, It begin, It end,
                                         bool& resultMightBeUnsorted)
        -> cppcoro::generator<It> {
  assert(row.size() == (*begin).size());
  assert(
      ql::ranges::is_sorted(begin, end, ql::ranges::lexicographical_compare));

  // To only get smaller entries, we first find a suitable upper bound in the
  // input range. We use `std::lower_bound` because the input row itself is not
  // a valid match.
  end = std::lower_bound(begin, end, row, ql::ranges::lexicographical_compare);

  const size_t numJoinColumns = row.size();
  auto isCompatible = [&](const auto& otherRow) {
    for (size_t k = 0u; k < numJoinColumns; ++k) {
      Id a = row[k];
      Id b = otherRow[k];
      bool aUndef = a == Id::makeUndefined();
      bool bUndef = b == Id::makeUndefined();
      bool eq = a == b;
      auto match = aUndef || bUndef || eq;
      if (!match) {
        return false;
      }
    }
    return true;
  };

  for (auto it = begin; it != end; ++it) {
    if (isCompatible(*it)) {
      resultMightBeUnsorted = true;
      co_yield it;
    }
  }
  co_return;
}

// This function first checks in which positions the `row` has UNDEF values, and
// then calls the cheapest possible of the functions defined above.
// Note: Using this function is always correct, but rather costly. We typically
// have additional information about the input (most notably which of the join
// columns contain no UNDEF at all) and therefore a more specialized routine
// should be chosen.
struct FindSmallerUndefRanges {
  CPP_template(typename Row,
               typename It)(requires std::random_access_iterator<It>) auto
  operator()(const Row& row, It begin, It end,
             bool& resultMightBeUnsorted) const -> cppcoro::generator<It> {
    size_t numLastUndefined = 0;
    assert(row.size() > 0);
    auto it = ql::ranges::rbegin(row);
    auto rend = ql::ranges::rend(row);
    for (; it < rend; ++it) {
      if (*it != Id::makeUndefined()) {
        break;
      }
      ++numLastUndefined;
    }

    for (; it < rend; ++it) {
      if (*it == Id::makeUndefined()) {
        return findSmallerUndefRangesArbitrary(row, begin, end,
                                               resultMightBeUnsorted);
      }
    }
    if (numLastUndefined == 0) {
      return findSmallerUndefRangesForRowsWithoutUndef(row, begin, end,
                                                       resultMightBeUnsorted);
    } else {
      return findSmallerUndefRangesForRowsWithUndefInLastColumns(
          row, numLastUndefined, begin, end, resultMightBeUnsorted);
    }
  }
};
constexpr FindSmallerUndefRanges findSmallerUndefRanges;
}  // namespace ad_utility

#endif  // QLEVER_SRC_UTIL_JOINALGORITHMS_FINDUNDEFRANGES_H
