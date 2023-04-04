//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Generator.h"

namespace ad_utility {

struct JoinColumnData {
  std::vector<size_t> jcsA_;
  std::vector<size_t> jcsB_;
  std::vector<size_t> colsCompleteA_;
  std::vector<size_t> colsCompleteB_;
  std::vector<size_t> permutation_;
};

inline JoinColumnData prepareJoinColumns(
    const std::vector<std::array<ColumnIndex, 2>>& joinColumns, size_t numColsA,
    size_t numColsB) {
  std::vector<size_t> permutation;
  permutation.resize(numColsA + numColsB - joinColumns.size());
  std::vector<size_t> jcsA, jcsB;
  for (auto [colA, colB] : joinColumns) {
    permutation.at(colA) = jcsA.size();
    jcsA.push_back(colA);
    jcsB.push_back(colB);
  }

  std::vector<size_t> colsAComplete = jcsA, colsBComplete = jcsB;

  for (size_t i = 0; i < numColsA; ++i) {
    if (!ad_utility::contains(jcsA, i)) {
      permutation.at(i) = colsAComplete.size();
      colsAComplete.push_back(i);
    }
  }

  size_t numSkippedJoinColumns = 0;
  for (size_t i = 0; i < numColsB; ++i) {
    if (!ad_utility::contains(jcsB, i)) {
      permutation.at(i - numSkippedJoinColumns + numColsA) =
          i - numSkippedJoinColumns + numColsA;
      colsBComplete.push_back(i);
    } else {
      ++numSkippedJoinColumns;
    }
  }
  return {std::move(jcsA), std::move(jcsB), std::move(colsAComplete),
          std::move(colsBComplete), std::move(permutation)};
}

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
// least one undefined entry, and one of the compatible rows form `[begin, end)`
// contains at least one undefined entry in a column in which `row` is defined.
// It is in general not possible for a zipper-style join algorithm to determine
// the correct position in the sorted output for such an element, so we have to
// keep track of this information.

// This function has the additional precondition that none of the entries of
// `row` may be undefined. This function runs in `O(2^C * log(N) + R)` where C
// is the number of columns, N is the size of the range `[begin, end)` and `R`
// is the number of matching elements.
// TODO<joka921> This can be optimized when we also know which columns of
// `[begin, end)` can possibly contain undefined values.
auto findSmallerUndefRangesForRowsWithoutUndef(
    const auto& row, auto begin, auto end,
    [[maybe_unused]] bool& outOfOrderElementFound)
    -> cppcoro::generator<decltype(begin)> {
  using Row = typename std::iterator_traits<decltype(begin)>::value_type;
  assert(row.size() == (*begin).size());
  assert(
      std::ranges::is_sorted(begin, end, std::ranges::lexicographical_compare));
  assert((std::ranges::all_of(
      row, [](Id id) { return id != Id::makeUndefined(); })));
  size_t numJoinColumns = row.size();
  // TODO<joka921> This can be done without copying.
  Row rowLower = row;

  size_t upperBound = 1ul << row.size();
  for (size_t i = 0; i < upperBound - 1; ++i) {
    for (size_t j = 0; j < numJoinColumns; ++j) {
      rowLower[j] = (i >> (numJoinColumns - j - 1)) & 1
                        ? row[j]
                        : ValueId::makeUndefined();
    }

    auto [begOfUndef, endOfUndef] = std::equal_range(
        begin, end, rowLower, std::ranges::lexicographical_compare);
    for (; begOfUndef != endOfUndef; ++begOfUndef) {
      co_yield begOfUndef;
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
auto findSmallerUndefRangesForRowsWithUndefInLastColumns(
    const auto& row, const size_t numLastUndefined, auto begin, auto end,
    bool& outOfOrderElementFound) -> cppcoro::generator<decltype(begin)> {
  using Row = typename std::iterator_traits<decltype(begin)>::value_type;
  const size_t numJoinColumns = row.size();
  assert(row.size() == (*begin).size());
  assert(numJoinColumns >= numLastUndefined);
  assert(
      std::ranges::is_sorted(begin, end, std::ranges::lexicographical_compare));
  const size_t numDefinedColumns = numJoinColumns - numLastUndefined;
  for (size_t i = 0; i < numDefinedColumns; ++i) {
    assert(row[i] != Id::makeUndefined());
  }
  for (size_t i = numDefinedColumns; i < numJoinColumns; ++i) {
    assert(row[i] == Id::makeUndefined());
  }

  // If every entry in the row is undefined, then it is the smallest possible
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
                                       std::ranges::lexicographical_compare);
    rowLower[numDefinedColumns - 1] =
        Id::fromBits(rowLower[numDefinedColumns - 1].getBits() + 1);
    auto endOfUndef = std::lower_bound(begin, end, rowLower,
                                       std::ranges::lexicographical_compare);
    for (; begOfUndef != endOfUndef; ++begOfUndef) {
      outOfOrderElementFound = true;
      co_yield begOfUndef;
    }
  }
}

// This function has no additional preconditions, but runs in `O((end - begin) *
// numColumns)`.
auto findSmallerUndefRangesArbitrary(const auto& row, auto begin, auto end,
                                     bool& outOfOrderElementFound)
    -> cppcoro::generator<decltype(begin)> {
  assert(row.size() == (*begin).size());
  assert(
      std::ranges::is_sorted(begin, end, std::ranges::lexicographical_compare));

  // To only get smaller entries, we first find a suitable upper bound in the
  // input range. We use `std::lower_bound` because the input row itself is not
  // a valid match.
  end = std::lower_bound(begin, end, row, std::ranges::lexicographical_compare);

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
      outOfOrderElementFound = true;
      co_yield it;
    }
  }
  co_return;
}

// This function first checks in which positions the `row` has UNDEF values, and
// then calls the cheapest possible of the functions defined above.
auto findSmallerUndefRanges(const auto& row, auto begin, auto end,
                            bool& outOfOrderFound)
    -> cppcoro::generator<decltype(begin)> {
  size_t numLastUndefined = 0;
  assert(row.size() > 0);
  auto it = std::ranges::rbegin(row);
  auto rend = std::ranges::rend(row);
  for (; it < rend; ++it) {
    if (*it != Id::makeUndefined()) {
      break;
    }
    ++numLastUndefined;
  }

  for (; it < rend; ++it) {
    if (*it == Id::makeUndefined()) {
      return findSmallerUndefRangesArbitrary(row, begin, end, outOfOrderFound);
    }
  }
  if (numLastUndefined == 0) {
    return findSmallerUndefRangesForRowsWithoutUndef(row, begin, end,
                                                     outOfOrderFound);
  } else {
    return findSmallerUndefRangesForRowsWithUndefInLastColumns(
        row, numLastUndefined, begin, end, outOfOrderFound);
  }
}

[[maybe_unused]] static inline auto noop = [](auto&&...) {};
// TODO<joka921> Comment, cleanup, move into header.
template <typename Range, typename ElFromFirstNotFoundAction = decltype(noop)>
[[nodiscard]] size_t zipperJoinWithUndef(
    Range&& range1, Range&& range2, auto&& lessThan, auto&& compatibleRowAction,
    auto&& findSmallerUndefRangesLeft, auto&& findSmallerUndefRangesRight,
    ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
  static constexpr bool hasNotFoundAction =
      !ad_utility::isSimilar<ElFromFirstNotFoundAction, decltype(noop)>;
  auto it1 = range1.begin();
  auto end1 = range1.end();
  auto it2 = range2.begin();
  auto end2 = range2.end();

  std::vector<bool> coveredFromLeft(end1 - it1);
  auto cover = [&](auto it) {
    if constexpr (hasNotFoundAction) {
      coveredFromLeft[it - range1.begin()] = true;
    }
  };

  bool outOfOrderFound = false;

  auto makeMergeWithUndefLeft =
      [&cover, &compatibleRowAction,
       &outOfOrderFound]<bool reversed, typename FindUndef>(
          const auto& lt, const FindUndef& findUndef) {
        return
            [&cover, &lt, &findUndef, &compatibleRowAction, &outOfOrderFound](
                const auto& el, auto startOfRange, auto endOfRange) {
              (void)cover;
              if constexpr (ad_utility::isSimilar<FindUndef, decltype(noop)>) {
                return false;
              } else {
                bool compatibleWasFound = false;
                auto smallerUndefRanges =
                    findUndef(el, startOfRange, endOfRange, outOfOrderFound);
                for (const auto& it : smallerUndefRanges) {
                  if (lt(*it, el)) {
                    compatibleWasFound = true;
                    if constexpr (reversed) {
                      compatibleRowAction(el, *it);
                    } else {
                      compatibleRowAction(*it, el);
                      cover(it);
                    }
                  }
                }
                return compatibleWasFound;
              }
            };
      };
  auto mergeWithUndefLeft = makeMergeWithUndefLeft.template operator()<false>(
      lessThan, findSmallerUndefRangesLeft);
  auto mergeWithUndefRight = makeMergeWithUndefLeft.template operator()<true>(
      lessThan, findSmallerUndefRangesRight);

  auto containsNoUndefined = []<typename T>(const T& row) {
    if constexpr (std::is_same_v<T, Id>) {
      return row != Id::makeUndefined();
    } else {
      return (std::ranges::none_of(
          row, [](Id id) { return id == Id::makeUndefined(); }));
    }
  };

  [&]() {
    while (it1 < end1 && it2 < end2) {
      while (lessThan(*it1, *it2)) {
        if constexpr (hasNotFoundAction) {
          if (!mergeWithUndefRight(*it1, range2.begin(), it2)) {
            if (containsNoUndefined(*it1)) {
              cover(it1);
              elFromFirstNotFoundAction(*it1);
            }
          } else {
            cover(it1);
          }
        }
        ++it1;
        if (it1 >= end1) {
          return;
        }
      }
      while (lessThan(*it2, *it1)) {
        mergeWithUndefLeft(*it2, range1.begin(), it1);
        ++it2;
        if (it2 >= end2) {
          return;
        }
      }

      // TODO <joka921> Maybe we can pass in the equality operator for increased
      // performance.
      auto eq = [&lessThan](const auto& el1, const auto& el2) {
        return !lessThan(el1, el2) && !lessThan(el2, el1);
      };
      auto endSame1 = std::find_if_not(
          it1, end1, [&](const auto& row) { return eq(row, *it2); });
      auto endSame2 = std::find_if_not(
          it2, end2, [&](const auto& row) { return eq(*it1, row); });

      std::for_each(it1, endSame1, [&](const auto& row) {
        mergeWithUndefRight(row, range2.begin(), it2);
      });
      std::for_each(it2, endSame2, [&](const auto& row) {
        mergeWithUndefLeft(row, range1.begin(), it1);
      });

      for (; it1 != endSame1; ++it1) {
        cover(it1);
        for (auto innerIt2 = it2; innerIt2 != endSame2; ++innerIt2) {
          compatibleRowAction(*it1, *innerIt2);
        }
      }
      it1 = endSame1;
      it2 = endSame2;
    }
  }();

  std::for_each(it2, end2, [&](const auto& row) {
    mergeWithUndefLeft(row, range1.begin(), range1.end());
  });

  if constexpr (hasNotFoundAction) {
      for (; it1 < end1; ++it1) {
        cover(it1);
        if (!mergeWithUndefRight(*it1, range2.begin(), range2.end())) {
          elFromFirstNotFoundAction(*it1);
        }
      }
  }

  size_t numOutOfOrderAtEnd = 0;
  // TODO<joka921> These will be out of order;
  if constexpr (hasNotFoundAction) {
      for (size_t i = 0; i < coveredFromLeft.size(); ++i) {
        if (!coveredFromLeft[i]) {
          elFromFirstNotFoundAction(*(range1.begin() + i));
          ++numOutOfOrderAtEnd;
        }
      }
  }
  return outOfOrderFound ? std::numeric_limits<size_t>::max()
                         : numOutOfOrderAtEnd;
}

// _____________________________________________________________________________
void gallopingJoin(auto&& smaller, auto&& larger, auto&& lessThan,
                   auto&& action) {
  auto itSmall = smaller.begin();
  auto endSmall = smaller.end();
  auto itLarge = larger.begin();
  auto endLarge = larger.end();
  auto eq = [&lessThan](const auto& el1, const auto& el2) {
    return !lessThan(el1, el2) && !lessThan(el2, el1);
  };
  while (itSmall < endSmall && itLarge < endLarge) {
    while (lessThan(*itSmall, *itLarge)) {
      ++itSmall;
      if (itSmall >= endSmall) {
        return;
      }
    }
    size_t step = 1;
    auto last = itLarge;
    // Perform the exponential search.
    while (lessThan(*itLarge, *itSmall)) {
      last = itLarge;
      itLarge += step;
      step *= 2;
      if (itLarge >= endLarge) {
        itLarge = endLarge - 1;
        if (lessThan(*itLarge, *itSmall)) {
          return;
        }
      }
    }
    if (eq(*itSmall, *itLarge)) {
      // We stepped into a block where l1 and l2 are equal. We need to
      // find the beginning of this block
      while (itLarge > larger.begin() && eq(*itSmall, *(itLarge - 1))) {
        --itLarge;
      }
    } else if (lessThan(*itSmall, *itLarge)) {
      // We stepped over the location where l1 and l2 may be equal.
      // Use binary search to locate that spot
      const auto& needle = *itSmall;
      itLarge = std::lower_bound(
          last, itLarge, needle,
          [&lessThan](const auto& row, const auto& needle) -> bool {
            return lessThan(row, needle);
          });
    }
    // TODO<joka921> We can also use the (cheaper) `lessThan` here.
    // TODO<joka921> The following block is the same for `zipper` and
    // `galloping`, so it can be factored out into a function.
    auto endSameSmall = std::find_if_not(
        itSmall, endSmall, [&](const auto& row) { return eq(row, *itLarge); });
    auto endSameLarge = std::find_if_not(
        itLarge, endLarge, [&](const auto& row) { return eq(row, *itSmall); });

    for (; itSmall != endSameSmall; ++itSmall) {
      for (auto innerItLarge = itLarge; innerItLarge != endSameLarge;
           ++innerItLarge) {
        action(*itSmall, *innerItLarge);
      }
    }
    itSmall = endSameSmall;
    itLarge = endSameLarge;
  }
}

// TODO<joka921> Comment this abstraction
template <typename Range>
[[nodiscard]] size_t specialOptionalJoin(
    Range&& range1, Range&& range2, auto&& lessThan, auto&& compatibleRowAction) {
  auto it1 = range1.begin();
  auto begUndef = range1.begin();
  auto endUndef = range1.begin();
  auto end1 = range1.end();
  auto it2 = range2.begin();
  auto end2 = range2.end();


  while (it1 < end1 && it2 < end2) {
    while (lessThan(*it1, *it2))
  }



}
}  // namespace ad_utility
