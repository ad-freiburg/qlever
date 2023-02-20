//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "global/Id.h"
#include "util/Generator.h"
#include "engine/idTable/IdTable.h"

namespace ad_utility {

// TODO<joka921> Comment
// TODO<joka921> We could also implement a version that is optimized on the [begin, end] range not having
// UNDEF values in some of the columns
template<typename Row>
auto findSmallerUndefRangesForRowsWithoutUndef(const auto& row,
                            auto begin, auto end) ->cppcoro::generator<decltype(begin)> {

  assert(row.size() == begin->size());
  size_t numJoinColumns = row.size();
  // TODO<joka921> This can be done without copying.
  Row rowLower = row;

  size_t upperBound = std::pow(2, row.size());
  for (size_t i = 0; i < upperBound - 1; ++i) {
    for (size_t j = 0; j < numJoinColumns; ++j) {
      rowLower[j] =
          (i >> (numJoinColumns - j - 1)) & 1 ? row[j] : ValueId::makeUndefined();
    }

    auto [begOfUndef, endOfUndef] = std::equal_range(begin, end, rowLower,
    std::ranges::lexicographical_compare);
    for (; begOfUndef != endOfUndef; ++begOfUndef) {
      co_yield begOfUndef;
    }
  }
}

template<typename Row>
auto findSmallerUndefRangesForRowsWithUndefInLastColumns(const auto& row, const size_t numJoinColumns, const size_t numLastUndefined,
                                               auto begin, auto end) ->cppcoro::generator<decltype(begin)> {
  assert(numJoinColumns > numLastUndefined);
  assert(numLastUndefined > 0);
  // TODO<joka921> This can be done without copying.
  Row rowLower = row;

  size_t numDefinedColumns = numJoinColumns - numLastUndefined;
  size_t upperBound = std::pow(2, numDefinedColumns);
  for (size_t permutationCounter = 0; permutationCounter < upperBound - 1; ++permutationCounter) {
    for (size_t colIdx = 0; colIdx < numDefinedColumns; ++colIdx) {
      rowLower[colIdx] =
          (permutationCounter >> colIdx) & 1 ? row[colIdx] : ValueId::makeUndefined();
    }

  auto begOfUndef = std::lower_bound(begin, end, rowLower,std::ranges::lexicographical_compare);
  static_assert(Id::makeUndefined().getBits() == 0);
  rowLower[numDefinedColumns] = Id::fromBits(1);
  auto endOfUndef = std::lower_bound(begin, end, rowLower,std::ranges::lexicographical_compare);
    for (; begOfUndef != endOfUndef; ++begOfUndef) {
      co_yield begOfUndef;
    }
  }
}

// The (very expensive) general case:
// The `row` contains UNDEF values in arbitrary positions, and same goes for the range [begin, end].
auto findSmallerUndefRanges(const auto& row, const size_t numJoinColumns,
                            auto begin, auto end) ->cppcoro::generator<decltype(begin)> {
  auto isCompatible = [&](const auto& otherRow) {
    for (size_t k=0u; k < numJoinColumns; ++k) {
      Id a = row[k];
      Id b = otherRow[k];
      bool aUndef = a == Id::makeUndefined();
      bool bUndef = b == Id::makeUndefined();
      bool eq = a==b;
      auto match = aUndef || bUndef || eq;
      if (!match) {
        return false;
      }
    }
    return true;
  };

  for (auto it = begin; it!= end; ++it) {
    if (isCompatible(*it)) {
      co_yield it;
      }
  }
  co_return;
}

template <typename Row>
auto makeSmallerUndefRanges(const size_t numJoinColumns) {
  return [&](const auto& rowLeft, auto begin, auto end) {
    return ad_utility::findSmallerUndefRanges<Row>(rowLeft, numJoinColumns,
                                                        begin, end);
  };
}

// TODO<joka921> Do we need a specialized overload for a single table?
// TODO<joka921> Add a reasonable constraint for the join columns.
template <typename ROW_A, typename ROW_B, int TABLE_WIDTH>
void addCombinedRowToIdTable(const ROW_A& row1, const ROW_B& row2,
                             const size_t numJoinColumns,
                             IdTableStatic<TABLE_WIDTH>* table) {
  auto& result = *table;
  result.emplace_back();
  size_t backIdx = result.size() - 1;

  std::cout << "Row a ";
  for (auto id : row1) {
    std::cout << id << ' ';
  }
  std::cout << "\nRow b ";
  for (auto id : row2) {
    std::cout << id;
  }
  std::cout << '\n' << std::endl;

  auto mergeWithUndefined = [](const ValueId a, const ValueId b) {
    static_assert(ValueId::makeUndefined().getBits() == 0u);
    return ValueId::fromBits(a.getBits() | b.getBits());
  };

  // fill the result
  size_t rIndex = 0;
  // TODO<joka921> We could possibly implement an optimization for the case where we know that there are no
  // undefined values in at least one of the inputs, but maybe that doesn't help too much.
  for (size_t col = 0; col < numJoinColumns; col++) {
    result(backIdx, rIndex) = mergeWithUndefined(row1[col], row2[col]);
    rIndex++;
  }

  for (size_t col = numJoinColumns; col < row1.numColumns(); ++col) {
    result(backIdx, rIndex) = row1[col];
    rIndex++;
  }

  for (size_t col = numJoinColumns; col < row2.numColumns(); col++) {
      result(backIdx, rIndex) = row2[col];
      rIndex++;
  }
}

auto makeCombineRows(const size_t numJoinColumns, auto& result) {
  auto combineRows = [numJoinColumns, &result](
                         const auto& row1, const auto& row2) {
    return ad_utility::addCombinedRowToIdTable(row1, row2, numJoinColumns, &result);
  };
  return combineRows;
}

[[maybe_unused]] static inline auto noop = [](auto&&...) {};
// TODO<joka921> Comment, cleanup, move into header.
template <typename Range, typename ElFromFirstNotFoundAction = decltype(noop)>
void zipperJoinWithUndef(
    Range&& range1, Range&& range2, auto&& lessThan,
    auto&& compatibleRowAction, auto&& findSmallerUndefRangesLeft,
    auto&& findSmallerUndefRangesRight,
    ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
  auto it1 = range1.begin();
  auto end1 = range1.end();
  auto it2 = range2.begin();
  auto end2 = range2.end();

  std::vector<bool> coveredFromLeft(end1 - it1);
  auto cover = [&](auto it) {
    coveredFromLeft[it - range1.begin()] = true;
  };

  auto makeMergeWithUndefLeft = [&cover, &compatibleRowAction] < bool reversed> (const auto& lt,
                                                       const auto& findUndef) {
    return [&cover, &lt, &findUndef, &compatibleRowAction](
               const auto& el, auto startOfRange, auto endOfRange) {
      (void) cover;
      bool compatibleWasFound = false;
      auto smallerUndefRanges = findUndef(el, startOfRange, endOfRange);
      //std::cout << "merging with undef ranges" << '\n';
      for (const auto& it : smallerUndefRanges) {
          if (lt(*it, el)) {
            compatibleWasFound = true;
            if constexpr (reversed) {
              compatibleRowAction(el, *it);
            } else {
              compatibleRowAction(el, *it);
              cover(it);
            }
          }
        }
      return compatibleWasFound;
    };
  };
  auto mergeWithUndefLeft = makeMergeWithUndefLeft.template operator()<false>(
      lessThan, findSmallerUndefRangesLeft);
  auto mergeWithUndefRight =
      makeMergeWithUndefLeft.template operator()<true>(lessThan, findSmallerUndefRangesRight);

  [&]() {
    while (it1 < end1 && it2 < end2) {
      while (lessThan(*it1, *it2)) {
        if (!mergeWithUndefRight(*it1, range2.begin(), it2)) {
          if (std::ranges::none_of(*it1, [](Id id){return id == Id::makeUndefined();})) {
            cover(it1);
            elFromFirstNotFoundAction(*it1);
          }
        } else {
          cover(it1);
        }
        ++it1;
        if (it1 >= end1) {
          return;
        }
      }
      while (lessThanReversed(*it2, *it1)) {
        mergeWithUndefLeft(*it2, range1.begin(), it1);
        ++it2;
        if (it2 >= end2) {
          return;
        }
      }

      // TODO <joka921> Maybe we can pass in the equality operator for increased performance.
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

  std::for_each(it2, end2,
                [&](const auto& row) {
                  mergeWithUndefLeft(row, range1.begin(), range1.end());});

  for (; it1 < end1; ++it1) {
    cover(it1);
    if (!mergeWithUndefRight(*it1, range2.begin(), range2.end())) {
      elFromFirstNotFoundAction(*it1); } else {
    }}

  // TODO<joka921> These will be out of order;
  for (size_t i = 0; i < coveredFromLeft.size(); ++i) {
    if (!coveredFromLeft[i]) {
      elFromFirstNotFoundAction(*(range1.begin() + i));
    }
  }
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
}  // namespace ad_utility
