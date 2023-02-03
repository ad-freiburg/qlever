//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "global/Id.h"

namespace ad_utility {
// TODO<joka921> Comment, cleanup, move into header.
// TODO<joka921> The `lessThanReversed` interface is very bad.
void zipperJoin(auto&& range1, auto&& range2, auto&& lessThan,
                auto&& lessThanReversed, auto&& action) {
  auto it1 = range1.begin();
  auto end1 = range1.end();
  auto it2 = range2.begin();
  auto end2 = range2.end();
  while (it1 < end1 && it2 < end2) {
    it1 = std::find_if_not(
        it1, end1, [&](const auto& row) { return lessThan(row, *it2); });
    if (it1 >= end1) {
      return;
    }
    it2 = std::find_if_not(it2, end2, [&](const auto& row) {
      return lessThanReversed(row, *it1);
    });
    if (it2 >= end2) {
      return;
    }

    auto eq = [&lessThan, &lessThanReversed](const auto& el1, const auto& el2) {
      return !lessThan(el1, el2) && !lessThanReversed(el2, el1);
    };
    auto endSame1 = std::find_if_not(
        it1, end1, [&](const auto& row) { return eq(row, *it2); });
    auto endSame2 = std::find_if_not(
        it2, end2, [&](const auto& row) { return eq(*it1, row); });

    for (; it1 != endSame1; ++it1) {
      for (auto innerIt2 = it2; innerIt2 != endSame2; ++innerIt2) {
        action(*it1, *innerIt2);
      }
    }
    it1 = endSame1;
    it2 = endSame2;
  }
}

template <typename Row>
auto findSmallerUndefRanges(const auto& row, const auto& joinColumns,
                            auto begin, auto end) {
  std::vector<std::pair<decltype(begin), decltype(end)>> result;
  std::vector<size_t> definedColumns;
  for (auto col : joinColumns) {
    if (row[col] != ValueId::makeUndefined()) {
      definedColumns.push_back(col);
    }
  }

  Row rowCopy = row;

  for (size_t i = 0; i < std::pow(2, definedColumns.size()); ++i) {
    for (size_t j = 0; j < definedColumns.size(); ++j) {
      rowCopy[definedColumns[j]] =
          i >> j ? row[definedColumns[j]] : ValueId::makeUndefined();
    }
    result.push_back(std::equal_range(begin, end, rowCopy,
                                      std::ranges::lexicographical_compare));
  }
  return result;
}

[[maybe_unused]] static inline auto noop = [](auto&&...) {};
// TODO<joka921> Comment, cleanup, move into header.
template <typename ElFromFirstNotFoundAction = decltype(noop)>
void zipperJoinWithUndef(
    auto&& range1, auto&& range2, auto&& lessThan, auto&& lessThanReversed,
    auto&& compatibleRowAction, auto&& findSmallerUndefRangesLeft,
    auto&& findSmallerUndefRangesRight,
    ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
  auto it1 = range1.begin();
  auto end1 = range1.end();
  auto it2 = range2.begin();
  auto end2 = range2.end();

  auto makeMergeWithUndefLeft = [&compatibleRowAction](const auto& lt,
                                                       const auto& findUndef,
                                                       const bool reversed) {
    return [&lt, &findUndef, &compatibleRowAction, reversed](
               const auto& el, auto startOfRange, auto endOfRange) {
      bool compatibleWasFound = false;
      auto smallerUndefRanges = findUndef(el, startOfRange, endOfRange);
      for (auto [begin, end] : smallerUndefRanges) {
        for (; begin != end; ++begin) {
          if (lt(*begin, el)) {
            compatibleWasFound = true;
            if (reversed) {
              compatibleRowAction(*begin, el);
            } else {
              compatibleRowAction(el, *begin);
            }
          }
        }
      }
      return compatibleWasFound;
    };
  };
  auto mergeWithUndefLeft = makeMergeWithUndefLeft(
      lessThanReversed, findSmallerUndefRangesLeft, false);
  auto mergeWithUndefRight =
      makeMergeWithUndefLeft(lessThan, findSmallerUndefRangesRight, true);
  [&]() {
    while (it1 < end1 && it2 < end2) {
      while (lessThan(*it1, *it2)) {
        if (!mergeWithUndefLeft(*it1, range2.begin(), it2)) {
          elFromFirstNotFoundAction(*it1);
        }
        ++it1;
        if (it1 >= end1) {
          return;
        }
      }
      while (lessThanReversed(*it2, *it1)) {
        mergeWithUndefRight(*it2, range1.begin(), it1);
        ++it2;
        if (it2 >= end2) {
          return;
        }
      }
      auto eq = [&lessThan, &lessThanReversed](const auto& el1,
                                               const auto& el2) {
        return !lessThan(el1, el2) && !lessThanReversed(el2, el1);
      };
      auto endSame1 = std::find_if_not(
          it1, end1, [&](const auto& row) { return eq(row, *it2); });
      auto endSame2 = std::find_if_not(
          it2, end2, [&](const auto& row) { return eq(*it1, row); });

      std::for_each(it1, endSame1, [&](const auto& row) {
        mergeWithUndefLeft(row, range2.begin(), it2);
      });
      std::for_each(it2, endSame2, [&](const auto& row) {
        mergeWithUndefLeft(row, range1.begin(), it1);
      });

      for (; it1 != endSame1; ++it1) {
        for (auto innerIt2 = it2; innerIt2 != endSame2; ++innerIt2) {
          compatibleRowAction(*it1, *innerIt2);
        }
      }
      it1 = endSame1;
      it2 = endSame2;
    }
  }();
  std::for_each(it1, end1,
                [&](const auto& row) { elFromFirstNotFoundAction(row); });
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
