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
#include "util/JoinAlgorithms/FindUndefRanges.h"
#include "util/JoinAlgorithms/JoinColumnData.h"

namespace ad_utility {

// A function that takes an arbitrary number of arguments by reference and does
// nothing.
[[maybe_unused]] static inline auto noop = [](const auto&...) {
  // This function deliberately does nothing (static analysis expectes a comment
  // here).
};

template <typename Pred, typename Range>
concept BinaryRangePredicate =
    std::indirect_binary_predicate<Pred, std::ranges::iterator_t<Range>,
                                   std::ranges::iterator_t<Range>>;

template <typename F, typename Range>
concept UnaryIteratorFunction =
    std::invocable<F, std::ranges::iterator_t<Range>>;

template <typename F, typename Range>
concept BinaryIteratorFunction =
    std::invocable<F, std::ranges::iterator_t<Range>,
                   std::ranges::iterator_t<Range>>;

// TODO<joka921> Comment, cleanup, move into header.
template <
    std::ranges::random_access_range Range,
    BinaryRangePredicate<Range> LessThan,
    UnaryIteratorFunction<Range> ElFromFirstNotFoundAction = decltype(noop)>
[[nodiscard]] size_t zipperJoinWithUndef(
    const Range& range1, const Range& range2, const LessThan& lessThan,
    const auto& compatibleRowAction, const auto& findSmallerUndefRangesLeft,
    const auto& findSmallerUndefRangesRight,
    ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
  static constexpr bool hasNotFoundAction =
      !ad_utility::isSimilar<ElFromFirstNotFoundAction, decltype(noop)>;
  auto it1 = std::begin(range1);
  auto end1 = std::end(range1);
  auto it2 = std::begin(range2);
  auto end2 = std::end(range2);

  std::vector<bool> coveredFromLeft(end1 - it1);
  auto cover = [&](auto it) {
    if constexpr (hasNotFoundAction) {
      coveredFromLeft[it - std::begin(range1)] = true;
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
              (void)findUndef;
              (void)compatibleRowAction;
              (void)cover;
              (void)lt;
              (void)outOfOrderFound;
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
          if (!mergeWithUndefRight(*it1, std::begin(range2), it2)) {
            if (containsNoUndefined(*it1)) {
              cover(it1);
              elFromFirstNotFoundAction(it1);
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
        mergeWithUndefLeft(*it2, std::begin(range1), it1);
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
        mergeWithUndefRight(row, std::begin(range2), it2);
      });
      std::for_each(it2, endSame2, [&](const auto& row) {
        mergeWithUndefLeft(row, std::begin(range1), it1);
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
    mergeWithUndefLeft(row, std::begin(range1), std::end(range1));
  });

  if constexpr (hasNotFoundAction) {
    for (; it1 < end1; ++it1) {
      cover(it1);
      if (!mergeWithUndefRight(*it1, std::begin(range2), std::end(range2))) {
        elFromFirstNotFoundAction(it1);
      }
    }
  }

  size_t numOutOfOrderAtEnd = 0;
  // TODO<joka921> These will be out of order;
  if constexpr (hasNotFoundAction) {
    for (size_t i = 0; i < coveredFromLeft.size(); ++i) {
      if (!coveredFromLeft[i]) {
        elFromFirstNotFoundAction(std::begin(range1) + i);
        ++numOutOfOrderAtEnd;
      }
    }
  }
  return outOfOrderFound ? std::numeric_limits<size_t>::max()
                         : numOutOfOrderAtEnd;
}

// _____________________________________________________________________________
template <std::ranges::random_access_range Range>
void gallopingJoin(const Range& smaller, const Range& larger,
                   const auto& lessThan, auto&& action) {
  auto itSmall = std::begin(smaller);
  auto endSmall = std::end(smaller);
  auto itLarge = std::begin(larger);
  auto endLarge = std::end(larger);
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
      while (itLarge > std::begin(larger) && eq(*itSmall, *(itLarge - 1))) {
        --itLarge;
      }
    } else if (lessThan(*itSmall, *itLarge)) {
      // We stepped over the location where l1 and l2 may be equal.
      // Use binary search to locate that spot
      const auto& needle = *itSmall;
      itLarge =
          std::lower_bound(last, itLarge, needle,
                           [&lessThan](const auto& a, const auto& b) -> bool {
                             return lessThan(a, b);
                           });
    }
    // TODO<joka921> The following block is the same for `zipper` and
    // `galloping`, so it can be factored out into a function.
    // Note: We could also use the (cheaper) `lessThan` here, but this would
    // require a lot of caution.
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
template <std::ranges::random_access_range Range>
void specialOptionalJoin(
    const Range& range1, const Range& range2, const auto& lessThan,
    const auto& compatibleRowAction,
    const UnaryIteratorFunction<Range> auto& elFromFirstNotFoundAction) {
  auto it1 = std::begin(range1);
  auto end1 = std::end(range1);
  auto it2 = std::begin(range2);
  auto end2 = std::end(range2);

  if (std::empty(range1)) {
    return;
  }

  size_t numCols = (*it1).size();

  auto compareAllButLast = [numCols](const auto& a, const auto& b) {
    for (size_t i = 0; i < numCols - 1; ++i) {
      if (a[i] != b[i]) {
        return a[i] < b[i];
      }
    }
    return false;
  };

  auto compareEqButLast = [numCols](const auto& a, const auto& b) {
    for (size_t i = 0; i < numCols - 1; ++i) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  };

  std::span<const Id> lastColumn1 = range1.getColumn(range1.numColumns() - 1);
  std::span<const Id> lastColumn2 = range2.getColumn(range1.numColumns() - 1);

  while (it1 < end1 && it2 < end2) {
    it2 = std::find_if_not(
        it2, end2, [&](const auto& row) { return lessThan(row, *it1); });
    if (it2 == end2) {
      break;
      // TODO<joka921> add the remaining elements from (it1 to end1).
    }

    auto next1 = std::find_if_not(it1, end1, [&](const auto& row) {
      return compareAllButLast(row, *it2);
    });

    for (auto it = it1; it != next1; ++it) {
      elFromFirstNotFoundAction(it);
    }
    it1 = next1;
    auto endSame1 = std::find_if_not(it1, end1, [&](const auto& row) {
      return compareEqButLast(row, *it2);
    });
    auto endSame2 = std::find_if_not(it2, end2, [&](const auto& row) {
      return compareEqButLast(*it1, row);
    });
    if (endSame1 == it1) {
      continue;
    }

    auto beg = it1 - range1.begin();
    auto end = endSame1 - range1.begin();
    std::span<const Id> leftSub{lastColumn1.begin() + beg,
                                lastColumn1.begin() + end};
    beg = it2 - range2.begin();
    end = endSame2 - range2.begin();
    std::span<const Id> rightSub{lastColumn2.begin() + beg,
                                 lastColumn2.begin() + end};

    auto endOfUndef = std::find_if(leftSub.begin(), leftSub.end(), [](Id id) {
      return id != Id::makeUndefined();
    });
    auto findSmallerUndefRangeLeft =
        [=](auto&&...) -> cppcoro::generator<decltype(endOfUndef)> {
      for (auto it = leftSub.begin(); it != endOfUndef; ++it) {
        co_yield it;
      }
    };

    auto compAction = [&](const Id& id1, const Id& id2) {
      compatibleRowAction(*(it1 + (&id1 - leftSub.data())),
                          *(it2 + (&id2 - rightSub.data())));
    };
    auto notFoundAction = [&elFromFirstNotFoundAction, &it1,
                           begin = leftSub.begin()](const auto& it) {
      elFromFirstNotFoundAction(it1 + (it - begin));
    };

    size_t numOutOfOrder = ad_utility::zipperJoinWithUndef(
        leftSub, rightSub, std::less<>{}, compAction, findSmallerUndefRangeLeft,
        noop, notFoundAction);
    AD_CORRECTNESS_CHECK(numOutOfOrder == 0);
    it1 = endSame1;
    it2 = endSame2;
  }
  for (auto it = it1; it != end1; ++it) {
    elFromFirstNotFoundAction(it);
  }
}
}  // namespace ad_utility
