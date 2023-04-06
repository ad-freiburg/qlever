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

// Some helper concepts.

// A predicate type `Pred` fulfills `BinaryRangePredicate<Range>` if it can be
// called with two values of the `Range`'s `value_type` and produces a result
// that can be converted to `bool`.
template <typename Pred, typename Range>
concept BinaryRangePredicate =
    std::indirect_binary_predicate<Pred, std::ranges::iterator_t<Range>,
                                   std::ranges::iterator_t<Range>>;

// A  function `F` fulfills `UnaryIteratorFunction` if it can be called with a
// single argument of the `Range`'s iterator type (NOT value type).
template <typename F, typename Range>
concept UnaryIteratorFunction =
    std::invocable<F, std::ranges::iterator_t<Range>>;

// A  function `F` fulfills `BinaryIteratorFunction` if it can be called with
// two arguments of the `Range`'s iterator type (NOT value type).
template <typename F, typename Range>
concept BinaryIteratorFunction =
    std::invocable<F, std::ranges::iterator_t<Range>,
                   std::ranges::iterator_t<Range>>;

/**
 * @brief This function performs a merge/zipper join that also handles UNDEF
 * values correctly. It is highly configurable to also support OPTIONAL joins
 * and MINUS and to allow for optimizations when some of the columns don't
 * contain undefined values.
 * @param left The left input to the join algorithm. Typically a range of rows
 * of IDs (e.g.`IdTable` or `IdTableView`).
 * @param right The right input to the join algorithm.
 * @param lessThan This function is called with one element from `left` and
 * `right` each and must return `true` if the first argument comes before the
 * second one. The ranges `left` and `right` must be sorted according to this
 *          function.
 * @param compatibleRowAction When an element from `left` and an element from
 * `right` match (either because they compare equal wrt the `lessThan` relation
 * or because they are found by the `findSmallerUndef...` functions (see below),
 * this function is called with the two *iterators* to the  elements, i.e .
 * `compatibleRowAction(itToLeft, itToRight)`
 * @param findSmallerUndefRangesLeft A function that gets an element `el` from
 * `right` and returns all the iterators to elements from `left` that are
 * smaller than `el` but are still compatible to `el` because of undefined
 * values. This should be one of the functions in `FindUndefRanges.h` or
 * `ad_utility::noop` if it is known in advance that the left input contains no
 * UNDEF values.
 * @param findSmallerUndefRangesRight Similar to `findSmallerUndefRangesLeft`
 * but reversed: gets an element from `left` and returns the compatible but
 * smaller elements from `right`.
 * @param elFromFirstNotFoundAction This function is called for each iterator in
 * `left` for which no corresponding match in `right` was found. This is `noop`
 * for "normal` joins, but can be set to implement `OPTIONAL` or `MINUS`.
 * @return 0 if the result is sorted, > 0 if the result is not sorted. `Sorted`
 * means that all the calls to `compatibleRowAction` were ordered wrt
 * `lessThan`. A result being out of order can happen if two rows with undefined
 * values in different places are merged, or when performing OPTIONAL or MINUS
 * with undefined values in the left input. TODO<joka921> The second of the
 * described cases leads to two sorted ranges in the output, this can possibly
 * be exploited to fix the result in a cheaper way than a full sort.
 */
template <
    std::ranges::random_access_range Range,
    BinaryRangePredicate<Range> LessThan,
    UnaryIteratorFunction<Range> ElFromFirstNotFoundAction = decltype(noop)>
[[nodiscard]] size_t zipperJoinWithUndef(
    const Range& left, const Range& right, const LessThan& lessThan,
    const BinaryIteratorFunction<Range> auto& compatibleRowAction,
    const auto& findSmallerUndefRangesLeft,
    const auto& findSmallerUndefRangesRight,
    ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
  using Iterator = std::ranges::iterator_t<Range>;
  static constexpr bool hasNotFoundAction =
      !ad_utility::isSimilar<ElFromFirstNotFoundAction, decltype(noop)>;
  auto it1 = std::begin(left);
  auto end1 = std::end(left);
  auto it2 = std::begin(right);
  auto end2 = std::end(right);

  std::vector<bool> coveredFromLeft;
  if constexpr (hasNotFoundAction) {
    coveredFromLeft.resize(end1 - it1);
  }
  auto cover = [&coveredFromLeft, begLeft = std::begin(left)](auto it) {
    if constexpr (hasNotFoundAction) {
      coveredFromLeft[it - begLeft] = true;
    }
  };

  bool outOfOrderFound = false;

  auto mergeWithUndefLeft = [&](Iterator itFromRight, Iterator leftBegin,
                                Iterator leftEnd) {
    if constexpr (!isSimilar<decltype(findSmallerUndefRangesLeft),
                             decltype(noop)>) {
      // We need to bind the const& to a variable, else it will be
      // dangling inside the `findUndef` coroutine.
      const auto& row = *itFromRight;
      for (const auto& it : findSmallerUndefRangesLeft(row, leftBegin, leftEnd,
                                                       outOfOrderFound)) {
        if (lessThan(*it, *itFromRight)) {
          compatibleRowAction(it, itFromRight);
          cover(it);
        }
      }
    }
  };
  auto containsNoUndefined = []<typename T>(const T& row) {
    if constexpr (std::is_same_v<T, Id>) {
      return row != Id::makeUndefined();
    } else {
      return (std::ranges::none_of(
          row, [](Id id) { return id == Id::makeUndefined(); }));
    }
  };

  auto mergeWithUndefRight = [&](Iterator itFromLeft, Iterator beginRight,
                                 Iterator endRight, bool hasNoMatch) {
    if constexpr (!isSimilar<decltype(findSmallerUndefRangesRight),
                             decltype(noop)>) {
      bool compatibleWasFound = false;
      // We need to bind the const& to a variable, else it will be
      // dangling inside the `findUndef` coroutine.
      const auto& row = *itFromLeft;
      for (const auto& it : findSmallerUndefRangesRight(
               row, beginRight, endRight, outOfOrderFound)) {
        if (lessThan(*it, *itFromLeft)) {
          compatibleWasFound = true;
          compatibleRowAction(itFromLeft, it);
        }
      }
      if (compatibleWasFound) {
        cover(itFromLeft);
      } else if (hasNoMatch && containsNoUndefined(row)) {
        elFromFirstNotFoundAction(itFromLeft);
        cover(itFromLeft);
      }
    } else {
      if (hasNoMatch && containsNoUndefined(*itFromLeft)) {
        elFromFirstNotFoundAction(itFromLeft);
        cover(itFromLeft);
      }
    }
  };

  [&]() {
    while (it1 < end1 && it2 < end2) {
      while (lessThan(*it1, *it2)) {
        mergeWithUndefRight(it1, std::begin(right), it2, true);
        ++it1;
        if (it1 >= end1) {
          return;
        }
      }
      while (lessThan(*it2, *it1)) {
        mergeWithUndefLeft(it2, std::begin(left), it1);
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

      bool hasNoMatch = it2 == endSame2;
      for (auto it = it1; it != endSame1; ++it) {
        mergeWithUndefRight(it, std::begin(right), it2, hasNoMatch);
      }
      for (auto it = it2; it != endSame2; ++it) {
        mergeWithUndefLeft(it, std::begin(left), it1);
      }

      for (; it1 != endSame1; ++it1) {
        cover(it1);
        for (auto innerIt2 = it2; innerIt2 != endSame2; ++innerIt2) {
          compatibleRowAction(it1, innerIt2);
        }
      }
      it1 = endSame1;
      it2 = endSame2;
    }
  }();

  for (auto it = it2; it != end2; ++it) {
    mergeWithUndefLeft(it, std::begin(left), std::end(left));
  }

  for (; it1 != end1; ++it1) {
    mergeWithUndefRight(it1, std::begin(right), std::end(right), true);
  }

  size_t numOutOfOrderAtEnd = 0;

  if constexpr (hasNotFoundAction) {
    for (size_t i = 0; i < coveredFromLeft.size(); ++i) {
      if (!coveredFromLeft[i]) {
        elFromFirstNotFoundAction(std::begin(left) + i);
        ++numOutOfOrderAtEnd;
      }
    }
  }
  return outOfOrderFound ? std::numeric_limits<size_t>::max()
                         : numOutOfOrderAtEnd;
}

/**
 *
 * @brief Perform the galloping join algorithm on a `smaller` and a `larger`
 * input. For each pair of matching iterators, the given `action` is called.
 * @param smaller The left input to the join. Must not contain UNDEF values,
 * otherwise the result is wrong. Should be much smaller than `larger`,
 * otherwise the zipper/merge join algorithm should be used for efficiency
 * reasons.
 * @param larger The right input to the join. Must not contain UNDEF values,
 * otherwise the result is wrong.
 * @param lessThan The predicate that is used to identify equal entry pairs in
 * `smaller` and `larger`. Both inputs must be sorted by this predicate.
 * @param action For each pair of equal entries (entryFromSmaller,
 * entryFromLarger), this function is called with the iterators to the matching
 * entries as arguments. These calls will be in ascending order wrt `lessThan`,
 * meaning that the result will be sorted.
 */
template <std::ranges::random_access_range Range>
void gallopingJoin(const Range& smaller, const Range& larger,
                   BinaryRangePredicate<Range> auto const& lessThan,
                   BinaryIteratorFunction<Range> auto const& action) {
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
        action(itSmall, innerItLarge);
      }
    }
    itSmall = endSameSmall;
    itLarge = endSameLarge;
  }
}

/**
 * @brief Perform an OPTIONAL join for the following special case: The `left`
 * input contains no UNDEF values in any of its join columns, the `right`
 * range contains UNDEF values only in its least significant join column. The
 * meaning of the other parameters and the preconditions on the input are the
 * same as for the more general `zipperJoinWithUndef` algorithm above. Note:
 * This algorithms can also be used to implement a MINUS operation for inputs
 * with the same preconditions by specifying an appropriate
 * `elFromFirstNotFoundAction`.
 * @param left The left input of the optional join. Must not contain UNDEF
 * values in the join columns.
 * @param right The right input of the optional join. Must only contain UNDEF
 * values in the least significant join column
 * @param lessThan Same as in `zipperJoinWithUndef`
 * @param compatibleRowAction Same as in `zipperJoinWithUndef`
 * @param elFromFirstNotFoundAction Same as in `zipperJoinWithUndef`
 */
void specialOptionalJoin(
    const IdTableView<0>& left, const IdTableView<0>& right,
    const auto& lessThan,
    const BinaryIteratorFunction<IdTableView<0>> auto& compatibleRowAction,
    const UnaryIteratorFunction<IdTableView<0>> auto&
        elFromFirstNotFoundAction) {
  auto it1 = std::begin(left);
  auto end1 = std::end(left);
  auto it2 = std::begin(right);
  auto end2 = std::end(right);

  if (std::empty(left)) {
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

  std::span<const Id> lastColumn1 = left.getColumn(left.numColumns() - 1);
  std::span<const Id> lastColumn2 = right.getColumn(left.numColumns() - 1);

  while (it1 < end1 && it2 < end2) {
    it2 = std::find_if_not(
        it2, end2, [&](const auto& row) { return lessThan(row, *it1); });
    if (it2 == end2) {
      break;
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

    auto beg = it1 - left.begin();
    auto end = endSame1 - left.begin();
    std::span<const Id> leftSub{lastColumn1.begin() + beg,
                                lastColumn1.begin() + end};
    beg = it2 - right.begin();
    end = endSame2 - right.begin();
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

    auto compAction = [&](const auto& itL, const auto& itR) {
      compatibleRowAction((it1 + (itL - leftSub.begin())),
                          (it2 + (itR - rightSub.begin())));
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
