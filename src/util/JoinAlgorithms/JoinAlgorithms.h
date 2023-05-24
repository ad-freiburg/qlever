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
#include "util/JoinAlgorithms/JoinColumnMapping.h"

namespace ad_utility {

// A function that takes an arbitrary number of arguments by reference and does
// nothing. This is used below when certain customization points of the join
// algorithms don't have to do anything.
struct Noop {
  void operator()(const auto&...) const {
    // This function deliberately does nothing (static analysis expects a
    // comment here).
  }
};
[[maybe_unused]] static inline auto noop = Noop{};

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

// Note: In the following functions, two rows of IDs are called `compatible` if
// for each position they are equal, or at least one of them is UNDEF. This is
// exactly the semantics of the SPARQL standard for rows that match in a JOIN
// operation.

/**
 * @brief This function performs a merge/zipper join that also handles UNDEF
 * values correctly. It is highly configurable to also support OPTIONAL joins
 * and MINUS and to allow for optimizations when some of the columns don't
 * contain UNDEF values.
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
 * @param findSmallerUndefRangesLeft A function that takes an element `el` from
 * `right` and returns all the iterators to elements from `left` that are
 * smaller than `el` but are still compatible to `el` because of UNDEF
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
 * `lessThan`. A result being out of order can happen if two rows with UNDEF
 * values in different places are merged, or when performing OPTIONAL or MINUS
 * with UNDEF values in the left input. TODO<joka921> The second of the
 * described cases leads to two sorted ranges in the output, this can possibly
 * be exploited to fix the result in a cheaper way than a full sort.
 */
template <
    std::ranges::random_access_range Range,
    BinaryRangePredicate<Range> LessThan, typename FindSmallerUndefRangesLeft,
    typename FindSmallerUndefRangesRight,
    UnaryIteratorFunction<Range> ElFromFirstNotFoundAction = decltype(noop)>
[[nodiscard]] size_t zipperJoinWithUndef(
    const Range& left, const Range& right, const LessThan& lessThan,
    const BinaryIteratorFunction<Range> auto& compatibleRowAction,
    const FindSmallerUndefRangesLeft& findSmallerUndefRangesLeft,
    const FindSmallerUndefRangesRight& findSmallerUndefRangesRight,
    ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
  using Iterator = std::ranges::iterator_t<Range>;

  // If this is not an OPTIONAL join or a MINUS we can apply several
  // optimizations, so we store this information.
  static constexpr bool hasNotFoundAction =
      !ad_utility::isSimilar<ElFromFirstNotFoundAction, decltype(noop)>;

  // Iterators for both ranges that will be used to advance during the zipper
  // algorithm.
  auto it1 = std::begin(left);
  auto end1 = std::end(left);
  auto it2 = std::begin(right);
  auto end2 = std::end(right);

  // If this is an OPTIONAL join or a MINUS then we have to keep track of the
  // information for which elements from the left input we have already found a
  // match in the right input. We call these elements "covered". For all
  // uncovered elements the `elFromFirstNotFoundAction` has to be called at the
  // end.
  std::vector<bool> coveredFromLeft;
  if constexpr (hasNotFoundAction) {
    coveredFromLeft.resize(left.size());
  }
  auto cover = [&coveredFromLeft, begLeft = std::begin(left)](auto it) {
    if constexpr (hasNotFoundAction) {
      coveredFromLeft[it - begLeft] = true;
    }
    (void)coveredFromLeft, (void)begLeft;
  };

  // Store the information whether the output contains rows that are completely
  // out of order because matching rows with UNDEF values in different columns
  // were encountered.
  bool outOfOrderFound = false;

  // The following function has to be called for every element in `right`. It
  // finds all elements in `left` that are smaller than the element, but are
  // compatible with it (because of UNDEF values this might happen) and adds
  // these matches to the result. The range `[leftBegin, leftEnd)` must cover
  // all elements in `left` that are smaller than `*itFromRight` to work
  // correctly. It would thus be always correct to pass in `left.begin()` and
  // `left.end()`, but passing in smaller ranges is more efficient.
  auto mergeWithUndefLeft = [&](Iterator itFromRight, Iterator leftBegin,
                                Iterator leftEnd) {
    if constexpr (!isSimilar<FindSmallerUndefRangesLeft, Noop>) {
      // We need to bind the const& to a variable, else it will be
      // dangling inside the `findSmallerUndefRangesLeft` generator.
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

  // This function returns true if and only if the given `row` (which is an
  // element of `left` or `right`) constains no UNDEF values. It is used inside
  // the following `mergeWithUndefRight` function.
  auto containsNoUndefined = []<typename T>(const T& row) {
    if constexpr (std::is_same_v<T, Id>) {
      return row != Id::makeUndefined();
    } else {
      return (std::ranges::none_of(
          row, [](Id id) { return id == Id::makeUndefined(); }));
    }
  };

  // This function is the inverse of `mergeWithUndefLeft` above. The bool
  // argument `hasNoMatch` has to be set to `true` iff no exact match for
  // `*itFromLeft` was found in `right`. If `hasNoMatch` is `true` and no
  // matching smaller rows are found in `right` and `*itFromLeft` contains no
  // UNDEF values, then the `elFromFirstNotFoundAction` is directly called
  // from inside this lambda. That way, the "optional" row will be in the
  // correct place in the result. The condition about containing no UNDEF values
  // is important, because otherwise `*itFromLeft` may be compatible to a larger
  // element in `right` that is only discovered later.
  auto mergeWithUndefRight = [&](Iterator itFromLeft, Iterator beginRight,
                                 Iterator endRight, bool hasNoMatch) {
    if constexpr (!isSimilar<FindSmallerUndefRangesRight, Noop>) {
      bool compatibleWasFound = false;
      // We need to bind the const& to a variable, else it will be
      // dangling inside the `findSmallerUndefRangesRight` generator.
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

  // The main loop of the zipper algorithm. It is wrapped in a lambda, so we can
  // easily `break` from an inner loop via a `return` statement.
  [&]() {
    // Iterate over all elements in `left` and `right` that have no exact
    // matches in the counterpart. We have to still call the correct
    // `mergeWithUndef...` function for each element.
    while (it1 < end1 && it2 < end2) {
      while (lessThan(*it1, *it2)) {
        // The last argument means "no exact match was found for this element".
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

      // Find the following ranges in `left` and `right` where the elements are
      // equal.
      // TODO <joka921> Maybe we can pass in the equality operator for increased
      // performance. We can also use `lessThan` directly, but then the order of
      // the two loops above is important.
      auto eq = [&lessThan](const auto& el1, const auto& el2) {
        return !lessThan(el1, el2) && !lessThan(el2, el1);
      };
      auto endSame1 = std::find_if_not(
          it1, end1, [&](const auto& row) { return eq(row, *it2); });
      auto endSame2 = std::find_if_not(
          it2, end2, [&](const auto& row) { return eq(*it1, row); });

      for (auto it = it1; it != endSame1; ++it) {
        mergeWithUndefRight(it, std::begin(right), it2, false);
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

  // Deal with the remaining elements that have no exact match in the other
  // input.
  for (auto it = it2; it != end2; ++it) {
    mergeWithUndefLeft(it, std::begin(left), std::end(left));
  }
  for (; it1 != end1; ++it1) {
    mergeWithUndefRight(it1, std::begin(right), std::end(right), true);
  }

  // If this is an OPTIONAL or MINUS join it might be that we have elements from
  // `left` that have no match in `right` and for which the
  // `elFromFirstNotFoundAction` hast not yet been called. Those are now added
  // to the result. These elements form a sorted range at the end of the output.
  size_t numOutOfOrderAtEnd = 0;
  if constexpr (hasNotFoundAction) {
    for (size_t i = 0; i < coveredFromLeft.size(); ++i) {
      if (!coveredFromLeft[i]) {
        elFromFirstNotFoundAction(std::begin(left) + i);
        ++numOutOfOrderAtEnd;
      }
    }
  }

  // If The return value is 0, then the result is sorted. If the return value is
  // `max()` then we can provide no guarantees about the sorting of the result.
  // Otherwise, the result consists of two consecutive sorted ranges, the second
  // of which has length `returnValue`.
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
 * @param elementFromSmallerNotFoundAction Is called for each element in
 * `smaller` that has no matching counterpart in `larger`. Can be used to
 * implement very efficient OPTIONAL or MINUS if neither of the inputs contains
 * UNDEF values, and if the left operand is much smaller.
 */
template <std::ranges::random_access_range Range,
          typename ElementFromSmallerNotFoundAction = Noop>
void gallopingJoin(
    const Range& smaller, const Range& larger,
    BinaryRangePredicate<Range> auto const& lessThan,
    BinaryIteratorFunction<Range> auto const& action,
    ElementFromSmallerNotFoundAction elementFromSmallerNotFoundAction = {}) {
  auto itSmall = std::begin(smaller);
  auto endSmall = std::end(smaller);
  auto itLarge = std::begin(larger);
  auto endLarge = std::end(larger);
  auto eq = [&lessThan](const auto& el1, const auto& el2) {
    return !lessThan(el1, el2) && !lessThan(el2, el1);
  };

  // Perform an exponential search for the element `*itS` in the range `[itL,
  // endLarge]`. Return a pair `(lower, upper)` of iterators, where the first
  // element that is `>=*itS` is in `[lower, upper)`. The only exception to this
  // is when the whole range (itL, endLarge) is smaller than `*itS`. In this
  // case the second iterator will be `endLarge`. This is defined in a way, s.t.
  // a subsequent `std::lower_bound(lower, upper)` will either find the element
  // or will return `upper` and `upper == endLarge`.
  auto exponentialSearch = [&lessThan, &endLarge](auto itL, auto itS) {
    size_t step = 1;
    auto lower = itL;
    while (lessThan(*itL, *itS)) {
      lower = itL;
      itL += step;
      step *= 2;
      if (itL >= endLarge) {
        return std::pair{lower, endLarge};
      }
    }
    // It might be, that `itL` points to the first element that is >= *itS. We
    // thus have to add one to the second element, s.t. the second element is a
    // guaranteed upper bound.
    return std::pair{lower, itL + 1};
  };
  while (itSmall < endSmall && itLarge < endLarge) {
    const auto& elLarge = *itLarge;
    // Linear search in the smaller input.
    while (lessThan(*itSmall, elLarge)) {
      if constexpr (!ad_utility::isSimilar<ElementFromSmallerNotFoundAction,
                                           Noop>) {
        elementFromSmallerNotFoundAction(itSmall);
      }
      ++itSmall;
      if (itSmall >= endSmall) {
        return;
      }
    }
    // Exponential search followed by binary search on the larger input.
    auto [lower, upper] = exponentialSearch(itLarge, itSmall);
    const auto& needle = *itSmall;
    itLarge =
        std::lower_bound(lower, upper, needle,
                         [&lessThan](const auto& a, const auto& b) -> bool {
                           return lessThan(a, b);
                         });
    if (itLarge == endLarge) {
      break;
    }

    // Find the ranges where both inputs are equal and add them to the result.
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
  if constexpr (!ad_utility::isSimilar<ElementFromSmallerNotFoundAction,
                                       Noop>) {
    while (itSmall != endSmall) {
      elementFromSmallerNotFoundAction(itSmall);
      ++itSmall;
    }
  }
}

/**
 * @brief Perform an OPTIONAL join for the following special case: The `right`
 * input contains no UNDEF values in any of its join columns, the `left`
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
 * @param compatibleRowAction Same as in `zipperJoinWithUndef`
 * @param elFromFirstNotFoundAction Same as in `zipperJoinWithUndef`
 */
void specialOptionalJoin(
    const IdTableView<0>& left, const IdTableView<0>& right,
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

  size_t numColumns = (*it1).size();
  // A predicate that compares two rows lexicographically but ignores the last
  // column.
  auto compareAllButLast = [numColumns](const auto& a, const auto& b) {
    for (size_t i = 0; i < numColumns - 1; ++i) {
      if (a[i] != b[i]) {
        return a[i] < b[i];
      }
    }
    return false;
  };

  // Similar to the previous lambda, but checks for equality.
  auto compareEqButLast = [numColumns](const auto& a, const auto& b) {
    for (size_t i = 0; i < numColumns - 1; ++i) {
      if (a[i] != b[i]) {
        return false;
      }
    }
    return true;
  };

  // The last columns from the left and right input. Those will be dealt with
  // separately.
  std::span<const Id> lastColumnLeft = left.getColumn(left.numColumns() - 1);
  std::span<const Id> lastColumnRight = right.getColumn(right.numColumns() - 1);

  while (it1 < end1 && it2 < end2) {
    // Skip over rows in `right` where the first columns don't match.
    it2 = std::find_if_not(it2, end2, [&](const auto& row) {
      return compareAllButLast(row, *it1);
    });
    if (it2 == end2) {
      break;
    }

    // Skip over rows in `left` where the first columns don't match, but call
    // the `notFoundAction` on them.
    auto next1 = std::find_if_not(it1, end1, [&](const auto& row) {
      return compareAllButLast(row, *it2);
    });

    for (auto it = it1; it != next1; ++it) {
      elFromFirstNotFoundAction(it);
    }
    it1 = next1;

    // Find the rows where the left and the right input match on the first
    // columns.
    auto endSame1 = std::find_if_not(it1, end1, [&](const auto& row) {
      return compareEqButLast(row, *it2);
    });
    auto endSame2 = std::find_if_not(it2, end2, [&](const auto& row) {
      return compareEqButLast(*it1, row);
    });
    if (endSame1 == it1) {
      continue;
    }

    // For the rows where the first columns match we will perform a one-column
    // join on the last column. This can be done efficiently, because the UNDEF
    // values are at the beginning of these sub-ranges.

    // Set up the corresponding sub-ranges of the last columns.
    auto beg = it1 - left.begin();
    auto end = endSame1 - left.begin();
    std::span<const Id> leftSub{lastColumnLeft.begin() + beg,
                                lastColumnLeft.begin() + end};
    beg = it2 - right.begin();
    end = endSame2 - right.begin();
    std::span<const Id> rightSub{lastColumnRight.begin() + beg,
                                 lastColumnRight.begin() + end};

    // Set up the generator for the UNDEF values.
    // TODO<joka921> We could probably also apply this optimization if both
    // inputs contain UNDEF values only in the last column, and possibly
    // also not only for `OPTIONAL` joins.
    auto endOfUndef = std::ranges::find_if_not(leftSub, &Id::isUndefined);
    auto findSmallerUndefRangeLeft =
        [leftSub,
         endOfUndef](auto&&...) -> cppcoro::generator<decltype(endOfUndef)> {
      for (auto it = leftSub.begin(); it != endOfUndef; ++it) {
        co_yield it;
      }
    };

    // Also set up the actions for compatible rows that now work on single
    // columns.
    auto compAction = [&compatibleRowAction, it1, it2, &leftSub, &rightSub](
                          const auto& itL, const auto& itR) {
      compatibleRowAction((it1 + (itL - leftSub.begin())),
                          (it2 + (itR - rightSub.begin())));
    };
    auto notFoundAction = [&elFromFirstNotFoundAction, &it1,
                           begin = leftSub.begin()](const auto& it) {
      elFromFirstNotFoundAction(it1 + (it - begin));
    };

    // Perform the join on the last column.
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
