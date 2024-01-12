//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <ranges>

#include "engine/idTable/IdTable.h"
#include "global/Id.h"
#include "util/Generator.h"
#include "util/JoinAlgorithms/FindUndefRanges.h"
#include "util/JoinAlgorithms/JoinColumnMapping.h"
#include "util/TransparentFunctors.h"

namespace ad_utility {

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
template <std::ranges::random_access_range Range1,
          std::ranges::random_access_range Range2, typename LessThan,
          typename FindSmallerUndefRangesLeft,
          typename FindSmallerUndefRangesRight,
          typename ElFromFirstNotFoundAction = decltype(noop)>
[[nodiscard]] auto zipperJoinWithUndef(
    const Range1& left, const Range2& right, const LessThan& lessThan,
    const auto& compatibleRowAction,
    const FindSmallerUndefRangesLeft& findSmallerUndefRangesLeft,
    const FindSmallerUndefRangesRight& findSmallerUndefRangesRight,
    ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
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
  auto mergeWithUndefLeft = [&](auto itFromRight, auto leftBegin,
                                auto leftEnd) {
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
    if constexpr (isSimilar<FindSmallerUndefRangesLeft, Noop> &&
                  isSimilar<FindSmallerUndefRangesRight, Noop>) {
      return true;
    } else if constexpr (std::is_same_v<T, Id>) {
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
  auto mergeWithUndefRight = [&](auto itFromLeft, auto beginRight,
                                 auto endRight, bool hasNoMatch) {
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
  size_t numOutOfOrder =
      outOfOrderFound ? std::numeric_limits<size_t>::max() : numOutOfOrderAtEnd;
  return numOutOfOrder;
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
template <std::ranges::random_access_range RangeSmaller,
          std::ranges::random_access_range RangeLarger,
          typename ElementFromSmallerNotFoundAction = Noop>
void gallopingJoin(
    const RangeSmaller& smaller, const RangeLarger& larger,
    auto const& lessThan, auto const& action,
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

namespace detail {
using Range = std::pair<size_t, size_t>;

// Store a contiguous random-access range (e.g. `std::vector`or `std::span`,
// together with a pair of indices `[beginIndex, endIndex)` that denote a
// contiguous subrange of the range. Note that this approach is more robust
// than storing iterators or subranges directly instead of indices, because many
// containers have their iterators invalidated when they are being moved (e.g.
// `std::string` or `IdTable`).
template <typename Block>
class BlockAndSubrange {
 private:
  std::shared_ptr<Block> block_;
  Range subrange_;

 public:
  // The reference type of the underlying container.
  using reference = std::iterator_traits<typename Block::iterator>::reference;
  using const_reference =
      std::iterator_traits<typename Block::const_iterator>::reference;

  // Construct from a container object, where the initial subrange will
  // represent the whole container.
  explicit BlockAndSubrange(Block block)
      : block_{std::make_shared<Block>(std::move(block))},
        subrange_{0, block_->size()} {}

  // Return a reference to the last element of the currently specified subrange.
  const_reference back() const {
    AD_CORRECTNESS_CHECK(!empty() && subrange_.second <= block_->size());
    return std::as_const(*block_)[subrange_.second - 1];
  }

  bool empty() const { return subrange_.second == subrange_.first; }

  // Return the currently specified subrange as a `std::ranges::subrange`
  // object.
  auto subrange() {
    return std::ranges::subrange{fullBlock().begin() + subrange_.first,
                                 fullBlock().begin() + subrange_.second};
  }

  // The const overload of the `subrange` method (see above).
  auto subrange() const {
    return std::ranges::subrange{fullBlock().begin() + subrange_.first,
                                 fullBlock().begin() + subrange_.second};
  }

  // Get a view that iterates over all the indices that belong to the subrange.
  auto getIndexRange() const {
    return std::views::iota(subrange_.first, subrange_.second);
  }

  Range getIndices() const { return subrange_; }

  const auto& fullBlock() const { return *block_; }
  auto& fullBlock() { return *block_; }

  // Specify the subrange by using two iterators `begin` and `end`. The
  // iterators must be valid iterators that point into the container, this is
  // checked by an assertion. The only legal way to obtain such iterators is to
  // call `subrange()` and then to extract valid iterators from the returned
  // subrange.
  template <typename It>
  void setSubrange(It begin, It end) {
    // We need this function to work with `iterator` as well as
    // `const_iterator`, so we template the actual implementation.
    auto impl = [&begin, &end, this](auto blockBegin, auto blockEnd) {
      auto checkIt = [&blockBegin, &blockEnd](const auto& it) {
        AD_CONTRACT_CHECK(blockBegin <= it && it <= blockEnd);
      };
      checkIt(begin);
      checkIt(end);
      AD_CONTRACT_CHECK(begin <= end);
      subrange_.first = begin - blockBegin;
      subrange_.second = end - blockBegin;
    };
    auto& block = fullBlock();
    if constexpr (requires { begin - block.begin(); }) {
      impl(block.begin(), block.end());
    } else {
      impl(std::as_const(block).begin(), std::as_const(block).end());
    }
  }

  // Overload of `setSubrange` for an actual subrange object.
  template <typename Subrange>
  void setSubrange(const Subrange& subrange) {
    setSubrange(std::ranges::begin(subrange), std::ranges::end(subrange));
  }
};

// A helper struct for the zipper join on blocks algorithm (see below). It
// combines the current iterator, then end iterator, the relevant projection to
// obtain the input to the comparsion, and a buffer for blocks that are
// currently required by the join algorithm for one side of the join.
template <typename Iterator, typename End, typename Projection>
struct JoinSide {
  using CurrentBlocks =
      std::vector<detail::BlockAndSubrange<typename Iterator::value_type>>;
  Iterator it_;
  [[no_unique_address]] const End end_;
  const Projection& projection_;
  CurrentBlocks currentBlocks_{};

  // Type aliases for a single element from a block from the left/right input.
  using value_type = std::ranges::range_value_t<typename Iterator::value_type>;
  // Type alias for the result of the projection.
  using ProjectedEl =
      std::decay_t<std::invoke_result_t<const Projection&, value_type>>;
};

// Deduction guide required by the `makeJoinSide` function.
template <typename It, typename End, typename Projection>
JoinSide(It, End, const Projection&) -> JoinSide<It, End, Projection>;

// Create a `JoinSide` object from a range of `blocks` and a `projection`. Note
// that the `blocks` are stored as a reference, so the caller is responsible for
// keeping them valid until the join is completed.
template <typename Blocks>
auto makeJoinSide(Blocks& blocks, const auto& projection) {
  return JoinSide{std::ranges::begin(blocks), std::ranges::end(blocks),
                  projection};
};

// A concept to identify instantiations of the `JoinSide` template.
template <typename T>
concept IsJoinSide = ad_utility::isInstantiation<T, JoinSide>;

// The class that actually performs the zipper join for blocks without UNDEF.
// See the public `zipperJoinForBlocksWithoutUndef` function below for details.
// The general approach of the algorithm is described in the following. Several
// details of the actual implementation are slightly different, but the
// description still helps to understand the algorithm as well as the used
// terminology.
//
// First one block from each of the inputs is read into a buffer. We then know,
// that we can completely join all elements in these buffers that are less than
// the minimum of the last element in the left buffered block and the right
// buffered block. Consider for example the following two blocks:
// left: [0-3]  right : [1-3]
// We can then safely join all elements that are less than `3` because all
// elements in both inputs which we haven't seen so far are guaranteed to be `>=
// 3`. We call this last element (3 in the example) the `currentEl` in the
// following as well as in the code.
//
// We then remove all processed elements from the buffered blocks, s.t. only the
// entries `>= currentEl` remain in the buffer. (Greater than might happen if
// the last element from the left and right block are not the same). So we are
// left with the following in the buffers: left: [3-3] right: [3-3]

// We then fill our buffer with blocks from left and right until we have either
// reached the end of the input or found an element `> currentEl`. We then know
// that we have all the elements `== currentEl` in the buffers, e.g.
// left: [3-3] [3-3] [3-7]
// right: [3-3] [3-3] [3-3] [3-5]
// We can then add the Cartesian product of all the elements `== currentEl` to
// the result and safely remove these elements from our buffer, leaving us with
// left: [4-7]  right: [4-5]
// Note that the lower bound for the blocks in the example is not necessarily 4
// but anything greater than 3. We also apply the following optimization: It is
// only required to have all the blocks with the `currentEl` in one of the
// buffers (either left or right) at the same time. We can then process the
// blocks from the other side in a streaming fashion. For example, if there are
// 5 blocks that contain the `currentEl` on the left side, but 5 million such
// blocks on the right side, it suffices to have the 5 blocks from the left side
// plus 1 block from the right side in the buffers at the same time.
//
// TODO<joka921> When an element appears in very many blocks on both sides we
// currently have a very high memory consumption. To fix this, one would need to
// have the possibility to revisit blocks that were seen earlier.
//
// After adding the Cartesian product we start a new round with a new
// `currentEl` (5 in this example). New blocks are added to one of the buffers
// if they become empty at one point in the algorithm.
template <IsJoinSide LeftSide, IsJoinSide RightSide, typename LessThan,
          typename CompatibleRowAction>
struct BlockZipperJoinImpl {
  // The left and right inputs of the join
  LeftSide leftSide_;
  RightSide rightSide_;
  // The used comparison.
  const LessThan& lessThan_;
  // The callback that is called for each pair of matching rows.
  CompatibleRowAction& compatibleRowAction_;

  // Type alias for the result of the projection. Elements from the left and
  // right input must be projected to the same type.
  using ProjectedEl = LeftSide::ProjectedEl;
  static_assert(std::same_as<ProjectedEl, typename RightSide::ProjectedEl>);

  // The largest element for which all blocks are currently stored in the buffer
  // and processed.
  std::optional<ProjectedEl> currentMinEl_ = std::nullopt;

  // Create an equality comparison from the `lessThan` predicate.
  bool eq(const auto& el1, const auto& el2) {
    return !lessThan_(el1, el2) && !lessThan_(el2, el1);
  };

  // Recompute the `currentEl`. It is the minimum of the last element in the
  // first block of either of the join sides.
  ProjectedEl getCurrentEl() {
    auto getFirst = [](const auto& side) {
      return side.projection_(side.currentBlocks_.front().back());
    };
    return std::min(getFirst(leftSide_), getFirst(rightSide_), lessThan_);
  };

  // Fill `side.currentBlocks_` with blocks from the range `[side.it_,
  // side.end_)` and advance `side.it_` for each read buffer until all elements
  // <= `currentEl` are added or until three blocks have been added to the
  // targetBuffer. Calling this function requires that all blocks that contain
  // elements `< currentEl` have already been consumed. Returns `true` if all
  // blocks that contain elements <= `currentEl` have been added, and `false` if
  // the function returned because 3 blocks were added without fulfilling the
  // condition.
  bool fillEqualToCurrentEl(auto& side, const auto& currentEl) {
    auto& it = side.it_;
    auto& end = side.end_;
    for (size_t numBlocksRead = 0; it != end && numBlocksRead < 3;
         ++it, ++numBlocksRead) {
      if (std::ranges::empty(*it)) {
        continue;
      }
      if (!eq((*it)[0], currentEl)) {
        AD_CORRECTNESS_CHECK(lessThan_(currentEl, (*it)[0]));
        return true;
      }
      AD_CORRECTNESS_CHECK(std::ranges::is_sorted(*it, lessThan_));
      side.currentBlocks_.emplace_back(std::move(*it));
    }
    return it == end;
  }

  // Fill the buffers in `leftSide_` and rightSide_` until they both contain at
  // least one block and at least one of them contains all the blocks with
  // elements `<= currentEl`. The returned `BlockStatus` reports which of the
  // sides contain all the relevant blocks. Only filling one side is used for
  // the optimization for the Cartesian product described in the documentation.
  enum struct BlockStatus { leftMissing, rightMissing, allFilled };
  BlockStatus fillEqualToCurrentElBothSides(const auto& currentEl) {
    bool allBlocksFromLeft = false;
    bool allBlocksFromRight = false;
    while (!(allBlocksFromLeft || allBlocksFromRight)) {
      allBlocksFromLeft = fillEqualToCurrentEl(leftSide_, currentEl);
      allBlocksFromRight = fillEqualToCurrentEl(rightSide_, currentEl);
    }
    if (!allBlocksFromRight) {
      return BlockStatus::rightMissing;
    } else if (!allBlocksFromLeft) {
      return BlockStatus::leftMissing;
    } else {
      return BlockStatus::allFilled;
    }
  };

  // Remove all elements from `blocks` (either `leftSide_.currentBlocks_` or
  // `rightSide_.currentBlocks`) s.t. only elements `> lastProcessedElement`
  // remain. This effectively removes all blocks completely, except maybe the
  // last one.
  template <typename Blocks, typename ProjectedEl>
  void removeEqualToCurrentEl(Blocks& blocks,
                              ProjectedEl lastProcessedElement) {
    // Erase all but the last block.
    AD_CORRECTNESS_CHECK(!blocks.empty());
    if (blocks.size() > 1 && !blocks.front().empty()) {
      AD_CORRECTNESS_CHECK(!lessThan_(lastProcessedElement,
                                      std::as_const(blocks.front()).back()));
    }
    blocks.erase(blocks.begin(), blocks.end() - 1);

    // Delete the part from the last block that is `<= lastProcessedElement`.
    decltype(auto) remainingBlock = blocks.at(0).subrange();
    auto beginningOfUnjoined = std::ranges::upper_bound(
        remainingBlock, lastProcessedElement, lessThan_);
    blocks.at(0).setSubrange(beginningOfUnjoined, remainingBlock.end());
    if (blocks.at(0).empty()) {
      blocks.clear();
    }
  };

  // For one of the inputs (`leftSide_.currentBlocks_` or
  // `rightSide_.currentBlocks_`) obtain a tuple of the following elements:
  // * A reference to the first full block
  // * The currently active subrange of that block
  // * An iterator pointing to the first element ` >= currentEl` in the block.
  auto getFirstBlock(auto& currentBlocks, const auto& currentEl) {
    AD_CORRECTNESS_CHECK(!currentBlocks.empty());
    const auto& first = currentBlocks.at(0);
    auto it = std::ranges::lower_bound(first.subrange(), currentEl, lessThan_);
    return std::tuple{std::ref(first.fullBlock()), first.subrange(), it};
  };

  // Call `compatibleRowAction` for all pairs of elements in the Cartesian
  // product of the blocks in `blocksLeft` and `blocksRight`.
  template <bool DoOptionalJoin>
  void addAll(const auto& blocksLeft, const auto& blocksRight) {
    if constexpr (DoOptionalJoin) {
      if (std::ranges::all_of(
              blocksRight | std::views::transform(
                                [](const auto& inp) { return inp.subrange(); }),
              std::ranges::empty)) {
        for (const auto& lBlock : blocksLeft) {
          compatibleRowAction_.setOnlyLeftInputForOptionalJoin(
              lBlock.fullBlock());
          for (size_t i : lBlock.getIndexRange()) {
            compatibleRowAction_.addOptionalRow(i);
          }
        }
      }
    }
    // TODO<C++23> use `std::views::cartesian_product`.
    for (const auto& lBlock : blocksLeft) {
      for (const auto& rBlock : blocksRight) {
        compatibleRowAction_.setInput(lBlock.fullBlock(), rBlock.fullBlock());
        for (size_t i : lBlock.getIndexRange()) {
          for (size_t j : rBlock.getIndexRange()) {
            compatibleRowAction_.addRow(i, j);
          }
        }
      }
    }
    compatibleRowAction_.flush();
  };

  // Return a vector of subranges of all elements in `blocks` that are equal to
  // `currentEl`. Effectively, these subranges cover all the blocks completely
  // except maybe the last one, which might contain elements `> currentEl` at
  // the end.
  auto getEqualToCurrentEl(const auto& blocks, const auto& currentEl) {
    auto result = blocks;
    if (result.empty()) {
      return result;
    }
    auto& last = result.back();
    last.setSubrange(
        std::ranges::equal_range(last.subrange(), currentEl, lessThan_));
    return result;
  };

  // Join the first block in `currentBlocksLeft` with the first block in
  // `currentBlocksRight`, but ignore all elements that are `>= currentEl`
  // The fully joined parts of the block are then removed from
  // `currentBlocksLeft/Right`, as they are not needed anymore.
  template <bool DoOptionalJoin>
  void joinAndRemoveLessThanCurrentEl(auto& currentBlocksLeft,
                                      auto& currentBlocksRight,
                                      const auto& currentEl) {
    // Get the first blocks.
    auto [fullBlockLeft, subrangeLeft, currentElItL] =
        getFirstBlock(currentBlocksLeft, currentEl);
    auto [fullBlockRight, subrangeRight, currentElItR] =
        getFirstBlock(currentBlocksRight, currentEl);

    compatibleRowAction_.setInput(fullBlockLeft.get(), fullBlockRight.get());
    auto addRowIndex = [begL = fullBlockLeft.get().begin(),
                        begR = fullBlockRight.get().begin(),
                        this](auto itFromL, auto itFromR) {
      compatibleRowAction_.addRow(itFromL - begL, itFromR - begR);
    };

    auto addNotFoundRowIndex = [&]() {
      if constexpr (DoOptionalJoin) {
        return [begL = fullBlockLeft.get().begin(), this](auto itFromL) {
          compatibleRowAction_.addOptionalRow(itFromL - begL);
        };

      } else {
        return ad_utility::noop;
      }
    }();
    [[maybe_unused]] auto res = zipperJoinWithUndef(
        std::ranges::subrange{subrangeLeft.begin(), currentElItL},
        std::ranges::subrange{subrangeRight.begin(), currentElItR}, lessThan_,
        addRowIndex, noop, noop, addNotFoundRowIndex);
    compatibleRowAction_.flush();

    // Remove the joined elements.
    currentBlocksLeft.at(0).setSubrange(currentElItL, subrangeLeft.end());
    currentBlocksRight.at(0).setSubrange(currentElItR, subrangeRight.end());
  };

  // If the `targetBuffer` is empty, read the next nonempty block from `[it,
  // end)` if there is one.
  void fillWithAtLeastOne(auto& side) {
    auto& targetBuffer = side.currentBlocks_;
    auto& it = side.it_;
    const auto& end = side.end_;
    while (targetBuffer.empty() && it != end) {
      auto& el = *it;
      if (!el.empty()) {
        AD_CORRECTNESS_CHECK(std::ranges::is_sorted(el, lessThan_));
        targetBuffer.emplace_back(std::move(el));
      }
      ++it;
    }
  };

  // Fill both buffers (left and right) until they contain at least one block.
  // Then recompute the `currentEl()` and keep on filling the buffers until at
  // least one of them contains all elements `<= currentEl`. The returned
  // `BlockStatus` tells us, which of the blocks contain all elements `<=
  // currentEl`.
  BlockStatus fillBuffer() {
    AD_CORRECTNESS_CHECK(leftSide_.currentBlocks_.size() <= 1);
    AD_CORRECTNESS_CHECK(rightSide_.currentBlocks_.size() <= 1);

    fillWithAtLeastOne(leftSide_);
    fillWithAtLeastOne(rightSide_);

    if (leftSide_.currentBlocks_.empty() || rightSide_.currentBlocks_.empty()) {
      // One of the inputs was exhausted, we are done.
      // If the left side is not empty and this is an optional join, then we
      // will add the remaining elements from the `leftSide_` later in the
      // `fillWithAllFromLeft` function.
      return BlockStatus::allFilled;
    }

    // Add the remaining blocks such that condition 3 from above is fulfilled.
    auto blockStatus = fillEqualToCurrentElBothSides(getCurrentEl());
    currentMinEl_ = getCurrentEl();
    return blockStatus;
  }

  // Combine the above functionality and perform one round of joining.
  // Has to be called alternately with `fillBuffer`.
  template <bool DoOptionalJoin, typename ProjectedEl>
  void joinBuffers(BlockStatus& blockStatus) {
    auto& currentBlocksLeft = leftSide_.currentBlocks_;
    auto& currentBlocksRight = rightSide_.currentBlocks_;
    joinAndRemoveLessThanCurrentEl<DoOptionalJoin>(
        currentBlocksLeft, currentBlocksRight, getCurrentEl());

    // TODO<joka921> This should still be the same.
    ProjectedEl currentEl = getCurrentEl();
    // At this point the `currentBlocksLeft/Right` only consist of elements `>=
    // currentEl`. We now obtain a view on the elements `== currentEl` which are
    // needed for the next step (the Cartesian product). In the last block,
    // there might be elements `> currentEl` which will be processed later.
    auto equalToCurrentElLeft =
        getEqualToCurrentEl(currentBlocksLeft, currentEl);
    auto equalToCurrentElRight =
        getEqualToCurrentEl(currentBlocksRight, currentEl);

    auto getNextBlocks = [&currentEl, self = this, &blockStatus](auto& target,
                                                                 auto& side) {
      self->removeEqualToCurrentEl(side.currentBlocks_, currentEl);
      bool allBlocksWereFilled = self->fillEqualToCurrentEl(side, currentEl);
      if (side.currentBlocks_.empty()) {
        AD_CORRECTNESS_CHECK(allBlocksWereFilled);
      }
      target = self->getEqualToCurrentEl(side.currentBlocks_, currentEl);
      if (allBlocksWereFilled) {
        blockStatus = BlockStatus::allFilled;
      }
    };
    // We are only guaranteed to have all relevant blocks from one side, so we
    // also need to pass through the remaining blocks from the other side.
    while (!equalToCurrentElLeft.empty() && !equalToCurrentElRight.empty()) {
      addAll<DoOptionalJoin>(equalToCurrentElLeft, equalToCurrentElRight);
      switch (blockStatus) {
        case BlockStatus::allFilled:
          removeEqualToCurrentEl(currentBlocksLeft, currentEl);
          removeEqualToCurrentEl(currentBlocksRight, currentEl);
          return;
        case BlockStatus::rightMissing:
          getNextBlocks(equalToCurrentElRight, rightSide_);
          continue;
        case BlockStatus::leftMissing:
          getNextBlocks(equalToCurrentElLeft, leftSide_);
          continue;
        default:
          AD_FAIL();
      }
    }
  }

  // Needed for the optional join: Call `addOptionalRow` for all remaining
  // elements from the left input after the right input has been completely
  // processed.
  auto fillWithAllFromLeft() {
    auto& currentBlocksLeft = leftSide_.currentBlocks_;
    for (auto& block : currentBlocksLeft) {
      compatibleRowAction_.setOnlyLeftInputForOptionalJoin(block.fullBlock());
      for (size_t idx : block.getIndexRange()) {
        compatibleRowAction_.addOptionalRow(idx);
      }
    }

    auto& it1 = leftSide_.it_;
    const auto& end1 = leftSide_.end_;
    while (it1 != end1) {
      auto& block = *it1;
      compatibleRowAction_.setOnlyLeftInputForOptionalJoin(block);
      for (size_t idx : ad_utility::integerRange(block.size())) {
        compatibleRowAction_.addOptionalRow(idx);
      }
      // We need to manually flush, because the `block` is captured by reference
      // and might not be valid anymore after increasing the iterator.
      compatibleRowAction_.flush();
      ++it1;
    }
    compatibleRowAction_.flush();
  }

  // The actual join routine that combines all the previous functions.
  template <bool DoOptionalJoin>
  void runJoin() {
    while (true) {
      BlockStatus blockStatus = fillBuffer();
      if (leftSide_.currentBlocks_.empty() ||
          rightSide_.currentBlocks_.empty()) {
        if constexpr (DoOptionalJoin) {
          fillWithAllFromLeft();
        }
        return;
      }
      joinBuffers<DoOptionalJoin, ProjectedEl>(blockStatus);
    }
  }
};

// Deduction guide for the above struct.
template <typename LHS, typename RHS, typename LessThan,
          typename CompatibleRowAction>
BlockZipperJoinImpl(LHS&, RHS&, const LessThan&, CompatibleRowAction&)
    -> BlockZipperJoinImpl<LHS, RHS, LessThan, CompatibleRowAction>;

}  // namespace detail

/**
 * @brief Perform a zipper/merge join between two sorted inputs that are given
 * as blocks of inputs, e.g. `std::vector<std::vector<int>>` or
 * `ad_utility::generator<std::vector<int>>`. The blocks can be specified via an
 * input range (e.g. a generator), but each single block must be a random access
 * range (e.g. vector). The blocks will be moved from. The space complexity is
 * `O(size of result)`. The result is only correct if none of the inputs
 * contains UNDEF values.
 * @param leftBlocks The left input range to the join algorithm. We use this for
 * blocks of rows of IDs (e.g.`std::vector<IdTable>` or
 * `ad_utility::generator<IdTable>`). Each block will be moved from.
 * @param rightBlocks The right input range to the join algorithm. Each block
 * will be moved from.
 * @param lessThan This function is called with one element from one of the
 * blocks of `leftBlocks` and `rightBlocks` each and must return `true` if the
 * first argument comes before the second one. The concatenation of all blocks
 * in `leftBlocks` must be sorted according to this function. The same
 * requirement holds for `rightBlocks`.
 * @param compatibleRowAction Needs to have three member functions:
 * `setInput(leftBlock, rightBlock)`, `addRow(size_t, size_t)`, and `flush()`.
 * When the `i`-th element of a `leftBlock` and the `j`-th element `rightBlock`
 * match (because they compare equal wrt the `lessThan` relation), then first
 * `setInput(leftBlock, rightBlock)` then `addRow(i, j)`, and finally `flush()`
 * is called. Of course `setInput` and `flush()` are only called once if there
 * are several matching pairs of elements from the same pair of blocks. The
 * calls to `addRow` will then all be between the calls to `setInput` and
 * `flush`.
 */
template <typename LeftBlocks, typename RightBlocks, typename LessThan,
          typename LeftProjection = std::identity,
          typename RightProjection = std::identity,
          typename DoOptionalJoinTag = std::false_type>
void zipperJoinForBlocksWithoutUndef(LeftBlocks&& leftBlocks,
                                     RightBlocks&& rightBlocks,
                                     const LessThan& lessThan,
                                     auto& compatibleRowAction,
                                     LeftProjection leftProjection = {},
                                     RightProjection rightProjection = {},
                                     DoOptionalJoinTag = {}) {
  static constexpr bool DoOptionalJoin = DoOptionalJoinTag::value;

  auto leftSide = detail::makeJoinSide(leftBlocks, leftProjection);
  auto rightSide = detail::makeJoinSide(rightBlocks, rightProjection);

  detail::BlockZipperJoinImpl impl{leftSide, rightSide, lessThan,
                                   compatibleRowAction};
  impl.template runJoin<DoOptionalJoin>();
}

}  // namespace ad_utility
