// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)

#ifndef QLEVER_ALGORITHM_H
#define QLEVER_ALGORITHM_H

#include <numeric>
#include <string>
#include <string_view>
#include <utility>

#include "global/Id.h"
#include "util/Forward.h"
#include "util/Generator.h"
#include "util/Iterators.h"
#include "util/TypeTraits.h"

namespace ad_utility {

/**
 * Checks whether an element is contained in a container.
 *
 * @param container Container& Elements to be searched
 * @param element T Element to search for
 * @return bool
 */
template <typename Container, typename T>
inline bool contains(Container&& container, const T& element) {
  // Overload for types like std::string that have a `find` member function
  if constexpr (ad_utility::isSimilar<Container, std::string> ||
                ad_utility::isSimilar<Container, std::string_view>) {
    return container.find(element) != container.npos;
  } else {
    return std::ranges::find(container.begin(), container.end(), element) !=
           container.end();
  }
}

/**
 * Checks whether an element in the container satisfies the predicate.
 *
 * @param container Container& Elements to be searched
 * @param predicate Predicate Predicate to check
 * @return bool
 */
template <typename Container, typename Predicate>
bool contains_if(const Container& container, const Predicate& predicate) {
  return std::find_if(container.begin(), container.end(), predicate) !=
         container.end();
}

/**
 * Appends the second vector to the first one.
 *
 * @param destination Vector& to which to append
 * @param source Vector&& to append
 */
template <typename T, ad_utility::SimilarTo<std::vector<T>> U>
void appendVector(std::vector<T>& destination, U&& source) {
  destination.insert(destination.end(),
                     ad_utility::makeForwardingIterator<U>(source.begin()),
                     ad_utility::makeForwardingIterator<U>(source.end()));
}

/**
 * Applies the UnaryOperation to all elements of the range and return a new
 * vector which contains the results.
 */
template <typename Range, typename F>
auto transform(Range&& input, F unaryOp) {
  using Output = std::decay_t<decltype(unaryOp(
      *ad_utility::makeForwardingIterator<Range>(input.begin())))>;
  std::vector<Output> out;
  out.reserve(input.size());
  std::transform(ad_utility::makeForwardingIterator<Range>(input.begin()),
                 ad_utility::makeForwardingIterator<Range>(input.end()),
                 std::back_inserter(out), unaryOp);
  return out;
}

/**
 * Flatten a vector<vector<T>> into a vector<T>. Currently requires an rvalue
 * (temporary or `std::move`d value) as an input.
 */
template <typename T>
std::vector<T> flatten(std::vector<std::vector<T>>&& input) {
  std::vector<T> out;
  // Reserve the total required space. It is the sum of all the vectors lengths.
  out.reserve(std::accumulate(
      input.begin(), input.end(), 0,
      [](size_t i, const std::vector<T>& elem) { return i + elem.size(); }));
  for (auto& sub : input) {
    // As the input is an rvalue, it is save to always move.
    appendVector(out, std::move(sub));
  }
  return out;
}

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
    result.push_back(std::equal_range(begin, end, rowCopy, std::ranges::lexicographical_compare));
  }
  return result;
}

[[maybe_unused]] static inline auto noop = [](auto&&...){};
// TODO<joka921> Comment, cleanup, move into header.
template<typename ElFromFirstNotFoundAction = decltype(noop)>
void zipperJoinWithUndef(auto&& range1, auto&& range2, auto&& lessThan,
                         auto&& lessThanReversed, auto&& compatibleRowAction,
                         auto&& findSmallerUndefRangesLeft,
                         auto&& findSmallerUndefRangesRight, ElFromFirstNotFoundAction elFromFirstNotFoundAction = {}) {
  auto it1 = range1.begin();
  auto end1 = range1.end();
  auto it2 = range2.begin();
  auto end2 = range2.end();
  while (it1 < end1 && it2 < end2) {
    while (lessThan(*it1, *it2)) {
      auto smallerUndefRanges =
          findSmallerUndefRangesLeft(*it1, range2.begin(), it2);
      bool compatibleWasFound = false;
      for (auto [begin, end] : smallerUndefRanges) {
        for (; begin != end; ++begin) {
          if (lessThanReversed(*begin, *it1)) {
            compatibleWasFound = true;
            compatibleRowAction(*it1, *begin);
          }
        }
      }
      if (!compatibleWasFound) {
        elFromFirstNotFoundAction(*it1);
      }
      ++it1;
    }
    if (it1 > end1) {
      return;
    }
    while (lessThanReversed(*it2, *it1)) {
      auto smallerUndefRanges =
          findSmallerUndefRangesRight(*it2, range1.begin(), it1);
      for (auto [begin, end] : smallerUndefRanges) {
        for (; begin != end; ++begin) {
          if (lessThan(*begin, *it2)) {
            compatibleRowAction(*begin, *it2);
          }
        }
      }
      ++it2;
    }
    if (it2 > end2) {
      return;
    }
    auto eq = [&lessThan, &lessThanReversed](const auto& el1, const auto& el2) {
      return !lessThan(el1, el2) && !lessThanReversed(el2, el1);
    };
    auto endSame1 = std::find_if_not(
        it1, end1, [&](const auto& row) { return eq(row, *it2); });
    auto endSame2 = std::find_if_not(
        it2, end2, [&](const auto& row) { return eq(*it1, row); });

    if (endSame1 == it1) {
      elFromFirstNotFoundAction(*it1);
    }
    for (; it1 != endSame1; ++it1) {
      for (auto innerIt2 = it2; innerIt2 != endSame2; ++innerIt2) {
        compatibleRowAction(*it1, *innerIt2);
      }
    }
    it1 = endSame1;
    it2 = endSame2;
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

#endif  // QLEVER_ALGORITHM_H
