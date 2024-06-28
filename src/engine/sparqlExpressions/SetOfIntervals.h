// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <limits>
#include <utility>
#include <vector>

#include "util/Exception.h"

namespace ad_utility {

/// A vector of pairs of <size_t, size_t> with the following semantics: It
/// represents the union of the intervals [first, second) of the individual
/// pairs. The intervals have to be pairwise disjoint and nonempty. They
/// also have to be sorted in ascending order.
struct SetOfIntervals {
  using Vec = std::vector<std::pair<size_t, size_t>>;
  Vec _intervals;
  constexpr static size_t upperBound = std::numeric_limits<size_t>::max();

  // _________________________________________________________________________
  bool operator==(const SetOfIntervals&) const = default;

  /// Sort the intervals in ascending order and assert that they are indeed
  /// disjoint and nonempty.
  static SetOfIntervals SortAndCheckDisjointAndNonempty(SetOfIntervals input);

  /// Assert that the set is sorted, and simplify it by merging adjacent
  /// intervals.
  static SetOfIntervals CheckSortedAndDisjointAndSimplify(
      const SetOfIntervals& input);

  /// Compute the intersection of two sets of intervals;
  struct Intersection {
    SetOfIntervals operator()(SetOfIntervals A, SetOfIntervals B) const;
  };

  /// Compute the union of two sets of intervals.
  struct Union {
    SetOfIntervals operator()(SetOfIntervals A, SetOfIntervals B) const;
  };

  /// Compute the complement of a set of intervals.
  struct Complement {
    SetOfIntervals operator()(SetOfIntervals s) const;
  };

  // Write `targetSize` many bools to the iterator. The i-th bool is true if
  // and only if `i` is contained in the set of intervals. `targetSize` has to
  // be >= the right end (not included) of the rightmost interval.
  template <typename OutputIterator>
  static void toBitVector(const SetOfIntervals& s, size_t targetSize,
                          OutputIterator it) {
    size_t previousEnd = 0;
    for (const auto& [begin, end] : s._intervals) {
      AD_CONTRACT_CHECK(end <= targetSize);
      auto spaceUntilInterval = begin - previousEnd;
      std::fill(it, it + spaceUntilInterval, false);
      it += spaceUntilInterval;

      auto sizeOfInterval = end - begin;
      std::fill(it, it + sizeOfInterval, true);
      it += sizeOfInterval;

      previousEnd = end;
    }
  }

  // Transform a SetOfIntervals to a std::vector<bool> of size `targetSize`
  // where the element at index i is true if and only if i is contained in the
  // set. `targetSize` has to be >= the right end (not included) of the
  // rightmost interval.
  // __________________________________________________________________________
  inline static std::vector<bool> toBitVector(const SetOfIntervals& a,
                                              size_t targetSize) {
    std::vector<bool> result(targetSize, false);
    toBitVector(a, targetSize, std::ranges::begin(result));
    return result;
  }
};

}  // namespace ad_utility
