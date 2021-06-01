// Copyright 2021, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Johannes Kalmbach<joka921> (johannes.kalmbach@gmail.com)

#pragma once

#include <utility>
#include <vector>

#include "../util/Exception.h"
#include "../util/TypeTraits.h"

namespace setOfIntervals {

/// A vector of pairs of size_t, size_t with the following semantic: It
/// represents the Union of the intervals [first, second) of the individual
/// pairs. The intervals have to be pairwise disjoint and nonempty;
using Set = std::vector<std::pair<size_t, size_t>>;

/// Sort the intervals in ascending orders and assert that they are indeed
/// disjoint and nonempty
Set SortAndCheckInvariants(Set input);

/// Assert that the set is sorted, and simplyfy it by merging adjacent
/// intervals;
Set CheckSortedAndSimplify(const Set& input);

/// Calculates the Intersection of two `Set`s
struct Intersection {
  Set operator()(Set rangesA, Set rangesB) const;
};

// Calculate the Union of two sets
struct Union {
  Set operator()(Set rangesA, Set rangesB) const;
};

// Transform a Set into a std::vector<bool> of size `targetSize` where each
// index is true if and only if it is contained in the set. `targetSize` has to
// be >= to the end entry of the highest interval in the `Set`
std::vector<bool> expandSet(const Set& a, size_t targetSize);

}  // namespace setOfIntervals
