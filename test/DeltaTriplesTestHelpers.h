// Copyright 2024, University of Freiburg
//  Chair of Algorithms and Data Structures.
//  Authors:
//    2024 Julian Mundhahs <mundhahj@tf.uni-freiburg.de>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "index/DeltaTriples.h"
#include "index/LocatedTriples.h"
#include "util/GTestHelpers.h"

#pragma once

namespace deltaTriplesTestHelpers {

// A matcher that applies `InnerMatcher` to all `LocatedTriplesPerBlock` of a
// `DeltaTriples`.
inline auto InAllPermutations =
    [](testing::Matcher<const LocatedTriplesPerBlock&> InnerMatcher)
    -> testing::Matcher<const DeltaTriples&> {
  return testing::AllOfArray(ad_utility::transform(
      Permutation::ALL, [&InnerMatcher](const Permutation::Enum& perm) {
        return testing::ResultOf(
            absl::StrCat(".getLocatedTriplesPerBlock(",
                         Permutation::toString(perm), ")"),
            [perm](const DeltaTriples& deltaTriples) {
              return deltaTriples.getLocatedTriplesForPermutation(perm);
            },
            InnerMatcher);
      }));
};
// A matcher that checks `numTriples()` for all `LocatedTriplesPerBlock` of a
// `DeltaTriples`.
inline auto NumTriplesInAllPermutations =
    [](size_t expectedNumTriples) -> testing::Matcher<const DeltaTriples&> {
  return InAllPermutations(AD_PROPERTY(LocatedTriplesPerBlock, numTriples,
                                       testing::Eq(expectedNumTriples)));
};
// A matcher that checks `numInserted()` and `numDeleted()` of a `DeltaTriples`
// and `numTriples()` for all `LocatedTriplesPerBlock` of the `DeltaTriples`.
inline auto NumTriples =
    [](size_t inserted, size_t deleted,
       size_t inAllPermutations) -> testing::Matcher<const DeltaTriples&> {
  return testing::AllOf(
      AD_PROPERTY(DeltaTriples, numInserted, testing::Eq(inserted)),
      AD_PROPERTY(DeltaTriples, numDeleted, testing::Eq(deleted)),
      NumTriplesInAllPermutations(inAllPermutations));
};

}  // namespace deltaTriplesTestHelpers
