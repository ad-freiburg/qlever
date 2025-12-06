// Copyright 2024 The QLever Authors, in particular:
//
// 2024        Julian Mundhahs <mundhahj@tf.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "index/DeltaTriples.h"
#include "index/LocatedTriples.h"
#include "util/GTestHelpers.h"

#ifndef QLEVER_TEST_DELTATRIPLESTESTHELPERS_H
#define QLEVER_TEST_DELTATRIPLESTESTHELPERS_H

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
// A matcher that checks `numInserted()`, `numDeleted()` and the derived
// `getCounts()` of a `DeltaTriples` and `numTriples()` for all
// `LocatedTriplesPerBlock` of the `DeltaTriples`.
inline auto NumTriples =
    [](int64_t inserted, int64_t deleted, size_t inAllPermutations,
       int64_t internalInserted = 0,
       int64_t internalDeleted = 0) -> testing::Matcher<const DeltaTriples&> {
  return testing::AllOf(
      AD_PROPERTY(DeltaTriples, numInserted, testing::Eq(inserted)),
      AD_PROPERTY(DeltaTriples, numDeleted, testing::Eq(deleted)),
      AD_PROPERTY(DeltaTriples, numInternalInserted,
                  testing::Eq(internalInserted)),
      AD_PROPERTY(DeltaTriples, numInternalDeleted,
                  testing::Eq(internalDeleted)),
      AD_PROPERTY(DeltaTriples, getCounts,
                  testing::Eq(DeltaTriplesCount{inserted, deleted})),
      NumTriplesInAllPermutations(inAllPermutations));
};

}  // namespace deltaTriplesTestHelpers

#endif  // QLEVER_TEST_DELTATRIPLESTESTHELPERS_H
