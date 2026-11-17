// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_TEST_ENGINE_SPATIALJOINCACHEDINDEXTESTHELPERS_H
#define QLEVER_TEST_ENGINE_SPATIALJOINCACHEDINDEXTESTHELPERS_H

#include <gmock/gmock.h>

#include "../util/GTestHelpers.h"
#include "engine/SpatialJoinCachedIndex.h"

namespace spatialJoinCachedIndexTestHelpers {

// Return a matcher for a `SpatialJoinCachedIndex` with the given statistics.
inline auto hasStats(size_t numSegments, size_t numShapes, size_t numLiveShapes,
                     size_t numRows) {
  using ::testing::Eq;
  using Index = SpatialJoinCachedIndex;
  return ::testing::AllOf(AD_PROPERTY(Index, numSegments, Eq(numSegments)),
                          AD_PROPERTY(Index, numShapes, Eq(numShapes)),
                          AD_PROPERTY(Index, numLiveShapes, Eq(numLiveShapes)),
                          AD_PROPERTY(Index, numRows, Eq(numRows)));
}

}  // namespace spatialJoinCachedIndexTestHelpers

#endif  // QLEVER_TEST_ENGINE_SPATIALJOINCACHEDINDEXTESTHELPERS_H
