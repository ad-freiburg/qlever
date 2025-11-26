// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "./SpatialJoinPrefilterTestHelpers.h"

// _____________________________________________________________________________
namespace {

using namespace SpatialJoinPrefilterTestHelpers;
using enum SpatialJoinType;

// Each of the following tests creates a `QueryExecutionContext` on a
// `GeoVocabulary` which holds various carefully selected literals. It
// then performs a spatial join and examines the result as well as the
// prefiltering during the geometry parsing.

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterNoIntersections) {
  // Case 1: No intersections
  // Left: Three geometries in Germany (3x Freiburg)
  // Right: Three geometries outside of Germany (London, Paris, New York)
  auto kg = buildLibSJTestDataset();
  auto qec = buildQec(kg, true);
  auto [vMap, nMap] = resolveValIdTable(qec, 6);

  // With prefilter: No results but one entire side gets filtered out by
  // bounding box
  SweeperTestResult testResult;
  runParsingAndSweeper(qec, "de", "other", {INTERSECTS}, testResult, true);
  checkSweeperTestResult(vMap, testResult,
                         {{}, boundingBoxGermanPlaces, {}, 3, 3, 3, 0},
                         INTERSECTS, true);

  // Without prefilter: No results but everything has to be checked
  SweeperTestResult testResultNoFilter;
  runParsingAndSweeper(qec, "de", "other", {INTERSECTS}, testResultNoFilter,
                       false);
  checkSweeperTestResult(
      vMap, testResultNoFilter,
      {{}, boundingBoxGermanPlaces, boundingBoxOtherPlaces, 6, 0, 3, 3},
      INTERSECTS, true);
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterIntersectsCoversAndNonIntersects) {
  // Case 2: Intersections, coverage and non-intersection
  // Left: University Freiburg, Campus Faculty of Engineering
  // Right: Three geometries in Freiburg: Road through the campus (intersection,
  // but not contained), main building on campus (contained) and Freiburg
  // Minster (no intersection)
  auto kg = buildLibSJTestDataset(false, true, true);
  auto qec = buildQec(kg, true);

  auto [vMap, nMap] = resolveValIdTable(qec, 7);

  auto vIdCampus = getValId(nMap, "uni-separate");
  auto vIdGkAllee = getValId(nMap, "gk-allee");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdMinster = getValId(nMap, "minster");

  // Intersects: Campus intersects road and main building
  SweeperTestResult testResultIntersects;
  runParsingAndSweeper(qec, "uni-separate", "de", {INTERSECTS},
                       testResultIntersects, true);
  checkSweeperTestResult(vMap, testResultIntersects,
                         {{{INTERSECTS, vIdCampus, vIdGkAllee, 0},
                           {INTERSECTS, vIdCampus, vIdUni, 0}},
                          {},
                          {},
                          3,
                          1,
                          1,
                          2},
                         INTERSECTS);

  // Contains: Campus contains main building
  SweeperTestResult testResultContains;
  runParsingAndSweeper(qec, "uni-separate", "de", {CONTAINS},
                       testResultContains, true);
  checkSweeperTestResult(vMap, testResultContains,
                         {{
                              {CONTAINS, vIdCampus, vIdUni, 0},
                          },
                          {},
                          {},
                          3,
                          1,
                          1,
                          2},
                         CONTAINS);

  // Within distance 5km: Minster satisfies this, s.t. all three geometries
  // from the right are expected to be returned.
  SweeperTestResult testResultWithinDist;
  runParsingAndSweeper(qec, "uni-separate", "de", {WITHIN_DIST, 5000},
                       testResultWithinDist, true);
  checkSweeperTestResult(vMap, testResultWithinDist,
                         {{{WITHIN_DIST, vIdCampus, vIdGkAllee, 0},
                           {WITHIN_DIST, vIdCampus, vIdUni, 0},
                           {WITHIN_DIST, vIdCampus, vIdMinster, 2225.01}},
                          {},
                          {},
                          4,
                          0,
                          1,
                          3},
                         WITHIN_DIST);
}

TEST(SpatialJoinTest, BoundingBoxPrefilterLargeContainsNotContains) {
  // Case 3: Large bounding box which contains half of the geometries
  // Left: Approximate boundary of Germany
  // Right: All other test geometries (3x in Freiburg, 1x in London, Paris and
  // New York)
  auto kg = buildLibSJTestDataset(true, false, false);
  auto qec = buildQec(kg, true);

  auto [vMap, nMap] = resolveValIdTable(qec, 7);

  auto vIdGermany = getValId(nMap, "approx-de");
  auto vIdMinster = getValId(nMap, "minster");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdGkAllee = getValId(nMap, "gk-allee");
  auto vIdLondon = getValId(nMap, "london");
  auto vIdParis = getValId(nMap, "eiffel");

  // Intersects with prefiltering: Three geometries in Germany intersect, the
  // other three don't and can be excluded by prefiltering
  SweeperTestResult testResultIntersects;
  runParsingAndSweeper(qec, "approx-de", "other", {INTERSECTS},
                       testResultIntersects, true);
  const SweeperResultWithIds expectedResultIntersects{
      {INTERSECTS, vIdGermany, vIdMinster, 0},
      {INTERSECTS, vIdGermany, vIdUni, 0},
      {INTERSECTS, vIdGermany, vIdGkAllee, 0}};
  checkSweeperTestResult(vMap, testResultIntersects,
                         {expectedResultIntersects, boundingBoxGermany,
                          boundingBoxGermanPlaces, 4, 3, 1, 3},
                         INTERSECTS, true);

  // Intersects without prefiltering: Same result but all geometries are checked
  SweeperTestResult testResultNoFilter;
  runParsingAndSweeper(qec, "approx-de", "other", {INTERSECTS},
                       testResultNoFilter, false);
  checkSweeperTestResult(vMap, testResultNoFilter,
                         {expectedResultIntersects, boundingBoxGermany,
                          boundingBoxAllPlaces, 7, 0, 1, 6},
                         INTERSECTS, true);

  // Within distance of 1 000 km: London and Paris are outside of the bounding
  // box of the left side (Germany) but within the distance range, New York is
  // outside
  SweeperTestResult testResultWithinDist;
  runParsingAndSweeper(qec, "approx-de", "other", {WITHIN_DIST, 1'000'000},
                       testResultWithinDist, true);
  checkSweeperTestResult(vMap, testResultWithinDist,
                         {{{WITHIN_DIST, vIdGermany, vIdUni, 0},
                           {WITHIN_DIST, vIdGermany, vIdMinster, 0},
                           {WITHIN_DIST, vIdGermany, vIdGkAllee, 0},
                           {WITHIN_DIST, vIdGermany, vIdLondon, 426521.1497},
                           {WITHIN_DIST, vIdGermany, vIdParis, 314975.6311}},
                          {},
                          {},
                          6,
                          1,
                          1,
                          5},
                         WITHIN_DIST);
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterDeactivatedTooLargeBox) {
  // Case 4: Very large bounding box, such that prefiltering is deactivated
  // automatically because it will likely not provide a performance gain
  // Left + Right: All geometries in Germany, France, UK, USA and South Africa
  auto kg = buildLibSJTestDataset(false, false, true, false, true);
  auto qec = buildQec(kg, true);
  auto [vMap, nMap] = resolveValIdTable(qec, 8);

  auto vIdUniSep = getValId(nMap, "uni-separate");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdGkAllee = getValId(nMap, "gk-allee");

  {
    auto cleanUp = setRuntimeParameterForTest<
        &RuntimeParameters::spatialJoinPrefilterMaxSize_>(2'500);

    // Intersects with prefiltering, but prefiltering is not used due to too
    // large bounding box
    SweeperTestResult testResult;
    runParsingAndSweeper(qec, "other", "uni-separate", {INTERSECTS}, testResult,
                         true, true);
    checkSweeperTestResult(vMap, testResult,
                           {{{INTERSECTS, vIdUni, vIdUniSep, 0},
                             {INTERSECTS, vIdGkAllee, vIdUniSep, 0}},
                            boundingBoxVeryLarge,
                            boundingBoxUniSeparate,
                            8,
                            0,
                            7,
                            1},
                           INTERSECTS, true);
  }

  // Update runtime parameter for second test
  double bbSize = util::geo::area(boundingBoxVeryLarge);
  EXPECT_GT(bbSize, 2'500);
  EXPECT_LT(bbSize, 10'000);

  {
    auto cleanUp = setRuntimeParameterForTest<
        &RuntimeParameters::spatialJoinPrefilterMaxSize_>(10'000);

    // Using the custom max size of the prefilter box, prefiltering should now
    // be used again.
    SweeperTestResult testResultCustomMax;
    runParsingAndSweeper(qec, "other", "uni-separate", {INTERSECTS},
                         testResultCustomMax, true, false);
    checkSweeperTestResult(vMap, testResultCustomMax,
                           {{{INTERSECTS, vIdUni, vIdUniSep, 0},
                             {INTERSECTS, vIdGkAllee, vIdUniSep, 0}},
                            boundingBoxVeryLarge,
                            boundingBoxUniSeparate,
                            8,
                            0,
                            7,
                            1},
                           INTERSECTS, true);
  }
}

// _____________________________________________________________________________
TEST(SpatialJoinTest, BoundingBoxPrefilterRegularImplementation) {
  // Case 5: Test that the regular implementation `LibspatialjoinAlgorithm()`
  // instead of the test mock version calls the parsing and prefiltering
  // correctly
  // Left: All other test geometries (3x in Freiburg, 1x in London, Paris and
  // New York)
  // Right: Approximate boundary of Germany
  auto kg = buildLibSJTestDataset(true, false, false);
  auto qec = buildQec(kg, true);

  auto [vMap, nMap] = resolveValIdTable(qec, 7);
  auto vIdGermany = getValId(nMap, "approx-de");
  auto vIdUni = getValId(nMap, "uni");
  auto vIdGkAllee = getValId(nMap, "gk-allee");
  auto vIdMinster = getValId(nMap, "minster");

  // Within search: Geometry inside of Germany
  SweeperTestResult testResultRegularImpl;
  runParsingAndSweeper(qec, "other", "approx-de", {WITHIN},
                       testResultRegularImpl, true, false, true);
  // Here we can only check the results and the number of geometries skipped by
  // prefilter, because we are not using the mock algorithm which captures the
  // other information.
  checkSweeperTestResult(vMap, testResultRegularImpl,
                         {{
                              {WITHIN, vIdUni, vIdGermany, 0},
                              {WITHIN, vIdGkAllee, vIdGermany, 0},
                              {WITHIN, vIdMinster, vIdGermany, 0},
                          },
                          {},
                          {},
                          0,
                          3,
                          0,
                          0});

  // One child is an empty index scan
  SweeperTestResult testResultEmpty;
  runParsingAndSweeper(qec, "does-not-exist", "approx-de", {INTERSECTS},
                       testResultEmpty, true, false, true);
  checkSweeperTestResult(vMap, testResultEmpty, {{}, {}, {}, 0, 0, 0, 0});
}

// Test for other utility functions related to geometry prefiltering

// _____________________________________________________________________________
TEST(SpatialJoinTest, prefilterGeoByBoundingBox) {
  auto kg = buildLibSJTestDataset(true, false, false, true);
  auto qec = buildQec(kg, true);
  const auto& index = qec->getIndex();

  auto [vMap, nMap] = resolveValIdTable(qec, 8);

  auto idxUni = getValId(nMap, "uni").getVocabIndex();
  auto idxLondon = getValId(nMap, "london").getVocabIndex();
  auto idxNewYork = getValId(nMap, "lib").getVocabIndex();
  auto idxInvalid = getValId(nMap, "invalid").getVocabIndex();

  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxUni));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxLondon));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxNewYork));

  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxUni));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxLondon));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxNewYork));

  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxOtherPlaces, index, idxUni));

  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxUniAndLondon, index, idxInvalid));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxGermany, index, idxInvalid));
  EXPECT_TRUE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      boundingBoxOtherPlaces, index, idxInvalid));

  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(std::nullopt,
                                                                index, idxUni));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      std::nullopt, index, idxLondon));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      std::nullopt, index, idxNewYork));
  EXPECT_FALSE(SpatialJoinAlgorithms::prefilterGeoByBoundingBox(
      std::nullopt, index, idxInvalid));
}

}  // namespace
