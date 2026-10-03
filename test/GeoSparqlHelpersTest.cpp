// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include <cmath>
#include <string>

#include "engine/SpatialJoinConfig.h"
#include "global/Constants.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeoSparqlHelpers.h"
#include "rdfTypes/Iri.h"
#include "util/GTestHelpers.h"

namespace {

using ad_utility::source_location;
using ad_utility::WktDist;
using ad_utility::WktGeometricRelation;
using ad_utility::WktLatitude;
using ad_utility::WktLongitude;
using ad_utility::detail::parseWktPoint;

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, ParseWktPoint) {
  // Test that the given WKT point parses correctly (with all three of
  // parseWktPoint, wktLatitude, and wktLongitude).
  auto testParseWktPointCorrect = [](const std::string& point,
                                     double expected_lng, double expected_lat) {
    auto [lng, lat] = parseWktPoint(point);
    ASSERT_DOUBLE_EQ(expected_lng, lng);
    ASSERT_DOUBLE_EQ(expected_lat, lat);
    ASSERT_DOUBLE_EQ(expected_lng, WktLongitude()(GeoPoint(lat, lng)));
    ASSERT_DOUBLE_EQ(expected_lat, WktLatitude()(GeoPoint(lat, lng)));
  };

  // Test that the given WKT point is invalid (with all three of
  // parseWktPoint, wktLongitude, and wktLatitude).
  auto testWktPointInvalid = [](const std::string& point) {
    auto [lat, lng] = parseWktPoint(point);
    ASSERT_TRUE(std::isnan(lng));
    ASSERT_TRUE(std::isnan(lat));
  };

  // Some valid WKT points, including those from the test for `wktDist` below.
  testParseWktPointCorrect("POINT(2.0 1.5)", 2.0, 1.5);
  testParseWktPointCorrect("POINT(2.0 -1.5)", 2.0, -1.5);
  testParseWktPointCorrect("PoInT(3   0.0)", 3.0, 0.0);
  testParseWktPointCorrect("pOiNt(7 -0.0)", 7.0, 0.0);
  testParseWktPointCorrect(" pOiNt\t(  7 \r -0.0 \n ) ", 7.0, 0.0);
  testParseWktPointCorrect("POINT(2.2945 48.8585)", 2.2945, 48.8585);
  testParseWktPointCorrect("POINT(2 48.8585)", 2.0, 48.8585);
  testParseWktPointCorrect("POINT(20 48.8585)", 20.0, 48.8585);
  testParseWktPointCorrect("POINT(7.8529 47.9957)", 7.8529, 47.9957);
  testParseWktPointCorrect("POINT(7.8529 47)", 7.8529, 47.0);
  testParseWktPointCorrect("POINT(17 47)", 17.0, 47.0);
  testParseWktPointCorrect("POINT(7 47)", 7.0, 47.0);

  // Invalid WKT points because of issues unrelated to the number format (one of
  // the quotes missing, one of the parentheses missing, it must be exactly two
  // coordinates).
  testWktPointInvalid("POINT42.0 7.8)");
  testWktPointInvalid("POINT(42.0 7.8");
  testWktPointInvalid("POINT(42.0)");
  testWktPointInvalid("POINT(42.0 7.8 3.14)");

  // Invalid WKT points because of issues related to the number format (dot must
  // have preceding integer part and succeeding decimal part, explicit plus sign
  // not allowed, scientific notation not allowed,
  testWktPointInvalid("POINT(42. 7.)");
  testWktPointInvalid("POINT(.42 .8)");
  testWktPointInvalid("POINT(+42.0 7.8)");
  testWktPointInvalid("POINT(42.0 +7.8)");
  testWktPointInvalid("POINT(42e3 7.8)");
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktDist) {
  using enum UnitOfMeasurement;
  GeoPoint eiffeltower{48.8585, 2.2945};
  GeoPoint frCathedral{47.9957, 7.8529};

  // Equal coordinates: distance 0.
  EXPECT_NEAR(WktDist()(frCathedral, frCathedral, KILOMETERS), 0, 0.01);
  EXPECT_NEAR(WktDist()(eiffeltower, eiffeltower, METERS), 0, 0.01);
  EXPECT_NEAR(WktDist()(eiffeltower, eiffeltower, MILES), 0, 0.01);

  // Distance between points: the Eiffel tower and the Freiburg Cathedral (421km
  // according to the distance measurement of Google Maps).
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral), 421.57, 0.02);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, KILOMETERS), 421.57, 0.02);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, METERS), 421569, 15);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, MILES), 261.95, 0.02);
  EXPECT_NEAR(ad_utility::WktMetricDist()(eiffeltower, frCathedral), 421569,
              15);

  // Distance between WKT non-point literals.
  EXPECT_NEAR(
      WktDist()(
          // Line between Freiburg Central Station and Freiburg University
          // Library.
          "\"LINESTRING(7.8412948 47.9977308, 7.8450491 47.9946000)\""
          "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
          // University building 101.
          "\"POLYGON((7.8346338 48.0126612,7.8348921 48.0123905,7.8349457 "
          "48.0124216,7.8349855 48.0124448,7.8353244 48.0126418,7.8354091 "
          "48.0126911,7.8352246 48.0129047,7.8351623 48.012879,7.8350687 "
          "48.0128404,7.8347244 48.0126985,7.8346338 48.0126612))\""
          "^^<http://www.opengis.net/ont/geosparql#wktLiteral>",
          KILOMETERS),
      1.7, 0.01);

  // Invalid WKT literal.
  EXPECT_TRUE(std::isnan(
      WktDist()(eiffeltower,
                // University building 101.
                "\"POLYGON(bla bli blu)\""
                "^^<http://www.opengis.net/ont/geosparql#wktLiteral>")));
}

// _____________________________________________________________________________
// Wrap a WKT geometry (without quotes or datatype) as a `geo:wktLiteral`
// string, as required by `GeoPointOrWkt`.
std::string wkt(const std::string& body) {
  return "\"" + body + "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
}

// Check that `WktGeometricRelation<SJType>()(a, b)` evaluates to `expected`.
template <SpatialJoinType::Enum SJType>
void checkGeoRelation(
    const ad_utility::GeoPointOrWkt& a, const ad_utility::GeoPointOrWkt& b,
    bool expected, source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  const auto geoRelationFunction = WktGeometricRelation<SJType>();
  EXPECT_EQ(geoRelationFunction(a, b), ValueId::makeFromBool(expected));
}

namespace geoRelationTestGeometries {
// A 10x10 square with corners (0,0) and (10,10).
const std::string polyBig = wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
// Strictly inside `polyBig`.
const std::string polySmallInside = wkt("POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))");
// Overlaps `polyBig` partially (shares the corner region around (5..10,
// 5..10), but each has area outside the other).
const std::string polyOverlap = wkt("POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))");
// Shares the edge x=10 with `polyBig`, but has disjoint interiors.
const std::string polyAdjacent =
    wkt("POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))");
// Same shape as `polyBig`.
const std::string polyEqual = wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
// Disjoint from all of the above.
const std::string polyFarAway =
    wkt("POLYGON((60 60, 70 60, 70 70, 60 70, 60 60))");

// A horizontal line from (0,0) to (10,0).
const std::string lineHoriz = wkt("LINESTRING(0 0, 10 0)");
// Crosses `lineHoriz` at the interior point (5,0).
const std::string lineCrossing = wkt("LINESTRING(5 -5, 5 5)");
// A strict, non-endpoint-sharing subset of `lineHoriz`'s interior.
const std::string lineSubset = wkt("LINESTRING(2 0, 8 0)");
// Overlaps `lineHoriz` collinearly, but sticks out on both ends.
const std::string lineOverlap = wkt("LINESTRING(5 0, 15 0)");
// Touches `lineHoriz` only at the shared endpoint (10,0).
const std::string lineTouching = wkt("LINESTRING(10 0, 10 10)");
// Disjoint from all of the above.
const std::string lineFarAway = wkt("LINESTRING(60 60, 70 60)");

// Strictly inside `polyBig`/interior of `lineHoriz`.
const GeoPoint pointInside{5, 5};
const std::string pointOnLineInterior = wkt("POINT(5 0)");
// On the boundary of `polyBig`/an endpoint of `lineHoriz`.
const std::string pointOnPolyBoundary = wkt("POINT(0 5)");
const std::string pointOnLineEndpoint = wkt("POINT(0 0)");
// Disjoint from all of the above.
const GeoPoint pointFarAway{50, 50};
}  // namespace geoRelationTestGeometries

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationIntersects) {
  using enum SpatialJoinType::Enum;
  using namespace geoRelationTestGeometries;
  checkGeoRelation<INTERSECTS>(polyBig, polySmallInside, true);
  checkGeoRelation<INTERSECTS>(polyBig, polyOverlap, true);
  checkGeoRelation<INTERSECTS>(polyBig, polyAdjacent, true);
  checkGeoRelation<INTERSECTS>(polyBig, polyFarAway, false);
  checkGeoRelation<INTERSECTS>(lineHoriz, lineCrossing, true);
  checkGeoRelation<INTERSECTS>(lineHoriz, lineFarAway, false);
  checkGeoRelation<INTERSECTS>(polyBig, pointInside, true);
  checkGeoRelation<INTERSECTS>(polyBig, pointFarAway, false);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationContainsWithin) {
  using enum SpatialJoinType::Enum;
  using namespace geoRelationTestGeometries;
  checkGeoRelation<CONTAINS>(polyBig, polySmallInside, true);
  checkGeoRelation<WITHIN>(polySmallInside, polyBig, true);
  checkGeoRelation<CONTAINS>(polySmallInside, polyBig, false);
  checkGeoRelation<CONTAINS>(polyBig, polyOverlap, false);
  checkGeoRelation<CONTAINS>(polyBig, polyFarAway, false);
  checkGeoRelation<CONTAINS>(polyBig, pointInside, true);
  checkGeoRelation<CONTAINS>(polyBig, pointOnPolyBoundary, false);
  checkGeoRelation<CONTAINS>(lineHoriz, lineSubset, true);
  checkGeoRelation<CONTAINS>(polyBig, polyEqual, true);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationCovers) {
  using enum SpatialJoinType::Enum;
  using namespace geoRelationTestGeometries;
  checkGeoRelation<COVERS>(polyBig, polySmallInside, true);
  checkGeoRelation<COVERS>(polyBig, pointOnPolyBoundary, true);
  checkGeoRelation<COVERS>(polyBig, polyFarAway, false);
  checkGeoRelation<COVERS>(polyBig, polyOverlap, false);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationTouches) {
  using enum SpatialJoinType::Enum;
  using namespace geoRelationTestGeometries;
  checkGeoRelation<TOUCHES>(polyBig, polyAdjacent, true);
  checkGeoRelation<TOUCHES>(polyBig, polySmallInside, false);
  checkGeoRelation<TOUCHES>(polyBig, polyOverlap, false);
  checkGeoRelation<TOUCHES>(polyBig, polyFarAway, false);
  checkGeoRelation<TOUCHES>(lineHoriz, lineTouching, true);
  checkGeoRelation<TOUCHES>(lineHoriz, pointOnLineEndpoint, true);
  checkGeoRelation<TOUCHES>(lineHoriz, pointOnLineInterior, false);
  checkGeoRelation<TOUCHES>(polyBig, pointOnPolyBoundary, true);
  checkGeoRelation<TOUCHES>(polyBig, pointInside, false);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationEquals) {
  using enum SpatialJoinType::Enum;
  using namespace geoRelationTestGeometries;
  checkGeoRelation<EQUALS>(polyBig, polyEqual, true);
  checkGeoRelation<EQUALS>(polyBig, polySmallInside, false);
  checkGeoRelation<EQUALS>(polyBig, polyFarAway, false);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationCrosses) {
  using enum SpatialJoinType::Enum;
  using namespace geoRelationTestGeometries;
  // Two lines crossing at a single interior point.
  checkGeoRelation<CROSSES>(lineHoriz, lineCrossing, true);
  // Collinear lines never cross.
  checkGeoRelation<CROSSES>(lineHoriz, lineSubset, false);
  checkGeoRelation<CROSSES>(lineHoriz, lineOverlap, false);
  // `crosses` is not defined between two geometries of equal, non-1
  // dimension: not even a point crosses itself.
  checkGeoRelation<CROSSES>(polyBig, polyOverlap, false);
  checkGeoRelation<CROSSES>(pointFarAway, pointFarAway, false);
  checkGeoRelation<CROSSES>(polyBig, polyFarAway, false);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationOverlaps) {
  using enum SpatialJoinType::Enum;
  using namespace geoRelationTestGeometries;
  checkGeoRelation<OVERLAPS>(polyBig, polyOverlap, true);
  checkGeoRelation<OVERLAPS>(polyBig, polySmallInside, false);
  checkGeoRelation<OVERLAPS>(polyBig, polyEqual, false);
  checkGeoRelation<OVERLAPS>(polyBig, polyFarAway, false);
  checkGeoRelation<OVERLAPS>(lineHoriz, lineOverlap, true);
  checkGeoRelation<OVERLAPS>(lineHoriz, lineSubset, false);
  // Two lines that only cross at a single point do not overlap (`overlaps`
  // requires the shared interior to be 1-dimensional, not a point).
  checkGeoRelation<OVERLAPS>(lineHoriz, lineCrossing, false);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelationUndefinedForInvalidWkt) {
  using enum SpatialJoinType::Enum;
  const auto geoRelationFunction = WktGeometricRelation<INTERSECTS>();
  EXPECT_EQ(geoRelationFunction(std::nullopt, wkt("POINT(0 0)")),
            ValueId::makeUndefined());
  EXPECT_EQ(geoRelationFunction(wkt("POLYGON(bla bli blu)"), wkt("POINT(0 0)")),
            ValueId::makeUndefined());
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktDe9imRelation) {
  // The `geof:relate` function is currently only a dummy implementation.
  AD_EXPECT_THROW_WITH_MESSAGE(
      ad_utility::WktDe9imRelation()(GeoPoint{1, 1}, GeoPoint{2, 2},
                                     std::string{"T*T***T**"}),
      ::testing::HasSubstr(
          "currently only implemented for a subset of all possible queries"));
}

}  // namespace
