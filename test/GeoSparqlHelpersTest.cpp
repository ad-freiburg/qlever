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
#include "rdfTypes/Iri.h"
#include "util/GTestHelpers.h"
#include "util/GeoSparqlHelpers.h"

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
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral), 421.68, 0.01);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, KILOMETERS), 421.68, 0.01);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, METERS), 421676, 1);
  EXPECT_NEAR(WktDist()(eiffeltower, frCathedral, MILES), 262.02, 0.01);
  EXPECT_NEAR(ad_utility::WktMetricDist()(eiffeltower, frCathedral), 421676, 1);

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
template <SpatialJoinType SJType>
void checkGeoRelationDummyImpl(
    source_location sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  const auto geoRelationFunction = WktGeometricRelation<SJType>();
  AD_EXPECT_THROW_WITH_MESSAGE(
      geoRelationFunction(GeoPoint{1, 1}, GeoPoint{2, 2}),
      ::testing::HasSubstr(
          "currently only implemented for a subset of all possible queries"));
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, WktGeometricRelation) {
  // Currently the geometric relation functions are only a dummy implementation
  using enum SpatialJoinType;
  checkGeoRelationDummyImpl<INTERSECTS>();
  checkGeoRelationDummyImpl<CONTAINS>();
  checkGeoRelationDummyImpl<COVERS>();
  checkGeoRelationDummyImpl<CROSSES>();
  checkGeoRelationDummyImpl<TOUCHES>();
  checkGeoRelationDummyImpl<EQUALS>();
  checkGeoRelationDummyImpl<OVERLAPS>();
  checkGeoRelationDummyImpl<WITHIN>();
}

}  // namespace
