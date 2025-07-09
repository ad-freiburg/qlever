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
using ad_utility::WktDistGeoPoints;
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
  // Equal longitude, latitudes with diff 3.0 and mean zero.
  ASSERT_NEAR(WktDistGeoPoints()(GeoPoint(1.5, 2.0), GeoPoint(-1.5, 2.0)),
              333.58, 0.01);

  // Equal latitude zero, longitudes with diff 4.0.
  ASSERT_NEAR(WktDistGeoPoints()(GeoPoint(0.0, 3.0), GeoPoint(-0.0, 7.0)),
              444.7804, 0.01);

  // Distance between the Eiffel tower and the Freibuger Münster (421km
  // according to the distance measurement of Google Maps).
  GeoPoint eiffeltower = GeoPoint(48.8585, 2.2945);
  GeoPoint frCathedral = GeoPoint(47.9957, 7.8529);
  using enum UnitOfMeasurement;
  ASSERT_NEAR(WktDistGeoPoints()(eiffeltower, frCathedral), 421.098, 0.01);
  ASSERT_NEAR(WktDistGeoPoints()(eiffeltower, frCathedral, KILOMETERS), 421.098,
              0.01);
  ASSERT_NEAR(WktDistGeoPoints()(eiffeltower, frCathedral, METERS), 421098, 1);
  ASSERT_NEAR(WktDistGeoPoints()(eiffeltower, frCathedral, MILES), 261.658,
              0.01);
  ASSERT_NEAR(ad_utility::WktMetricDistGeoPoints()(eiffeltower, frCathedral),
              421098, 1);

  ASSERT_NEAR(WktDistGeoPoints()(eiffeltower, eiffeltower, METERS), 0, 0.01);
  ASSERT_NEAR(WktDistGeoPoints()(eiffeltower, eiffeltower, MILES), 0, 0.01);
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, KmToUnit) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;
  auto kmToUnit = kilometerToUnit;

  ASSERT_NEAR(kmToUnit(0.0, std::nullopt), 0.0, 0.0001);
  ASSERT_NEAR(kmToUnit(0.0, KILOMETERS), 0.0, 0.0001);
  ASSERT_NEAR(kmToUnit(0.0, METERS), 0.0, 0.0001);
  ASSERT_NEAR(kmToUnit(0.0, MILES), 0.0, 0.0001);
  ASSERT_NEAR(kmToUnit(-500.0, KILOMETERS), -500.0, 0.0001);
  ASSERT_NEAR(kmToUnit(-500.0, std::nullopt), -500.0, 0.0001);
  ASSERT_NEAR(kmToUnit(500.0, METERS), 500000.0, 0.0001);
  ASSERT_NEAR(kmToUnit(500.0, MILES), 310.685595, 0.0001);
  ASSERT_NEAR(kmToUnit(1.0, MILES), 0.62137119, 0.0001);
  AD_EXPECT_THROW_WITH_MESSAGE(kmToUnit(1.0, UNKNOWN),
                               ::testing::HasSubstr("Unsupported unit"));
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, UnitToKm) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;
  auto toKm = valueInUnitToKilometer;

  ASSERT_NEAR(toKm(0.0, std::nullopt), 0.0, 0.0001);
  ASSERT_NEAR(toKm(0.0, KILOMETERS), 0.0, 0.0001);
  ASSERT_NEAR(toKm(0.0, METERS), 0.0, 0.0001);
  ASSERT_NEAR(toKm(0.0, MILES), 0.0, 0.0001);
  ASSERT_NEAR(toKm(-500.0, KILOMETERS), -500.0, 0.0001);
  ASSERT_NEAR(toKm(-500.0, std::nullopt), -500.0, 0.0001);
  ASSERT_NEAR(toKm(500000.0, METERS), 500.0, 0.0001);
  ASSERT_NEAR(toKm(310.685595, MILES), 500.0, 0.0001);
  ASSERT_NEAR(toKm(0.62137119, MILES), 1.0, 0.0001);
  AD_EXPECT_THROW_WITH_MESSAGE(toKm(1.0, UNKNOWN),
                               ::testing::HasSubstr("Unsupported unit"));
}

// _____________________________________________________________________________
TEST(GeoSparqlHelpers, IriToUnit) {
  using namespace ad_utility::detail;
  using enum UnitOfMeasurement;
  auto iriToUnit = iriToUnitOfMeasurement;

  ASSERT_EQ(iriToUnit(""), UNKNOWN);
  ASSERT_EQ(iriToUnit("http://example.com"), UNKNOWN);
  ASSERT_EQ(iriToUnit("http://qudt.org/vocab/unit/"), UNKNOWN);
  ASSERT_EQ(iriToUnit("http://qudt.org/vocab/unit/M"), METERS);
  ASSERT_EQ(iriToUnit("http://qudt.org/vocab/unit/KiloM"), KILOMETERS);
  ASSERT_EQ(iriToUnit("http://qudt.org/vocab/unit/MI"), MILES);
}

// _____________________________________________________________________________
template <SpatialJoinType SJType>
void checkGeoRelationDummyImpl(
    source_location sourceLocation = source_location::current()) {
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
}

}  // namespace
