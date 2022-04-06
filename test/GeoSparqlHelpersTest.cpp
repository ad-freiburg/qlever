// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de

#include <gtest/gtest.h>

#include <cmath>
#include <string>

#include "../src/util/GeoSparqlHelpers.h"

using ad_utility::wktDist;
using ad_utility::wktLatitude;
using ad_utility::wktLongitude;
using ad_utility::detail::checkWktPoint;
using ad_utility::detail::parseWktPoint;

TEST(GeoSparqlHelpers, CheckWktPoint) {
  // Valid points.
  ASSERT_STREQ("42.0 7.8)\"", checkWktPoint("\"POINT(42.0 7.8)\""));
  ASSERT_STREQ("42.0 7.8)\"", checkWktPoint("\"point(42.0 7.8)\""));
  ASSERT_STREQ("42.0 7.8)\"", checkWktPoint("\"PoInT(42.0 7.8)\""));
  ASSERT_STREQ("42.0  7.8 )\"", checkWktPoint("\"POINT ( 42.0  7.8 )\""));
  ASSERT_STREQ("-42.0  -7.8)\"", checkWktPoint("\"POINT(-42.0  -7.8)\""));
  ASSERT_STREQ("42 7)\"", checkWktPoint("\"POINT(42 7)\""));

  // Invalid points.
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0 7.8)"));
  ASSERT_FALSE(checkWktPoint("POINT(42.0 7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT42.0 7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0 7.8\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(+42.0 7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0 +7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42e3 7.8)\""));
}

TEST(GeoSparqlHelpers, WktLatLng) {
  // Valid WKT points.
  ASSERT_FLOAT_EQ(4.20, wktLongitude("\"POINT(4.20  3.14)\""));
  ASSERT_FLOAT_EQ(3.14, wktLatitude("\"POINT(-4.20  3.14)\""));

  // Invalid WKT points.
  ASSERT_TRUE(std::isnan(wktLongitude("\"POINT\"")));
  ASSERT_TRUE(std::isnan(wktLatitude("\"POINT\"")));
}

TEST(GeoSparqlHelpers, ParseWktPoint) {
  auto assertPointEq = [](double expected_lng, double expected_lat,
                          const std::string& point) {
    auto [lat, lng] = parseWktPoint(point);
    ASSERT_FLOAT_EQ(expected_lng, lng);
    ASSERT_FLOAT_EQ(expected_lat, lat);
  };
  assertPointEq(2.0, 1.5, "\"POINT(2.0 1.5)\"");
  assertPointEq(2.0, -1.5, "\"POINT(2.0 -1.5)\"");
  assertPointEq(3.0, 0.0, "\"PoInT(3   0.0)\"");
  assertPointEq(7.0, 0.0, "\"pOiNt(7 -0.0)\"");
}

TEST(GeoSparqlHelpers, WktDist) {
  // Equal longitude, latitudes with diff 3.0 and mean zero.
  ASSERT_FLOAT_EQ(3.0 * (111.13209 - 0.56605 + 0.0012),
                  wktDist("\"POINT(2.0 1.5)\"", "\"POINT(2.0 -1.5)\""));

  // Equal latitude zero, longitudes with diff 4.0.
  ASSERT_FLOAT_EQ(4.0 * (111.41513 - 0.09455 + 0.00012),
                  wktDist("\"PoInT(3   0.0)\"", "\"pOiNt(7 -0.0)\""));

  // One or both of the points invalid.
  ASSERT_TRUE(std::isnan(wktDist("\"POINT(2.0 1.5)\"", "\"POINT")));
  ASSERT_TRUE(std::isnan(wktDist("\"POINT", "\"POINT(2.0 -1.5)\"")));
  ASSERT_TRUE(std::isnan(wktDist("\"POINT", "\"POINT")));
}
