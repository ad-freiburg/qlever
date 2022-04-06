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
  // Valid points (note that in the case of success, checkWktPoint returns a
  // pointer to the character, where the first coordinate starts).
  ASSERT_STREQ(checkWktPoint("\"POINT(42.0 7.8)\""), "42.0 7.8)\"");
  ASSERT_STREQ(checkWktPoint("\"point(42.0 7.8)\""), "42.0 7.8)\"");
  ASSERT_STREQ(checkWktPoint("\"PoInT(42.0 7.8)\""), "42.0 7.8)\"");
  ASSERT_STREQ(checkWktPoint("\"POINT ( 42.0  7.8 )\""), "42.0  7.8 )\"");
  ASSERT_STREQ(checkWktPoint("\"POINT(-42.0  -7.8)\""), "-42.0  -7.8)\"");
  ASSERT_STREQ(checkWktPoint("\"POINT(  42 7 )\""), "42 7 )\"");

  // Invalid points (one of the quotes missing, one of the parentheses missing,
  // explicit plus sign not allowed, scientific notation not allowed, it must be
  // exactly two coordinates).
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0 7.8)"));
  ASSERT_FALSE(checkWktPoint("POINT(42.0 7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT42.0 7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0 7.8\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(+42.0 7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0 +7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42e3 7.8)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0)\""));
  ASSERT_FALSE(checkWktPoint("\"POINT(42.0 7.8 3.14)\""));
}

TEST(GeoSparqlHelpers, WktLatLng) {
  // Valid WKT points.
  ASSERT_FLOAT_EQ(wktLongitude("\"POINT(4.20  3.14)\""), 4.20);
  ASSERT_FLOAT_EQ(wktLatitude("\"POINT(-4.20  3.14)\""), 3.14);

  // Invalid WKT points.
  ASSERT_TRUE(std::isnan(wktLongitude("\"POINT(4.20)\"")));
  ASSERT_TRUE(std::isnan(wktLatitude("\"POINT\"(4.20)\"")));
}

TEST(GeoSparqlHelpers, ParseWktPoint) {
  auto testParseWktPoint = [](const std::string& point, double expected_lng,
                              double expected_lat) {
    auto [lat, lng] = parseWktPoint(point);
    ASSERT_FLOAT_EQ(expected_lng, lng);
    ASSERT_FLOAT_EQ(expected_lat, lat);
  };
  testParseWktPoint("\"POINT(2.0 1.5)\"", 2.0, 1.5);
  testParseWktPoint("\"POINT(2.0 -1.5)\"", 2.0, -1.5);
  testParseWktPoint("\"PoInT(3   0.0)\"", 3.0, 0.0);
  testParseWktPoint("\"pOiNt(7 -0.0)\"", 7.0, 0.0);
}

TEST(GeoSparqlHelpers, WktDist) {
  // Equal longitude, latitudes with diff 3.0 and mean zero. This returns 3
  // times the k1 from the formula, where the cosines are all 1.
  ASSERT_FLOAT_EQ(wktDist("\"POINT(2.0 1.5)\"", "\"POINT(2.0 -1.5)\""),
                  3.0 * (111.13209 - 0.56605 + 0.0012));

  // Equal latitude zero, longitudes with diff 4.0. This returns, 4 times the k2
  // from the formula, where the cosines are all 1.
  ASSERT_FLOAT_EQ(wktDist("\"PoInT(3   0.0)\"", "\"pOiNt(7 -0.0)\""),
                  4.0 * (111.41513 - 0.09455 + 0.00012));

  // Distance between the Eiffel tower and the Freibuger Münster (421km
  // according to the distance measurement of Google Maps, so our formula is not
  // that bad).
  ASSERT_FLOAT_EQ(
      wktDist("\"POINT(2.2945 48.8585)\"", "\"POINT(7.8529 47.9957)\""),
      422.41513);

  // One or both of the points invalid.
  ASSERT_TRUE(std::isnan(wktDist("\"POINT(2.0 1.5)\"", "\"POINT")));
  ASSERT_TRUE(std::isnan(wktDist("\"POINT", "\"POINT(2.0 -1.5)\"")));
  ASSERT_TRUE(std::isnan(wktDist("\"POINT", "\"POINT")));
}
