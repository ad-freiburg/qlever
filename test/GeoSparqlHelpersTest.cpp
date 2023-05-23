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
using ad_utility::detail::parseWktPoint;

TEST(GeoSparqlHelpers, ParseWktPoint) {
  // Test that the given WKT point parses correctly (with all three of
  // parseWktPoint, wktLatitude, and wktLongitude).
  auto testParseWktPointCorrect = [](const std::string& point,
                                     double expected_lng, double expected_lat) {
    auto [lng, lat] = parseWktPoint(point);
    ASSERT_DOUBLE_EQ(expected_lng, lng);
    ASSERT_DOUBLE_EQ(expected_lat, lat);
    ASSERT_DOUBLE_EQ(expected_lng, wktLongitude(point));
    ASSERT_DOUBLE_EQ(expected_lat, wktLatitude(point));
  };

  // Test that the given WKT point is invalid (with all three of
  // parseWktPoint, wktLongitude, and wktLatitude).
  auto testWktPointInvalid = [](const std::string& point) {
    auto [lat, lng] = parseWktPoint(point);
    ASSERT_TRUE(std::isnan(lng));
    ASSERT_TRUE(std::isnan(lat));
    ASSERT_TRUE(std::isnan(wktLongitude(point)));
    ASSERT_TRUE(std::isnan(wktLatitude(point)));
  };

  // Some valid WKT points, including those from the test for `wktDist` below.
  testParseWktPointCorrect("POINT(2.0 1.5)", 2.0, 1.5);
  testParseWktPointCorrect("POINT(2.0 -1.5)", 2.0, -1.5);
  testParseWktPointCorrect("PoInT(3   0.0)", 3.0, 0.0);
  testParseWktPointCorrect("pOiNt(7 -0.0)", 7.0, 0.0);
  testParseWktPointCorrect(" pOiNt\t(  7 \r -0.0 \n ) ", 7.0, 0.0);
  testParseWktPointCorrect("POINT(2.2945 48.8585)", 2.2945, 48.8585);
  testParseWktPointCorrect("POINT(7.8529 47.9957)", 7.8529, 47.9957);

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

TEST(GeoSparqlHelpers, WktDist) {
  // Equal longitude, latitudes with diff 3.0 and mean zero. This returns 3
  // times the k1 from the formula, where the cosines are all 1.
  ASSERT_DOUBLE_EQ(wktDist("POINT(2.0 1.5)", "POINT(2.0 -1.5)"),
                   3.0 * (111.13209 - 0.56605 + 0.0012));

  // Equal latitude zero, longitudes with diff 4.0. This returns, 4 times the k2
  // from the formula, where the cosines are all 1.
  ASSERT_DOUBLE_EQ(wktDist("PoInT(3   0.0)", "pOiNt(7 -0.0)"),
                   4.0 * (111.41513 - 0.09455 + 0.00012));

  // Distance between the Eiffel tower and the Freibuger Münster (421km
  // according to the distance measurement of Google Maps, so our formula is not
  // that bad).
  ASSERT_DOUBLE_EQ(wktDist("POINT(2.2945 48.8585)", "POINT(7.8529 47.9957)"),
                   422.41514462162974);

  // One or both of the points invalid.
  ASSERT_TRUE(std::isnan(wktDist("POINT(2.0 1.5)", "POINT")));
  ASSERT_TRUE(std::isnan(wktDist("POINT", "POINT(2.0 -1.5)")));
  ASSERT_TRUE(std::isnan(wktDist("POINT", "POINT")));
}
