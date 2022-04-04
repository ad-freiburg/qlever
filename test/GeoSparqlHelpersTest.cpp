// Copyright 2022, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de

#include <gtest/gtest.h>

#include <cmath>
#include <string>

#include "../src/util/GeoSparqlHelpers.h"

TEST(GeoSparqlHelpers, CheckWktPointPrefix) {
  ASSERT_TRUE(ad_utility::detail::checkWktPointPrefix("\"POINT(xxxxx"));
  ASSERT_TRUE(ad_utility::detail::checkWktPointPrefix("\"point(xxxxx"));
  ASSERT_TRUE(ad_utility::detail::checkWktPointPrefix("\"pOiNt(xxxxx"));
  ASSERT_TRUE(ad_utility::detail::checkWktPointPrefix("\"POINT(1xxxx"));
  ASSERT_TRUE(ad_utility::detail::checkWktPointPrefix("\"POINT(xxxxx"));
  ASSERT_FALSE(ad_utility::detail::checkWktPointPrefix("\"POINT("));
  ASSERT_FALSE(ad_utility::detail::checkWktPointPrefix("\"POINTxxxxx"));
  ASSERT_FALSE(ad_utility::detail::checkWktPointPrefix("POINT(xxxxxx"));
}

TEST(GeoSparqlHelpers, WktLatLng) {
  // Valid WKT points.
  ASSERT_FLOAT_EQ(4.20, ad_utility::wktLongitude("\"POINT(4.20 3.14)\""));
  ASSERT_FLOAT_EQ(3.14, ad_utility::wktLatitude("\"POINT(-4.20 3.14)\""));

  // Invalid WKT points.
  ASSERT_TRUE(std::isnan(ad_utility::wktLongitude("\"POINT\"")));
  ASSERT_TRUE(std::isnan(ad_utility::wktLatitude("\"POINT\"")));
}

TEST(GeoSparqlHelpers, WktDist) {
  // Equal longitude, latitudes with diff 3.0 and mean zero.
  ASSERT_FLOAT_EQ(
      3.0 * (111.13209 - 0.56605 + 0.0012),
      ad_utility::wktDist("\"POINT(2.0 1.5)\"", "\"POINT(2.0 -1.5)\""));

  // Equal latitude zero, longitudes with diff 4.0.
  ASSERT_FLOAT_EQ(
      4.0 * (111.41513 - 0.09455 + 0.00012),
      ad_utility::wktDist("\"PoInT(3   0.0)\"", "\"pOiNt(7 -0.0)\""));

  // Invalid WKT points.
  ASSERT_TRUE(std::isnan(ad_utility::wktDist("\"POINT", "\"POINT")));
}
