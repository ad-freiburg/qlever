//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include "global/Constants.h"
#include "parser/GeoPoint.h"

TEST(GeoPoint, GeoPoint) {
  GeoPoint g = GeoPoint(70.5, 130.2);

  ASSERT_DOUBLE_EQ(g.getLat(), 70.5);
  ASSERT_DOUBLE_EQ(g.getLng(), 130.2);

  ASSERT_THROW(GeoPoint(-99.5, 1.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(99.5, 1.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(9.5, -185.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(9.5, 185.0), CoordinateOutOfRangeException);
  ASSERT_NO_THROW(GeoPoint(90, 180));
  ASSERT_NO_THROW(GeoPoint(0, 1.0));
  ASSERT_NO_THROW(GeoPoint(1.0, -180.0));
  ASSERT_NO_THROW(GeoPoint(0, 0));

  g = GeoPoint(0, 0);
  ASSERT_DOUBLE_EQ(g.getLat(), 0);
  ASSERT_DOUBLE_EQ(g.getLng(), 0);
  g = GeoPoint(90, 180);
  ASSERT_DOUBLE_EQ(g.getLat(), 90);
  ASSERT_DOUBLE_EQ(g.getLng(), 180);
  g = GeoPoint(-90, -180);
  ASSERT_DOUBLE_EQ(g.getLat(), -90);
  ASSERT_DOUBLE_EQ(g.getLng(), -180);
  g = GeoPoint(-90, 180);
  ASSERT_DOUBLE_EQ(g.getLat(), -90);
  ASSERT_DOUBLE_EQ(g.getLng(), 180);
  g = GeoPoint(90, -180);
  ASSERT_DOUBLE_EQ(g.getLat(), 90);
  ASSERT_DOUBLE_EQ(g.getLng(), -180);
  g = GeoPoint(0, 180);
  ASSERT_DOUBLE_EQ(g.getLat(), 0);
  ASSERT_DOUBLE_EQ(g.getLng(), 180);
  g = GeoPoint(90, 0);
  ASSERT_DOUBLE_EQ(g.getLat(), 90);
  ASSERT_DOUBLE_EQ(g.getLng(), 0);
}

TEST(GeoPoint, string) {
  GeoPoint g = GeoPoint(-70.5, -130.2);
  ASSERT_EQ(g.toStringRepresentation(), "POINT(-130.200000 -70.500000)");

  g = GeoPoint(90, 180);
  ASSERT_EQ(g.toStringRepresentation(), "POINT(180.000000 90.000000)");

  g = GeoPoint(0, 0);
  ASSERT_EQ(g.toStringRepresentation(), "POINT(0.000000 0.000000)");

  g = GeoPoint(-70.5, -130.2);
  auto strpair = g.toStringAndType();
  ASSERT_EQ(strpair.first, "POINT(-130.200000 -70.500000)");
  ASSERT_EQ(strpair.second, GEO_WKT_LITERAL);
}

TEST(GeoPoint, bitRepresentation) {
  GeoPoint g = GeoPoint(-70.5, -130.2);
  constexpr double lat = ((-70.5 + 90) / (2 * 90)) * (1 << 30);
  ASSERT_EQ(g.toBitRepresentation() >> 30, round(lat));
  constexpr double lng = ((-130.2 + 180) / (2 * 180)) * (1 << 30);
  ASSERT_EQ(g.toBitRepresentation() & ((1 << 30) - 1), round(lng));

  constexpr size_t expect = (static_cast<size_t>(1) << 30) << 30 | (1 << 30);
  g = GeoPoint(90, 180);
  ASSERT_EQ(g.toBitRepresentation(), expect);

  g = GeoPoint(-90, -180);
  ASSERT_EQ(g.toBitRepresentation(), 0);

  const size_t expect2 =
      (static_cast<size_t>(round(lat)) << 30) | static_cast<size_t>(round(lng));
  g = GeoPoint::fromBitRepresentation(expect2);
  ASSERT_DOUBLE_EQ(round(1000 * g.getLat()), 1000 * -70.5);
  ASSERT_DOUBLE_EQ(round(1000 * g.getLng()), 1000 * -130.2);

  g = GeoPoint::fromBitRepresentation(0);
  ASSERT_DOUBLE_EQ(g.getLat(), -90);
  ASSERT_DOUBLE_EQ(g.getLng(), -180);
}

TEST(GeoPoint, parseFromLiteral) {
  constexpr auto testParseFromLiteral = [](const char* input, bool hasVal,
                                           double lng = 0.0, double lat = 0.0) {
    std::string content = absl::StrCat(
        "\"", input, "\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
    auto value =
        ad_utility::triple_component::Literal::fromStringRepresentation(
            content);
    auto g = GeoPoint::parseFromLiteral(value);
    ASSERT_EQ(g.has_value(), hasVal);
    if (g.has_value()) {
      ASSERT_DOUBLE_EQ(g.value().getLat(), lat);
      ASSERT_DOUBLE_EQ(g.value().getLng(), lng);
    }
  };

  testParseFromLiteral("POINT(24.3 26.8)", true, 24.3, 26.8);
  testParseFromLiteral("POINT (24.3   26.8)", true, 24.3, 26.8);
  testParseFromLiteral("point(24.3 26.8)", true, 24.3, 26.8);
  testParseFromLiteral(" pOiNt (24.3 26.8 )", true, 24.3, 26.8);
  testParseFromLiteral("POINT(0.3 -90.0)", true, 0.3, -90.0);
  testParseFromLiteral("POINT(90.0 -180)", true, 90, -180);
  testParseFromLiteral("POINT(0 0)", true);
  testParseFromLiteral("POINT(0.0 0.0)", true);
  testParseFromLiteral(" POiNT ( 0.0  0.0 ) ", true);
  testParseFromLiteral("POINT(24.326.8)", false);
  testParseFromLiteral("POINT(24.326.8)", false);
  testParseFromLiteral("POINT()", false);
  testParseFromLiteral("()", false);
  testParseFromLiteral("(2.3 5.6)", false);
  testParseFromLiteral("notapoint", false);
  testParseFromLiteral("", false);
}
