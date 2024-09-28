//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>

#include <vector>

#include "global/Constants.h"
#include "parser/GeoPoint.h"
#include "util/GTestHelpers.h"
#include "util/GeoSparqlHelpers.h"

// _____________________________________________________________________________
TEST(GeoPoint, GeoPoint) {
  GeoPoint g = GeoPoint(70.5, 130.2);

  ASSERT_DOUBLE_EQ(g.getLat(), 70.5);
  ASSERT_DOUBLE_EQ(g.getLng(), 130.2);

  // Check that we get warnings only when either the latitude or the longitude
  // is out of range.
  auto isOutOfRangeWarning =
      ::testing::MatchesRegex(".*WARN.*out of range, clamped to POINT.*");
  auto invalidCoordinate = ad_utility::detail::invalidCoordinate;
  auto capturedOutputForGeoPoint = [](double lat, double lng) -> std::string {
    testing::internal::CaptureStdout();
    GeoPoint(lat, lng);
    return testing::internal::GetCapturedStdout();
  };
  EXPECT_THAT(capturedOutputForGeoPoint(-99.5, 1.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(99.5, 1.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(9.5, -185.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(9.5, 185.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(0, 185.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(90.1, 180.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(90.1, -180.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(-90.1, 180.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(-90.1, -180.0), isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(invalidCoordinate, 20.0),
              isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(20.0, invalidCoordinate),
              isOutOfRangeWarning);
  EXPECT_THAT(capturedOutputForGeoPoint(invalidCoordinate, invalidCoordinate),
              isOutOfRangeWarning);
  ASSERT_TRUE(capturedOutputForGeoPoint(0, 0).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(0, 180).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(0, -180).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(90, 0).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(90, 180).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(90, -180).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(-90, 0).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(-90, 180).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(-90, -180).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(0, 1.0).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(1.0, -180.0).empty());
  ASSERT_TRUE(capturedOutputForGeoPoint(0, 0).empty());

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

// _____________________________________________________________________________
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

// _____________________________________________________________________________
TEST(GeoPoint, bitRepresentation) {
  GeoPoint g = GeoPoint(-70.5, -130.2);
  constexpr double lat = ((-70.5 + 90) / (2 * 90)) * (1 << 30);
  ASSERT_EQ(g.toBitRepresentation() >> 30, round(lat));
  constexpr double lng = ((-130.2 + 180) / (2 * 180)) * (1 << 30);
  ASSERT_EQ(g.toBitRepresentation() & ((1 << 30) - 1), round(lng));

  constexpr size_t expect1 = (static_cast<size_t>(1) << 60) - 1;
  g = GeoPoint(90, 180);
  ASSERT_EQ(g.toBitRepresentation(), expect1);
  // Upper 4 bits must be 0 for ValueId Datatype
  ASSERT_EQ(g.toBitRepresentation() >> 60, 0);

  g = GeoPoint(-90, -180);
  ASSERT_EQ(g.toBitRepresentation(), 0);

  constexpr size_t expect2 = (static_cast<size_t>(1) << 30) - 1;
  g = GeoPoint(-90, 180);
  ASSERT_EQ(g.toBitRepresentation(), expect2);

  const size_t expect3 =
      (static_cast<size_t>(round(lat)) << 30) | static_cast<size_t>(round(lng));
  g = GeoPoint::fromBitRepresentation(expect3);
  constexpr auto precision = 0.00001;
  ASSERT_NEAR(g.getLat(), -70.5, precision);
  ASSERT_NEAR(g.getLng(), -130.2, precision);

  g = GeoPoint::fromBitRepresentation(0);
  ASSERT_DOUBLE_EQ(g.getLat(), -90);
  ASSERT_DOUBLE_EQ(g.getLng(), -180);
}

// _____________________________________________________________________________
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

  // Tests for Literals with wkt type
  testParseFromLiteral("POINT(24.3 26.8)", true, 24.3, 26.8);
  testParseFromLiteral("POINT (24.3   26.8)", true, 24.3, 26.8);
  testParseFromLiteral("point(24.3 26.8)", true, 24.3, 26.8);
  testParseFromLiteral(" pOiNt (24.3 26.8 )", true, 24.3, 26.8);
  testParseFromLiteral("POINT(0.3 -90.0)", true, 0.3, -90.0);
  testParseFromLiteral("POINT(-180.0 90.0)", true, -180, 90);
  testParseFromLiteral("POINT(0.0 0.0)", true);
  testParseFromLiteral(" POiNT ( 0.0  0.0 ) ", true);
  testParseFromLiteral("POLYGON(0.0 0.0, 1.1 1.1, 2.2 2.2)", false);
  testParseFromLiteral(
      "MULTIPOLYGON((0.0 0.0, 1.1 1.1, 2.2 2.2),(3.3 3.3, 4.4 4.4, 5.5 5.5))",
      false);
  testParseFromLiteral("POINT(24.326.8)", false);
  testParseFromLiteral("POINT(24.326.8)", false);
  testParseFromLiteral("POINT()", false);
  testParseFromLiteral("()", false);
  testParseFromLiteral("(2.3 5.6)", false);
  testParseFromLiteral("notapoint", false);
  testParseFromLiteral("", false);

  // Literals of different type
  ASSERT_FALSE(
      GeoPoint::parseFromLiteral(
          ad_utility::triple_component::Literal::fromStringRepresentation(
              "\"123\"^^xsd:integer"))
          .has_value());
  ASSERT_FALSE(
      GeoPoint::parseFromLiteral(
          ad_utility::triple_component::Literal::fromStringRepresentation(
              "\"hi\"@en"))
          .has_value());
}

// _____________________________________________________________________________
TEST(GeoPoint, equal) {
  auto g1 = GeoPoint(-70.5, -130.2);
  auto g2 = GeoPoint(90, 180);
  auto g3 = GeoPoint(-90, -180);
  auto g4 = GeoPoint(-90, 180);
  auto g5 = GeoPoint::fromBitRepresentation(0);
  auto g6 = GeoPoint(-70.5, -130.2);
  auto g7 = GeoPoint(0, 0);
  auto g8 = GeoPoint(0, 0);
  auto g9 = GeoPoint(-90, -180);

  std::vector<GeoPoint> f = {g1, g2, g3, g4, g7};
  for (size_t i = 0; i < f.size(); i++) {
    for (size_t j = 0; j < f.size(); j++) {
      if (i != j) {
        ASSERT_FALSE(f[i] == f[j]);
      }
    }
  }

  ASSERT_TRUE(g1 == g6);
  ASSERT_TRUE(g7 == g8);
  ASSERT_TRUE(g5 == g3);
  ASSERT_TRUE(g3 == g9);
}

// _____________________________________________________________________________
TEST(GeoPoint, Hashing) {
  GeoPoint g1{50.0, 50.0};
  GeoPoint g2{10.5, 80.5};
  ad_utility::HashSet<GeoPoint> set{g1, g2};
  EXPECT_THAT(set, ::testing::UnorderedElementsAre(g1, g2));
}
