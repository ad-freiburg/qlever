//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gmock/gmock.h>

#include "GeometryInfoTestHelpers.h"
#include "parser/GeoPoint.h"
#include "util/GTestHelpers.h"
#include "util/GeometryInfo.h"

namespace {

using namespace ad_utility;
using namespace geoInfoTestHelpers;

// Example WKT literals for the tests below
constexpr std::string_view lit =
    "\"POINT(3 4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view lit2 =
    "\"LINESTRING(2 2, 4 "
    "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
constexpr std::string_view lit3 =
    "\"POLYGON(2 4, 4 4, 4 "
    "2, 2 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";

// ____________________________________________________________________________
TEST(GeometryInfoTest, BasicTests) {
  // Constructor and getters
  GeometryInfo g{5, {{1, 1}, {2, 2}}, {1.5, 1.5}};
  ASSERT_EQ(g.getWktType().type_, 5);
  ASSERT_NEAR(g.getCentroid().centroid_.getLat(), 1.5, 0.0001);
  ASSERT_NEAR(g.getCentroid().centroid_.getLng(), 1.5, 0.0001);
  auto [lowerLeft, upperRight] = g.getBoundingBox();
  ASSERT_NEAR(lowerLeft.getLat(), 1, 0.0001);
  ASSERT_NEAR(lowerLeft.getLng(), 1, 0.0001);
  ASSERT_NEAR(upperRight.getLat(), 2, 0.0001);
  ASSERT_NEAR(upperRight.getLng(), 2, 0.0001);

  // Too large wkt type value
  AD_EXPECT_THROW_WITH_MESSAGE(GeometryInfo(120, {{1, 1}, {2, 2}}, {1.5, 1.5}),
                               ::testing::HasSubstr("WKT Type out of range"));

  // Wrong bounding box point ordering
  AD_EXPECT_THROW_WITH_MESSAGE(
      GeometryInfo(1, {{2, 2}, {1, 1}}, {1.5, 1.5}),
      ::testing::HasSubstr("Bounding box coordinates invalid"));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromWktLiteral) {
  auto g = GeometryInfo::fromWktLiteral(lit);
  GeometryInfo exp{1, {{4, 3}, {4, 3}}, {4, 3}};
  checkGeoInfo(g, exp);

  auto g2 = GeometryInfo::fromWktLiteral(lit2);
  GeometryInfo exp2{2, {{2, 2}, {4, 4}}, {3, 3}};
  checkGeoInfo(g2, exp2);

  auto g3 = GeometryInfo::fromWktLiteral(lit3);
  GeometryInfo exp3{3, {{2, 2}, {4, 4}}, {3, 3}};
  checkGeoInfo(g3, exp3);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromGeoPoint) {
  GeoPoint p{1.234, 5.678};
  auto g = GeometryInfo::fromGeoPoint(p);
  GeometryInfo exp{1, {p, p}, p};
  checkGeoInfo(g, exp);

  GeoPoint p2{0, 0};
  auto g2 = GeometryInfo::fromGeoPoint(p2);
  GeometryInfo exp2{1, {p2, p2}, p2};
  checkGeoInfo(g2, exp2);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, RequestedInfoInstance) {
  checkRequestedInfoForInstance(GeometryInfo::fromWktLiteral(lit));
  checkRequestedInfoForInstance(GeometryInfo::fromWktLiteral(lit2));
  checkRequestedInfoForInstance(GeometryInfo::fromWktLiteral(lit3));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, RequestedInfoLiteral) {
  checkRequestedInfoForWktLiteral(lit);
  checkRequestedInfoForWktLiteral(lit2);
  checkRequestedInfoForWktLiteral(lit3);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, BoundingBoxAsWKT) {
  BoundingBox bb1{{0, 0}, {1, 1}};
  ASSERT_EQ(bb1.asWkt(), "POLYGON((0 0,1 0,1 1,0 1,0 0))");
  BoundingBox bb2{{0, 0}, {0, 0}};
  ASSERT_EQ(bb2.asWkt(), "POLYGON((0 0,0 0,0 0,0 0,0 0))");
  auto bb3 = GeometryInfo::getBoundingBox(
      "\"LINESTRING(2 4,8 8)\""
      "^^<http://www.opengis.net/ont/geosparql#wktLiteral>");
  ASSERT_EQ(bb3.asWkt(), "POLYGON((2 4,8 4,8 8,2 8,2 4))");
}

}  // namespace
