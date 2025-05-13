//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

#include "GeometryInfoTestHelpers.h"
#include "parser/GeoPoint.h"
#include "util/GeometryInfo.h"
namespace {

using ad_utility::GeometryInfo;

// ____________________________________________________________________________
TEST(GeometryInfoTest, BasicTests) {
  // Constructor and getters
  GeometryInfo g{5, {{1, 1}, {2, 2}}, {1.5, 1.5}};
  ASSERT_EQ(g.getWktType(), 5);
  ASSERT_NEAR(g.getCentroid().getLat(), 1.5, 0.0001);
  ASSERT_NEAR(g.getCentroid().getLng(), 1.5, 0.0001);
  auto [lowerLeft, upperRight] = g.getBoundingBox();
  ASSERT_NEAR(lowerLeft.getLat(), 1, 0.0001);
  ASSERT_NEAR(lowerLeft.getLng(), 1, 0.0001);
  ASSERT_NEAR(upperRight.getLat(), 2, 0.0001);
  ASSERT_NEAR(upperRight.getLng(), 2, 0.0001);

  // Too large wkt type value
  ASSERT_ANY_THROW(GeometryInfo(
      120, std::pair<GeoPoint, GeoPoint>(GeoPoint(1, 1), GeoPoint(2, 2)),
      GeoPoint(1.5, 1.5)));

  // Wrong bounding box point ordering
  ASSERT_ANY_THROW(GeometryInfo(
      1, std::pair<GeoPoint, GeoPoint>(GeoPoint(2, 2), GeoPoint(1, 1)),
      GeoPoint(1.5, 1.5)));
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromWktLiteral) {
  std::string lit =
      "\"POINT(3 4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  auto g = GeometryInfo::fromWktLiteral(lit);
  GeometryInfo exp{1, {{4, 3}, {4, 3}}, {4, 3}};
  checkGeoInfo(g, exp);

  std::string lit2 =
      "\"LINESTRING(2 2, 4 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  auto g2 = GeometryInfo::fromWktLiteral(lit2);
  GeometryInfo exp2{2, {{2, 2}, {4, 4}}, {3, 3}};
  checkGeoInfo(g2, exp2);

  std::string lit3 =
      "\"POLYGON(2 4, 4 4, 4 "
      "2, 2 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
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

}  // namespace
