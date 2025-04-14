//  Copyright 2025, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <gtest/gtest.h>

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
  ASSERT_EQ(g.getWktType(), 1);
  ASSERT_NEAR(g.getCentroid().getLat(), 4, 0.001);
  ASSERT_NEAR(g.getCentroid().getLng(), 3, 0.001);
  auto [lowerLeft, upperRight] = g.getBoundingBox();
  ASSERT_NEAR(lowerLeft.getLat(), 4, 0.001);
  ASSERT_NEAR(lowerLeft.getLng(), 3, 0.001);
  ASSERT_NEAR(upperRight.getLat(), 4, 0.001);
  ASSERT_NEAR(upperRight.getLng(), 3, 0.001);

  std::string lit2 =
      "\"LINESTRING(2 2, 4 "
      "4)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  auto g2 = GeometryInfo::fromWktLiteral(lit2);
  ASSERT_EQ(g2.getWktType(), 2);
  ASSERT_NEAR(g2.getCentroid().getLat(), 3, 0.001);
  ASSERT_NEAR(g2.getCentroid().getLng(), 3, 0.001);
  auto [lowerLeft2, upperRight2] = g2.getBoundingBox();
  ASSERT_NEAR(lowerLeft2.getLat(), 2, 0.001);
  ASSERT_NEAR(lowerLeft2.getLng(), 2, 0.001);
  ASSERT_NEAR(upperRight2.getLat(), 4, 0.001);
  ASSERT_NEAR(upperRight2.getLng(), 4, 0.001);

  std::string lit3 =
      "\"POLYGON(2 4, 4 4, 4 "
      "2, 2 2)\"^^<http://www.opengis.net/ont/geosparql#wktLiteral>";
  auto g3 = GeometryInfo::fromWktLiteral(lit3);
  ASSERT_EQ(g3.getWktType(), 3);
  ASSERT_NEAR(g3.getCentroid().getLat(), 3, 0.001);
  ASSERT_NEAR(g3.getCentroid().getLng(), 3, 0.001);
  auto [lowerLeft3, upperRight3] = g3.getBoundingBox();
  ASSERT_NEAR(lowerLeft3.getLat(), 2, 0.001);
  ASSERT_NEAR(lowerLeft3.getLng(), 2, 0.001);
  ASSERT_NEAR(upperRight3.getLat(), 4, 0.001);
  ASSERT_NEAR(upperRight3.getLng(), 4, 0.001);
}

// ____________________________________________________________________________
TEST(GeometryInfoTest, FromGeoPoint) {
  GeoPoint p{1.234, 5.678};
  auto g = GeometryInfo::fromGeoPoint(p);
  ASSERT_EQ(g.getWktType(), 1);
  ASSERT_NEAR(g.getCentroid().getLat(), 1.234, 0.001);
  ASSERT_NEAR(g.getCentroid().getLng(), 5.678, 0.001);
  ASSERT_NEAR(g.getBoundingBox().first.getLat(), 1.234, 0.001);
  ASSERT_NEAR(g.getBoundingBox().first.getLng(), 5.678, 0.001);
  ASSERT_NEAR(g.getBoundingBox().second.getLat(), 1.234, 0.001);
  ASSERT_NEAR(g.getBoundingBox().second.getLng(), 5.678, 0.001);

  GeoPoint p2{0, 0};
  auto g2 = GeometryInfo::fromGeoPoint(p2);
  ASSERT_EQ(g2.getWktType(), 1);
  ASSERT_NEAR(g2.getCentroid().getLng(), 0, 0.001);
  ASSERT_NEAR(g2.getCentroid().getLat(), 0, 0.001);
  ASSERT_NEAR(g2.getBoundingBox().first.getLat(), 0, 0.001);
  ASSERT_NEAR(g2.getBoundingBox().first.getLng(), 0, 0.001);
  ASSERT_NEAR(g2.getBoundingBox().second.getLat(), 0, 0.001);
  ASSERT_NEAR(g2.getBoundingBox().second.getLng(), 0, 0.001);
}

}  // namespace
