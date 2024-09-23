//  Copyright 2024, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Christoph Ullinger <ullingec@informatik.uni-freiburg.de>

#include <gtest/gtest.h>

#include "parser/GeoPoint.h"

TEST(GeoPoint, GeoPoint) {
  GeoPoint g = GeoPoint(70.5, 130.2);

  ASSERT_FLOAT_EQ(g.getLat(), 70.5);
  ASSERT_FLOAT_EQ(g.getLng(), 130.2);

  ASSERT_THROW(GeoPoint(-99.5, 1.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(99.5, 1.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(9.5, -185.0), CoordinateOutOfRangeException);
  ASSERT_THROW(GeoPoint(9.5, 185.0), CoordinateOutOfRangeException);
  ASSERT_NO_THROW(GeoPoint(90, 180));
  ASSERT_NO_THROW(GeoPoint(0, 1.0));
  ASSERT_NO_THROW(GeoPoint(1.0, -180.0));
  ASSERT_NO_THROW(GeoPoint(0, 0));

  g = GeoPoint(0, 0);
  ASSERT_FLOAT_EQ(g.getLat(), 0);
  ASSERT_FLOAT_EQ(g.getLng(), 0);
  g = GeoPoint(90, 180);
  ASSERT_FLOAT_EQ(g.getLat(), 90);
  ASSERT_FLOAT_EQ(g.getLng(), 180);
  g = GeoPoint(-90, -180);
  ASSERT_FLOAT_EQ(g.getLat(), -90);
  ASSERT_FLOAT_EQ(g.getLng(), -180);
  g = GeoPoint(-90, 180);
  ASSERT_FLOAT_EQ(g.getLat(), -90);
  ASSERT_FLOAT_EQ(g.getLng(), 180);
  g = GeoPoint(90, -180);
  ASSERT_FLOAT_EQ(g.getLat(), 90);
  ASSERT_FLOAT_EQ(g.getLng(), -180);
  g = GeoPoint(0, 180);
  ASSERT_FLOAT_EQ(g.getLat(), 0);
  ASSERT_FLOAT_EQ(g.getLng(), 180);
  g = GeoPoint(90, 0);
  ASSERT_FLOAT_EQ(g.getLat(), 90);
  ASSERT_FLOAT_EQ(g.getLng(), 0);
}

TEST(GeoPoint, string) {
  GeoPoint g = GeoPoint(-70.5, -130.2);

  // TODO<ullingerc>
  ASSERT_EQ(g.toStringRepresentation(), "POINT(-130.2 -70.5)");

  auto strpair = g.toStringAndType();
  ASSERT_EQ(strpair.first, "POINT(-130.2 -70.5)");
  ASSERT_EQ(strpair.second, "http://www.opengis.net/ont/geosparql#wktLiteral");
}

TEST(GeoPoint, toBitRepresentation) {
  // TODO<ullingerc>
  GeoPoint g = GeoPoint(-70.5, -130.2);

  g = GeoPoint(90, 180);

  g = GeoPoint(0, 0);
}

TEST(GeoPoint, fromBitRepresentation) {
  // TODO<ullingerc>
  GeoPoint g = GeoPoint(-70.5, -130.2);

  g = GeoPoint(90, 180);

  g = GeoPoint(0, 0);
}

TEST(GeoPoint, parseFromLiteral) {
  // TODO<ullingerc>
  GeoPoint g = GeoPoint(-70.5, -130.2);

  g = GeoPoint(90, 180);

  g = GeoPoint(0, 0);
}
