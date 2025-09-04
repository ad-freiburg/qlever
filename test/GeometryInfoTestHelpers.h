// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
#define QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H

#include <gtest/gtest.h>

#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GTestHelpers.h"

namespace geoInfoTestHelpers {

using namespace ad_utility;
using Loc = source_location;

// Helpers that assert (approx.) equality of two GeometryInfo objects or for
// instances of the associated helper classes.

// ____________________________________________________________________________
inline void checkGeometryType(std::optional<GeometryType> a,
                              std::optional<GeometryType> b,
                              Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  ASSERT_EQ(a.value().type_, b.value().type_);
}

// ____________________________________________________________________________
inline void checkCentroid(std::optional<Centroid> a, std::optional<Centroid> b,
                          Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  ASSERT_NEAR(a.value().centroid_.getLat(), b.value().centroid_.getLat(),
              0.001);
  ASSERT_NEAR(a.value().centroid_.getLng(), b.value().centroid_.getLng(),
              0.001);
}

// ____________________________________________________________________________
inline void checkBoundingBox(std::optional<BoundingBox> a,
                             std::optional<BoundingBox> b,
                             Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  auto [all, aur] = a.value();
  auto [bll, bur] = b.value();
  ASSERT_NEAR(all.getLat(), bll.getLat(), 0.001);
  ASSERT_NEAR(all.getLng(), bll.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
}

// ____________________________________________________________________________
inline void checkGeoInfo(std::optional<GeometryInfo> actual,
                         std::optional<GeometryInfo> expected,
                         Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(actual.has_value(), expected.has_value());
  if (!actual.has_value() || !expected.has_value()) {
    return;
  }

  auto a = actual.value();
  auto b = expected.value();

  checkGeometryType(a.getWktType(), b.getWktType());

  checkCentroid(a.getCentroid(), b.getCentroid());

  checkBoundingBox(a.getBoundingBox(), b.getBoundingBox());
}

// ____________________________________________________________________________
inline void checkRequestedInfoForInstance(
    std::optional<GeometryInfo> optGeoInfo,
    Loc sourceLocation = Loc::current()) {
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  auto l = generateLocationTrace(sourceLocation);
  checkGeoInfo(gi, gi.getRequestedInfo<GeometryInfo>());
  checkBoundingBox(gi.getBoundingBox(), gi.getRequestedInfo<BoundingBox>(),
                   sourceLocation);
  checkCentroid(gi.getCentroid(), gi.getRequestedInfo<Centroid>(),
                sourceLocation);
  checkGeometryType(gi.getWktType(), gi.getRequestedInfo<GeometryType>(),
                    sourceLocation);
}

// ____________________________________________________________________________
inline void checkRequestedInfoForWktLiteral(
    const std::string_view& wkt, Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  auto optGeoInfo = GeometryInfo::fromWktLiteral(wkt);
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  checkGeoInfo(gi, GeometryInfo::getRequestedInfo<GeometryInfo>(wkt));
  checkBoundingBox(gi.getBoundingBox(),
                   GeometryInfo::getRequestedInfo<BoundingBox>(wkt));
  checkCentroid(gi.getCentroid(),
                GeometryInfo::getRequestedInfo<Centroid>(wkt));
  checkGeometryType(gi.getWktType(),
                    GeometryInfo::getRequestedInfo<GeometryType>(wkt));
}

// ____________________________________________________________________________
inline void checkInvalidLiteral(std::string_view wkt,
                                bool expectValidGeometryType = false,
                                Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);

  EXPECT_FALSE(GeometryInfo::fromWktLiteral(wkt).has_value());
  EXPECT_EQ(GeometryInfo::getWktType(wkt).has_value(), expectValidGeometryType);
  EXPECT_FALSE(GeometryInfo::getCentroid(wkt).has_value());
  EXPECT_FALSE(GeometryInfo::getBoundingBox(wkt).has_value());

  EXPECT_FALSE(GeometryInfo::getRequestedInfo<GeometryInfo>(wkt).has_value());
  EXPECT_EQ(GeometryInfo::getRequestedInfo<GeometryType>(wkt).has_value(),
            expectValidGeometryType);
  EXPECT_FALSE(GeometryInfo::getRequestedInfo<Centroid>(wkt).has_value());
  EXPECT_FALSE(GeometryInfo::getRequestedInfo<BoundingBox>(wkt).has_value());
}

// ____________________________________________________________________________
inline void checkUtilBoundingBox(util::geo::DBox a, util::geo::DBox b,
                                 Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_NEAR(a.getLowerLeft().getX(), b.getLowerLeft().getX(), 0.001);
  ASSERT_NEAR(a.getLowerLeft().getY(), b.getLowerLeft().getY(), 0.001);
  ASSERT_NEAR(a.getUpperRight().getX(), b.getUpperRight().getX(), 0.001);
  ASSERT_NEAR(a.getUpperRight().getY(), b.getUpperRight().getY(), 0.001);
}

// Helpers to convert points and bounding boxes from double lat/lng to web
// mercator int32 representation used by libspatialjoin
inline util::geo::I32Point webMercProjFunc(const util::geo::DPoint& p) {
  auto projPoint = latLngToWebMerc(p);
  return {static_cast<int>(projPoint.getX() * PREC),
          static_cast<int>(projPoint.getY() * PREC)};
}
inline util::geo::I32Box boxToWebMerc(const util::geo::DBox& b) {
  return {webMercProjFunc(b.getLowerLeft()),
          webMercProjFunc(b.getUpperRight())};
}

};  // namespace geoInfoTestHelpers

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
