// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
#define QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H

#include <gtest/gtest.h>

#include "util/GTestHelpers.h"
#include "util/GeometryInfo.h"

namespace geoInfoTestHelpers {

using namespace ad_utility;
using Loc = source_location;

inline void checkGeometryType(GeometryType a, GeometryType b,
                              Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.type_, b.type_);
}

inline void checkCentroid(Centroid a, Centroid b,
                          Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_NEAR(a.centroid_.getLat(), b.centroid_.getLat(), 0.001);
  ASSERT_NEAR(a.centroid_.getLng(), b.centroid_.getLng(), 0.001);
}

inline void checkBoundingBox(BoundingBox a, BoundingBox b,
                             Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  auto [all, aur] = a;
  auto [bll, bur] = b;
  ASSERT_NEAR(all.getLat(), bll.getLat(), 0.001);
  ASSERT_NEAR(all.getLng(), bll.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
}

// Helper that asserts (approx.) equality of two GeometryInfo objects
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

inline void checkRequestedInfoForInstance(GeometryInfo gi,
                                          Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  checkGeoInfo(gi, gi.getRequestedInfo<GeometryInfo>());
  checkBoundingBox(gi.getBoundingBox(), gi.getRequestedInfo<BoundingBox>(),
                   sourceLocation);
  checkCentroid(gi.getCentroid(), gi.getRequestedInfo<Centroid>(),
                sourceLocation);
  checkGeometryType(gi.getWktType(), gi.getRequestedInfo<GeometryType>(),
                    sourceLocation);
}

inline void checkRequestedInfoForWktLiteral(
    const std::string_view& wkt, Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  auto gi = GeometryInfo::fromWktLiteral(wkt);
  checkGeoInfo(gi, GeometryInfo::getRequestedInfo<GeometryInfo>(wkt));
  checkBoundingBox(gi.getBoundingBox(),
                   GeometryInfo::getRequestedInfo<BoundingBox>(wkt));
  checkCentroid(gi.getCentroid(),
                GeometryInfo::getRequestedInfo<Centroid>(wkt));
  checkGeometryType(gi.getWktType(),
                    GeometryInfo::getRequestedInfo<GeometryType>(wkt));
}

};  // namespace geoInfoTestHelpers

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
