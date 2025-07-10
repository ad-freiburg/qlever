// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
#define QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H

#include <gtest/gtest.h>

#include "rdfTypes/GeometryInfo.h"
#include "util/GTestHelpers.h"

namespace geoInfoTestHelpers {

using namespace ad_utility;
using Loc = source_location;

// ____________________________________________________________________________
inline void checkGeometryType(GeometryType a, GeometryType b,
                              Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.type(), b.type());
}

// ____________________________________________________________________________
inline void checkCentroid(Centroid a, Centroid b,
                          Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_NEAR(a.centroid().getLat(), b.centroid().getLat(), 0.001);
  ASSERT_NEAR(a.centroid().getLng(), b.centroid().getLng(), 0.001);
}

// ____________________________________________________________________________
inline void checkBoundingBox(BoundingBox a, BoundingBox b,
                             Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  auto [all, aur] = a.pair();
  auto [bll, bur] = b.pair();
  ASSERT_NEAR(all.getLat(), bll.getLat(), 0.001);
  ASSERT_NEAR(all.getLng(), bll.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
}

// ____________________________________________________________________________
inline void checkMetricLength(MetricLength a, MetricLength b,
                              Loc sourceLocation = Loc::current()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_NEAR(a.length(), b.length(),
              // The metric length may be off by up to 1%
              0.01 * a.length());
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

  checkMetricLength(a.getMetricLength(), b.getMetricLength());
}

// ____________________________________________________________________________
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
  checkMetricLength(gi.getMetricLength(), gi.getRequestedInfo<MetricLength>());
}

// ____________________________________________________________________________
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
  checkMetricLength(gi.getMetricLength(),
                    GeometryInfo::getRequestedInfo<MetricLength>(wkt));
}

};  // namespace geoInfoTestHelpers

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
