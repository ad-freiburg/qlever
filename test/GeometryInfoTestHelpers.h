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
inline void checkCentroid(std::optional<Centroid> a, std::optional<Centroid> b,
                          Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  ASSERT_NEAR(a.value().centroid().getLat(), b.value().centroid().getLat(),
              0.001);
  ASSERT_NEAR(a.value().centroid().getLng(), b.value().centroid().getLng(),
              0.001);
}

// ____________________________________________________________________________
inline void checkBoundingBox(std::optional<BoundingBox> a,
                             std::optional<BoundingBox> b,
                             Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  auto [all, aur] = a.value().pair();
  auto [bll, bur] = b.value().pair();
  ASSERT_NEAR(all.getLat(), bll.getLat(), 0.001);
  ASSERT_NEAR(all.getLng(), bll.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
  ASSERT_NEAR(aur.getLng(), bur.getLng(), 0.001);
}

// ____________________________________________________________________________
inline void checkMetricLength(std::optional<MetricLength> a,
                              std::optional<MetricLength> b,
                              Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  ASSERT_NEAR(a.value().length(), b.value().length(),
              // The metric length may be off by up to 1%
              0.01 * a.value().length());
}

// ____________________________________________________________________________
inline void checkMetricArea(std::optional<MetricArea> a,
                            std::optional<MetricArea> b,
                            Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(a.has_value(), b.has_value());
  if (!a.has_value()) {
    return;
  }
  ASSERT_NEAR(a.value().area(), b.value().area(),
              // The metric area may be off by up to 1%
              0.01 * a.value().area());
}

// ____________________________________________________________________________
inline void checkGeoInfo(std::optional<GeometryInfo> actual,
                         std::optional<GeometryInfo> expected,
                         Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(actual.has_value(), expected.has_value());
  if (!actual.has_value() || !expected.has_value()) {
    return;
  }

  auto a = actual.value();
  auto b = expected.value();

  EXPECT_EQ(a.getWktType(), b.getWktType());

  checkCentroid(a.getCentroid(), b.getCentroid());

  checkBoundingBox(a.getBoundingBox(), b.getBoundingBox());

  EXPECT_EQ(a.getNumGeometries(), b.getNumGeometries());

  checkMetricLength(a.getMetricLength(), b.getMetricLength());

  checkMetricArea(a.getMetricArea(), b.getMetricArea());
}

// ____________________________________________________________________________
inline void checkRequestedInfoForInstance(
    std::optional<GeometryInfo> optGeoInfo,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  auto l = generateLocationTrace(sourceLocation);

  checkGeoInfo(gi, gi.getRequestedInfo<GeometryInfo>());
  checkBoundingBox(gi.getBoundingBox(), gi.getRequestedInfo<BoundingBox>(),
                   sourceLocation);
  checkCentroid(gi.getCentroid(), gi.getRequestedInfo<Centroid>(),
                sourceLocation);
  EXPECT_EQ(std::optional<GeometryType>{gi.getWktType()},
            gi.getRequestedInfo<GeometryType>());
  EXPECT_EQ(gi.getNumGeometries(), gi.getRequestedInfo<NumGeometries>());
  checkMetricLength(gi.getMetricLength(), gi.getRequestedInfo<MetricLength>());
  checkMetricArea(gi.getMetricArea(), gi.getRequestedInfo<MetricArea>());
}

// ____________________________________________________________________________
inline void checkRequestedInfoForWktLiteral(
    const std::string_view& wkt, Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  auto optGeoInfo = GeometryInfo::fromWktLiteral(wkt);
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  checkGeoInfo(gi, GeometryInfo::getRequestedInfo<GeometryInfo>(wkt));
  checkBoundingBox(gi.getBoundingBox(),
                   GeometryInfo::getRequestedInfo<BoundingBox>(wkt));
  checkCentroid(gi.getCentroid(),
                GeometryInfo::getRequestedInfo<Centroid>(wkt));
  EXPECT_EQ(std::optional<GeometryType>{gi.getWktType()},
            GeometryInfo::getRequestedInfo<GeometryType>(wkt));
  EXPECT_EQ(gi.getNumGeometries(),
            GeometryInfo::getRequestedInfo<NumGeometries>(wkt));
  checkMetricLength(gi.getMetricLength(),
                    GeometryInfo::getRequestedInfo<MetricLength>(wkt));
  checkMetricArea(gi.getMetricArea(),
                  GeometryInfo::getRequestedInfo<MetricArea>(wkt));
}

// ____________________________________________________________________________
inline void checkInvalidLiteral(std::string_view wkt,
                                bool expectValidGeometryType = false,
                                bool expectNumGeom = false,
                                Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
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
  EXPECT_EQ(GeometryInfo::getRequestedInfo<NumGeometries>(wkt).has_value(),
            expectNumGeom);
}

// ____________________________________________________________________________
inline void checkUtilBoundingBox(util::geo::DBox a, util::geo::DBox b,
                                 Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
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

// ____________________________________________________________________________
inline MetricLength getLengthForTesting(std::string_view quotedWktLiteral) {
  auto len = ad_utility::GeometryInfo::getMetricLength(quotedWktLiteral);
  if (!len.has_value()) {
    throw std::runtime_error("Cannot compute expected length");
  }
  return len.value();
}

using DAnyGeometry = util::geo::AnyGeometry<double>;
using ad_utility::detail::AnyGeometryMember;

// ____________________________________________________________________________
inline void checkAnyGeometryMemberEnum(
    DAnyGeometry geom, AnyGeometryMember enumVal,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  EXPECT_EQ(geom.getType(), static_cast<uint8_t>(enumVal));
}

// ____________________________________________________________________________
template <typename T>
inline T getGeometryOfTypeOrThrow(
    const std::string_view wkt, Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  using namespace ad_utility::detail;
  auto l = generateLocationTrace(sourceLocation);
  auto parseRes = parseWkt(wkt);
  if (!parseRes.second.has_value()) {
    throw std::runtime_error("Could not parse wkt literal");
  }
  return std::visit(
      [](const auto& parsed) -> T {
        using V = std::decay_t<decltype(parsed)>;
        if constexpr (std::is_same_v<V, T>) {
          return parsed;
        } else {
          throw std::runtime_error("Wrong geometry type of parse result");
        }
      },
      parseRes.second.value());
}

// ____________________________________________________________________________
template <typename T>
inline void testMetricArea(const std::string_view wkt, double expectedArea,
                           size_t expectedNumPolygons = 0,
                           Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  using namespace ad_utility::detail;
  auto l = generateLocationTrace(sourceLocation);
  const auto parsed = getGeometryOfTypeOrThrow<T>(wkt);

  // One percent deviation from expected area is ok
  const double allowedError = 0.01 * expectedArea;

  if constexpr (std::is_same_v<T, Collection<CoordType>>) {
    EXPECT_EQ(collectionToS2Polygons(parsed).size(), expectedNumPolygons);
  }
  EXPECT_NEAR(computeMetricArea(parsed), expectedArea, allowedError);
  EXPECT_NEAR(computeMetricArea(ParsedWkt{parsed}), expectedArea, allowedError);
}

// ____________________________________________________________________________
inline MetricArea getAreaForTesting(const std::string_view wkt) {
  auto area = GeometryInfo::getMetricArea(wkt);
  if (!area.has_value()) {
    return MetricArea{std::numeric_limits<double>::quiet_NaN()};
  }
  return area.value();
}

};  // namespace geoInfoTestHelpers

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
