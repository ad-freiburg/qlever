// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
#define QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H

#include <gtest/gtest.h>

#include "./printers/GeometryInfoPrinters.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GTestHelpers.h"

namespace geoInfoTestHelpers {

using namespace ad_utility;
using Loc = source_location;

// Helpers that check (approx.) equality of two GeometryInfo objects or for
// instances of the associated helper classes.

// Convert to optional explicitly because arguments to matcher are not
// strongly typed.
#define AD_GEOINFO_TEST_OPTIONAL_HELPER(ContainedType) \
  using namespace ::testing;                           \
  std::optional<ContainedType> expected = expectedRaw; \
  std::optional<ContainedType> actual = arg;           \
  if (!actual.has_value()) {                           \
    return !expected.has_value();                      \
  } else if (!expected.has_value()) {                  \
    return false;                                      \
  }

// ____________________________________________________________________________
inline std::string description(auto& expected) {
  return absl::StrCat("Is approximately equal to ",
                      ::testing::PrintToString(expected));
}

// ____________________________________________________________________________
MATCHER_P(GeoPointNear, expectedRaw, description(expectedRaw)) {
  AD_GEOINFO_TEST_OPTIONAL_HELPER(GeoPoint);
  return ExplainMatchResult(
      AllOf(Property(&GeoPoint::getLat,
                     DoubleNear(expected.value().getLat(), 0.001)),
            Property(&GeoPoint::getLng,
                     DoubleNear(expected.value().getLng(), 0.001))),
      actual.value(), result_listener);
}
#define EXPECT_GEOPOINT_NEAR(a, b) EXPECT_THAT(a, GeoPointNear(b))

// ____________________________________________________________________________
MATCHER_P(CentroidNear, expectedRaw, description(expectedRaw)) {
  AD_GEOINFO_TEST_OPTIONAL_HELPER(Centroid);
  return ExplainMatchResult(
      Property(&Centroid::centroid, GeoPointNear(expected.value().centroid())),
      actual.value(), result_listener);
}
#define EXPECT_CENTROID_NEAR(a, b) EXPECT_THAT(a, CentroidNear(b))

// ____________________________________________________________________________
MATCHER_P(BoundingBoxNear, expectedRaw, description(expectedRaw)) {
  AD_GEOINFO_TEST_OPTIONAL_HELPER(BoundingBox);
  auto [lowerLeft, upperRight] = expected.value().pair();
  return ExplainMatchResult(
      AllOf(Property(&BoundingBox::lowerLeft, GeoPointNear(lowerLeft)),
            Property(&BoundingBox::upperRight, GeoPointNear(upperRight))),
      actual.value(), result_listener);
}
#define EXPECT_BOUNDINGBOX_NEAR(a, b) EXPECT_THAT(a, BoundingBoxNear(b))

// ____________________________________________________________________________
MATCHER_P(MetricLengthNear, expectedRaw, description(expectedRaw)) {
  AD_GEOINFO_TEST_OPTIONAL_HELPER(MetricLength);
  auto allowedError = expected.value().length() * 0.01;
  return ExplainMatchResult(
      Property(&MetricLength::length,
               DoubleNear(expected.value().length(), allowedError)),
      actual.value(), result_listener);
}
#define EXPECT_METRICLENGTH_NEAR(a, b) EXPECT_THAT(a, MetricLengthNear(b))

// ____________________________________________________________________________
MATCHER_P(MetricAreaNear, expectedRaw, description(expectedRaw)) {
  AD_GEOINFO_TEST_OPTIONAL_HELPER(MetricArea);
  auto allowedError = expected.value().area() * 0.01;
  return ExplainMatchResult(
      Property(&MetricArea::area,
               DoubleNear(expected.value().area(), allowedError)),
      actual.value(), result_listener);
}
#define EXPECT_METRICAREA_NEAR(a, b) EXPECT_THAT(a, MetricAreaNear(b))

// ____________________________________________________________________________
MATCHER_P(GeoInfoMatcher, expectedRaw, description(expectedRaw)) {
  AD_GEOINFO_TEST_OPTIONAL_HELPER(GeometryInfo);
  auto geoInfo = expected.value();
  auto inner = Optional(AllOf(
      Property(&GeometryInfo::getWktType, Eq(geoInfo.getWktType())),
      Property(&GeometryInfo::getCentroid,
               Property(&Centroid::centroid,
                        GeoPointNear(geoInfo.getCentroid().centroid()))),
      Property(&GeometryInfo::getBoundingBox,
               BoundingBoxNear(geoInfo.getBoundingBox())),
      Property(&GeometryInfo::getNumGeometries, Eq(geoInfo.getNumGeometries())),
      Property(&GeometryInfo::getMetricLength,
               MetricLengthNear(geoInfo.getMetricLength())),
      Property(&GeometryInfo::getMetricArea,
               MetricAreaNear(geoInfo.getMetricArea()))));
  return ::testing::ExplainMatchResult(inner, actual, result_listener);
}
#define EXPECT_GEOMETRYINFO(a, b) EXPECT_THAT(a, GeoInfoMatcher(b))

// ____________________________________________________________________________
inline void checkRequestedInfoForInstance(
    std::optional<GeometryInfo> optGeoInfo,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  auto l = generateLocationTrace(sourceLocation);

  EXPECT_GEOMETRYINFO(gi, gi.getRequestedInfo<GeometryInfo>());
  EXPECT_BOUNDINGBOX_NEAR(gi.getBoundingBox(),
                          gi.getRequestedInfo<BoundingBox>());
  EXPECT_CENTROID_NEAR(gi.getCentroid(), gi.getRequestedInfo<Centroid>());
  EXPECT_EQ(std::optional<GeometryType>{gi.getWktType()},
            gi.getRequestedInfo<GeometryType>());
  EXPECT_EQ(gi.getNumGeometries(), gi.getRequestedInfo<NumGeometries>());
  EXPECT_METRICLENGTH_NEAR(gi.getMetricLength(),
                           gi.getRequestedInfo<MetricLength>());
  EXPECT_METRICAREA_NEAR(gi.getMetricArea(), gi.getRequestedInfo<MetricArea>());
}

// ____________________________________________________________________________
inline void checkRequestedInfoForWktLiteral(
    const std::string_view& wkt, Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  auto optGeoInfo = GeometryInfo::fromWktLiteral(wkt);
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  EXPECT_GEOMETRYINFO(gi, GeometryInfo::getRequestedInfo<GeometryInfo>(wkt));
  EXPECT_BOUNDINGBOX_NEAR(gi.getBoundingBox(),
                          GeometryInfo::getRequestedInfo<BoundingBox>(wkt));
  EXPECT_CENTROID_NEAR(gi.getCentroid(),
                       GeometryInfo::getRequestedInfo<Centroid>(wkt));
  EXPECT_EQ(std::optional<GeometryType>{gi.getWktType()},
            GeometryInfo::getRequestedInfo<GeometryType>(wkt));
  EXPECT_EQ(gi.getNumGeometries(),
            GeometryInfo::getRequestedInfo<NumGeometries>(wkt));
  EXPECT_METRICLENGTH_NEAR(gi.getMetricLength(),
                           GeometryInfo::getRequestedInfo<MetricLength>(wkt));
  EXPECT_METRICAREA_NEAR(gi.getMetricArea(),
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
inline void checkGeoPoint(std::optional<GeoPoint> actual,
                          std::optional<GeoPoint> expected,
                          Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(actual.has_value(), expected.has_value());
  if (!actual.has_value()) {
    return;
  }
  ASSERT_NEAR(actual.value().getLat(), expected.value().getLat(), 0.0001);
  ASSERT_NEAR(actual.value().getLng(), expected.value().getLng(), 0.0001);
}

// ____________________________________________________________________________
inline void checkGeoPointOrWkt(std::optional<GeoPointOrWkt> actual,
                               std::optional<GeoPointOrWkt> expected,
                               Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_EQ(actual.has_value(), expected.has_value());
  if (!actual.has_value()) {
    return;
  }
  std::visit(
      [](const auto& a, const auto& b) {
        using Ta = std::decay_t<decltype(a)>;
        using Tb = std::decay_t<decltype(b)>;
        if constexpr (std::is_same_v<Ta, GeoPoint> &&
                      std::is_same_v<Tb, GeoPoint>) {
          checkGeoPoint(a, b);
        } else if constexpr (std::is_same_v<Ta, std::string> &&
                             std::is_same_v<Tb, std::string>) {
          ASSERT_EQ(a, b);
        } else {
          ASSERT_FALSE(true)
              << "Actual and expected `GeoPointOrWktValueGetter` results have "
                 "different types";
        }
      },
      actual.value(), expected.value());
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

// ____________________________________________________________________________
MATCHER_P(GeoPointOrWktMatcher, expectedRaw, description(expectedRaw)) {
  AD_GEOINFO_TEST_OPTIONAL_HELPER(GeoPointOrWkt);
  return ::testing::ExplainMatchResult(
      std::visit(
          [&](auto& contained) -> Matcher<std::optional<GeoPointOrWkt>> {
            using T = std::decay_t<decltype(contained)>;
            if constexpr (std::is_same_v<T, GeoPoint>) {
              return Optional(VariantWith<GeoPoint>(GeoPointNear(contained)));
            } else {
              return Optional(VariantWith<std::string>(Eq(contained)));
            }
          },
          expected.value()),
      actual, result_listener);
}

};  // namespace geoInfoTestHelpers

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
