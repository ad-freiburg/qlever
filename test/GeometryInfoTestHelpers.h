// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
#define QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H

#include <gtest/gtest.h>
#include <util/geo/Geo.h>

#include "./printers/GeometryInfoPrinters.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/GeometryInfoHelpersImpl.h"
#include "util/GTestHelpers.h"
#include "util/OverloadCallOperator.h"
#include "util/TypeTraits.h"

namespace geoInfoTestHelpers {

using namespace ad_utility;
using namespace qlever;
using namespace ::testing;
using Loc = source_location;

using ad_utility::detail::AnyGeometryMember;
using ad_utility::detail::DAnyGeometry;
using ad_utility::detail::ParsedWkt;
using ad_utility::detail::ParseResult;
using ad_utility::detail::visitAnyGeometry;

// Helpers that check (approx.) equality of two GeometryInfo objects or for
// instances of the associated helper classes.
static constexpr double allowedCoordinateError = 0.001;

// ____________________________________________________________________________
inline auto geoPointNear =
    liftOptionalMatcher<GeoPoint>([](GeoPoint expected) -> Matcher<GeoPoint> {
      return AllOf(
          Property(&GeoPoint::getLat,
                   DoubleNear(expected.getLat(), allowedCoordinateError)),
          Property(&GeoPoint::getLng,
                   DoubleNear(expected.getLng(), allowedCoordinateError)));
    });
#define EXPECT_GEOPOINT_NEAR(a, b) EXPECT_THAT(a, geoPointNear(b))

// ____________________________________________________________________________
inline auto centroidNear = liftOptionalMatcher<qlever::Centroid>(
    [](qlever::Centroid expected) -> Matcher<qlever::Centroid> {
      return Property(&qlever::Centroid::centroid,
                      geoPointNear(expected.centroid()));
    });
#define EXPECT_CENTROID_NEAR(a, b) EXPECT_THAT(a, centroidNear(b))

// ____________________________________________________________________________
inline auto boundingBoxNear = liftOptionalMatcher<qlever::BoundingBox>(
    [](qlever::BoundingBox expected) -> Matcher<qlever::BoundingBox> {
      auto [lowerLeft, upperRight] = expected.pair();
      return AllOf(
          Property(&qlever::BoundingBox::lowerLeft, geoPointNear(lowerLeft)),
          Property(&qlever::BoundingBox::upperRight, geoPointNear(upperRight)));
    });
#define EXPECT_BOUNDINGBOX_NEAR(a, b) EXPECT_THAT(a, boundingBoxNear(b))

// ____________________________________________________________________________
inline auto metricLengthNear = liftOptionalMatcher<qlever::MetricLength>(
    [](qlever::MetricLength expected) -> Matcher<qlever::MetricLength> {
      // The metric length may be off by up to 1%
      auto allowedError = expected.length() * 0.01;
      return Property(&qlever::MetricLength::length,
                      DoubleNear(expected.length(), allowedError));
    });
#define EXPECT_METRICLENGTH_NEAR(a, b) EXPECT_THAT(a, metricLengthNear(b))

// ____________________________________________________________________________
inline auto metricAreaNear = liftOptionalMatcher<qlever::MetricArea>(
    [](qlever::MetricArea expected) -> Matcher<qlever::MetricArea> {
      // The metric area may be off by up to 1%
      auto allowedError = expected.area() * 0.01;
      return Property(&qlever::MetricArea::area,
                      DoubleNear(expected.area(), allowedError));
    });
#define EXPECT_METRICAREA_NEAR(a, b) EXPECT_THAT(a, metricAreaNear(b))

// ____________________________________________________________________________
inline auto geoInfoMatcher = liftOptionalMatcher<qlever::GeometryInfo>(
    [](qlever::GeometryInfo expected) -> Matcher<qlever::GeometryInfo> {
      return AllOf(Property(&qlever::GeometryInfo::getWktType,
                            Eq(expected.getWktType())),
                   Property(&qlever::GeometryInfo::getCentroid,
                            centroidNear(expected.getCentroid())),
                   Property(&qlever::GeometryInfo::getBoundingBox,
                            boundingBoxNear(expected.getBoundingBox())),
                   Property(&qlever::GeometryInfo::getNumGeometries,
                            Eq(expected.getNumGeometries())),
                   Property(&qlever::GeometryInfo::getMetricLength,
                            metricLengthNear(expected.getMetricLength())),
                   Property(&qlever::GeometryInfo::getMetricArea,
                            metricAreaNear(expected.getMetricArea())));
    });
#define EXPECT_GEOMETRYINFO(a, b) EXPECT_THAT(a, geoInfoMatcher(b))

// ____________________________________________________________________________
inline void checkRequestedInfoForInstance(
    std::optional<qlever::GeometryInfo> optGeoInfo,
    Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  auto l = generateLocationTrace(sourceLocation);

  EXPECT_GEOMETRYINFO(gi, gi.getRequestedInfo<qlever::GeometryInfo>());
  EXPECT_BOUNDINGBOX_NEAR(gi.getBoundingBox(),
                          gi.getRequestedInfo<qlever::BoundingBox>());
  EXPECT_CENTROID_NEAR(gi.getCentroid(),
                       gi.getRequestedInfo<qlever::Centroid>());
  EXPECT_EQ(std::optional<qlever::GeometryType>{gi.getWktType()},
            gi.getRequestedInfo<qlever::GeometryType>());
  EXPECT_EQ(gi.getNumGeometries(),
            gi.getRequestedInfo<qlever::NumGeometries>());
  EXPECT_METRICLENGTH_NEAR(gi.getMetricLength(),
                           gi.getRequestedInfo<qlever::MetricLength>());
  EXPECT_METRICAREA_NEAR(gi.getMetricArea(),
                         gi.getRequestedInfo<qlever::MetricArea>());
}

// ____________________________________________________________________________
inline void checkRequestedInfoForWktLiteral(
    const std::string_view& wkt, Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  auto optGeoInfo = qlever::GeometryInfo::fromWktLiteral(wkt);
  ASSERT_TRUE(optGeoInfo.has_value());
  auto gi = optGeoInfo.value();
  EXPECT_GEOMETRYINFO(
      gi, qlever::GeometryInfo::getRequestedInfo<qlever::GeometryInfo>(wkt));
  EXPECT_BOUNDINGBOX_NEAR(
      gi.getBoundingBox(),
      qlever::GeometryInfo::getRequestedInfo<qlever::BoundingBox>(wkt));
  EXPECT_CENTROID_NEAR(
      gi.getCentroid(),
      qlever::GeometryInfo::getRequestedInfo<qlever::Centroid>(wkt));
  EXPECT_EQ(std::optional<qlever::GeometryType>{gi.getWktType()},
            qlever::GeometryInfo::getRequestedInfo<qlever::GeometryType>(wkt));
  EXPECT_EQ(gi.getNumGeometries(),
            qlever::GeometryInfo::getRequestedInfo<qlever::NumGeometries>(wkt));
  EXPECT_METRICLENGTH_NEAR(
      gi.getMetricLength(),
      qlever::GeometryInfo::getRequestedInfo<qlever::MetricLength>(wkt));
  EXPECT_METRICAREA_NEAR(
      gi.getMetricArea(),
      qlever::GeometryInfo::getRequestedInfo<qlever::MetricArea>(wkt));
}

// ____________________________________________________________________________
inline void checkInvalidLiteral(std::string_view wkt,
                                bool expectValidGeometryType = false,
                                bool expectNumGeom = false,
                                Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);

  EXPECT_FALSE(qlever::GeometryInfo::fromWktLiteral(wkt).has_value());
  EXPECT_EQ(qlever::GeometryInfo::getWktType(wkt).has_value(),
            expectValidGeometryType);
  EXPECT_FALSE(qlever::GeometryInfo::getCentroid(wkt).has_value());
  EXPECT_FALSE(qlever::GeometryInfo::getBoundingBox(wkt).has_value());

  EXPECT_FALSE(qlever::GeometryInfo::getRequestedInfo<qlever::GeometryInfo>(wkt)
                   .has_value());
  EXPECT_EQ(qlever::GeometryInfo::getRequestedInfo<qlever::GeometryType>(wkt)
                .has_value(),
            expectValidGeometryType);
  EXPECT_FALSE(qlever::GeometryInfo::getRequestedInfo<qlever::Centroid>(wkt)
                   .has_value());
  EXPECT_FALSE(qlever::GeometryInfo::getRequestedInfo<qlever::BoundingBox>(wkt)
                   .has_value());
  EXPECT_EQ(qlever::GeometryInfo::getRequestedInfo<qlever::NumGeometries>(wkt)
                .has_value(),
            expectNumGeom);
}

// ____________________________________________________________________________
inline void checkUtilBoundingBox(::util::geo::DBox a, ::util::geo::DBox b,
                                 Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  auto l = generateLocationTrace(sourceLocation);
  ASSERT_NEAR(a.getLowerLeft().getX(), b.getLowerLeft().getX(), 0.001);
  ASSERT_NEAR(a.getLowerLeft().getY(), b.getLowerLeft().getY(), 0.001);
  ASSERT_NEAR(a.getUpperRight().getX(), b.getUpperRight().getX(), 0.001);
  ASSERT_NEAR(a.getUpperRight().getY(), b.getUpperRight().getY(), 0.001);
}

// Helpers to convert points and bounding boxes from double lat/lng to web
// mercator int32 representation used by libspatialjoin
inline ::util::geo::I32Point webMercProjFunc(const ::util::geo::DPoint& p) {
  auto projPoint = latLngToWebMerc(p);
  return {static_cast<int>(projPoint.getX() * PREC),
          static_cast<int>(projPoint.getY() * PREC)};
}
inline ::util::geo::I32Box boxToWebMerc(const ::util::geo::DBox& b) {
  return {webMercProjFunc(b.getLowerLeft()),
          webMercProjFunc(b.getUpperRight())};
}

// ____________________________________________________________________________
inline qlever::MetricLength getLengthForTesting(
    std::string_view quotedWktLiteral) {
  auto len = qlever::GeometryInfo::getMetricLength(quotedWktLiteral);
  if (!len.has_value()) {
    throw std::runtime_error("Cannot compute expected length");
  }
  return len.value();
}

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
inline qlever::MetricArea getAreaForTesting(const std::string_view wkt) {
  auto area = qlever::GeometryInfo::getMetricArea(wkt);
  if (!area.has_value()) {
    return MetricArea{std::numeric_limits<double>::quiet_NaN()};
  }
  return area.value();
}

// ____________________________________________________________________________
inline auto geoPointOrWktMatcher = liftOptionalMatcher<qlever::GeoPointOrWkt>(
    [](qlever::GeoPointOrWkt expected) -> Matcher<qlever::GeoPointOrWkt> {
      return std::visit(
          [&](auto& contained) -> Matcher<qlever::GeoPointOrWkt> {
            using T = std::decay_t<decltype(contained)>;
            if constexpr (std::is_same_v<T, GeoPoint>) {
              return VariantWith<GeoPoint>(
                  SafeMatcherCast<const GeoPoint&>(geoPointNear(contained)));
            } else {
              return VariantWith<std::string>(Eq(contained));
            }
          },
          expected);
    });

// In the following there are functions to generate gtest matchers for all of
// the geometry types supported by `pb_util`.
using namespace ::util::geo;

// ____________________________________________________________________________
inline auto utilPointNear = [](DPoint expected) -> Matcher<DPoint> {
  return AllOf(Property(&DPoint::getX,
                        DoubleNear(expected.getX(), allowedCoordinateError)),
               Property(&DPoint::getY,
                        DoubleNear(expected.getY(), allowedCoordinateError)));
};

// ____________________________________________________________________________
inline auto utilLineNear =
    liftMatcherToElementsAreArray<DPoint, DLine>(utilPointNear);

// ____________________________________________________________________________
inline auto utilMultiPointNear =
    liftMatcherToElementsAreArray<DPoint, DMultiPoint>(utilPointNear);

// ____________________________________________________________________________
inline auto utilMultiLineNear =
    liftMatcherToElementsAreArray<DLine, DMultiLine>(utilLineNear);

// ____________________________________________________________________________
inline auto utilPolygonNear = [](DPolygon expected) -> Matcher<DPolygon> {
  return AllOf(
      Property(&DPolygon::getOuter, utilLineNear(expected.getOuter())),
      Property(&DPolygon::getInners, utilMultiLineNear(expected.getInners())));
};

// ____________________________________________________________________________
inline auto utilMultiPolygonNear =
    liftMatcherToElementsAreArray<DPolygon, DMultiPolygon>(utilPolygonNear);

// Forward declaration for `utilAnyGeometryNear` below.
struct ParsedWktNearForwardDecl {
  Matcher<std::optional<ParsedWkt>> operator()(
      std::optional<ParsedWkt> expected) const;
};

// ____________________________________________________________________________
inline auto anyGeometryToParsedWkt = [](const DAnyGeometry& geom) {
  return visitAnyGeometry(
      [](const auto& contained) { return ParsedWkt{contained}; }, geom);
};

// ____________________________________________________________________________
inline auto utilAnyGeometryNear =
    [](DAnyGeometry expected) -> Matcher<DAnyGeometry> {
  return AllOf(
      Property(&DAnyGeometry::getType, Eq(expected.getType())),
      ResultOf(anyGeometryToParsedWkt,
               ParsedWktNearForwardDecl{}(anyGeometryToParsedWkt(expected))));
};

// ____________________________________________________________________________
inline auto utilCollectionNear =
    liftMatcherToElementsAreArray<DAnyGeometry, DCollection>(
        utilAnyGeometryNear);

// ____________________________________________________________________________
inline auto utilGeometryNear = OverloadCallOperator{
    utilPointNear,      utilLineNear,      utilPolygonNear,
    utilMultiPointNear, utilMultiLineNear, utilMultiPolygonNear,
    utilCollectionNear};

// ____________________________________________________________________________
inline auto parsedWktNear = liftOptionalMatcher<ParsedWkt>(
    [](ParsedWkt expected) -> Matcher<ParsedWkt> {
      return std::visit(
          [](const auto& contained) -> Matcher<ParsedWkt> {
            using T = std::decay_t<decltype(contained)>;
            return VariantWith<T>(
                SafeMatcherCast<const T&>(utilGeometryNear(contained)));
          },
          std::move(expected));
    });

// ____________________________________________________________________________
inline Matcher<std::optional<ParsedWkt>> ParsedWktNearForwardDecl::operator()(
    std::optional<ParsedWkt> expected) const {
  return parsedWktNear(std::move(expected));
}

// ____________________________________________________________________________
inline auto parseResultNear = liftOptionalMatcher<ParseResult>(
    [](ParseResult expected) -> Matcher<ParseResult> {
      return Pair(Eq(expected.first), parsedWktNear(expected.second));
    });

// ____________________________________________________________________________
struct ExpectedGeometryN {
  size_t n_;
  std::string wkt_;
  std::optional<ParsedWkt> expected_;
};

};  // namespace geoInfoTestHelpers

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
