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
inline auto centroidNear =
    liftOptionalMatcher<Centroid>([](Centroid expected) -> Matcher<Centroid> {
      return Property(&Centroid::centroid, geoPointNear(expected.centroid()));
    });
#define EXPECT_CENTROID_NEAR(a, b) EXPECT_THAT(a, centroidNear(b))

// ____________________________________________________________________________
inline auto boundingBoxNear = liftOptionalMatcher<BoundingBox>(
    [](BoundingBox expected) -> Matcher<BoundingBox> {
      auto [lowerLeft, upperRight] = expected.pair();
      return AllOf(
          Property(&BoundingBox::lowerLeft, geoPointNear(lowerLeft)),
          Property(&BoundingBox::upperRight, geoPointNear(upperRight)));
    });
#define EXPECT_BOUNDINGBOX_NEAR(a, b) EXPECT_THAT(a, boundingBoxNear(b))

// ____________________________________________________________________________
inline auto metricLengthNear = liftOptionalMatcher<MetricLength>(
    [](MetricLength expected) -> Matcher<MetricLength> {
      // The metric length may be off by up to 1%
      auto allowedError = expected.length() * 0.01;
      return Property(&MetricLength::length,
                      DoubleNear(expected.length(), allowedError));
    });
#define EXPECT_METRICLENGTH_NEAR(a, b) EXPECT_THAT(a, metricLengthNear(b))

// ____________________________________________________________________________
inline auto metricAreaNear = liftOptionalMatcher<MetricArea>(
    [](MetricArea expected) -> Matcher<MetricArea> {
      // The metric area may be off by up to 1%
      auto allowedError = expected.area() * 0.01;
      return Property(&MetricArea::area,
                      DoubleNear(expected.area(), allowedError));
    });
#define EXPECT_METRICAREA_NEAR(a, b) EXPECT_THAT(a, metricAreaNear(b))

// ____________________________________________________________________________
inline auto geoInfoMatcher = liftOptionalMatcher<GeometryInfo>(
    [](GeometryInfo expected) -> Matcher<GeometryInfo> {
      return AllOf(
          Property(&GeometryInfo::getWktType, Eq(expected.getWktType())),
          Property(&GeometryInfo::getCentroid,
                   centroidNear(expected.getCentroid())),
          Property(&GeometryInfo::getBoundingBox,
                   boundingBoxNear(expected.getBoundingBox())),
          Property(&GeometryInfo::getNumGeometries,
                   Eq(expected.getNumGeometries())),
          Property(&GeometryInfo::getMetricLength,
                   metricLengthNear(expected.getMetricLength())),
          Property(&GeometryInfo::getMetricArea,
                   metricAreaNear(expected.getMetricArea())),
          Property(&GeometryInfo::getSourceCrsType,
                   Eq(expected.getSourceCrsType())),
          Property(&GeometryInfo::getSerializationType,
                   Eq(expected.getSerializationType())));
    });
#define EXPECT_GEOMETRYINFO(a, b) EXPECT_THAT(a, geoInfoMatcher(b))

// Return a matcher that applies `matcher` to the result of
// `getRequestedInfo<T>` for a `GeometryInfo` instance or a WKT literal (type
// `Input`).
template <typename T, typename Input, typename M>
auto requestedInfoIs(M matcher) {
  return ResultOf(
      "`getRequestedInfo`",
      [](const Input& input) {
        if constexpr (std::is_same_v<Input, GeometryInfo>) {
          return input.template getRequestedInfo<T>();
        } else {
          return GeometryInfo::getRequestedInfo<T>(input);
        }
      },
      matcher);
}

// Return a matcher for a `GeometryInfo` instance or a WKT literal (type
// `Input`) for which `getRequestedInfo` returns the respective part of
// `expected` for each type of requested info.
template <typename Input>
Matcher<Input> requestedInfoMatches(const GeometryInfo& expected) {
  return AllOf(
      requestedInfoIs<GeometryInfo, Input>(geoInfoMatcher(expected)),
      requestedInfoIs<BoundingBox, Input>(
          boundingBoxNear(expected.getBoundingBox())),
      requestedInfoIs<Centroid, Input>(centroidNear(expected.getCentroid())),
      requestedInfoIs<GeometryType, Input>(Optional(expected.getWktType())),
      requestedInfoIs<NumGeometries, Input>(
          Optional(expected.getNumGeometries())),
      requestedInfoIs<MetricLength, Input>(
          metricLengthNear(expected.getMetricLength())),
      requestedInfoIs<MetricArea, Input>(
          metricAreaNear(expected.getMetricArea())),
      requestedInfoIs<SourceCrsType, Input>(
          Optional(expected.getSourceCrsType())),
      requestedInfoIs<SerializationType, Input>(
          Optional(expected.getSerializationType())));
}

// Return a matcher for a WKT literal for which neither a `GeometryInfo` nor a
// centroid or bounding box can be computed. The geometry type and the number
// of geometries can be computed iff `expectValidGeometryType` and
// `expectNumGeom` are set, respectively.
inline Matcher<std::string_view> isInvalidWktLiteral(
    bool expectValidGeometryType = false, bool expectNumGeom = false) {
  using GI = GeometryInfo;
  using SV = std::string_view;
  auto hasValue = [](bool expected) {
    return ResultOf(
        "`has_value()`", [](const auto& opt) { return opt.has_value(); },
        expected);
  };
  return AllOf(
      ResultOf(
          "`fromWktLiteral`", [](SV wkt) { return GI::fromWktLiteral(wkt); },
          Eq(std::nullopt)),
      ResultOf(
          "`getWktType`", [](SV wkt) { return GI::getWktType(wkt); },
          hasValue(expectValidGeometryType)),
      ResultOf(
          "`getCentroid`", [](SV wkt) { return GI::getCentroid(wkt); },
          Eq(std::nullopt)),
      ResultOf(
          "`getBoundingBox`", [](SV wkt) { return GI::getBoundingBox(wkt); },
          Eq(std::nullopt)),
      requestedInfoIs<GeometryInfo, SV>(Eq(std::nullopt)),
      requestedInfoIs<GeometryType, SV>(hasValue(expectValidGeometryType)),
      requestedInfoIs<Centroid, SV>(Eq(std::nullopt)),
      requestedInfoIs<BoundingBox, SV>(Eq(std::nullopt)),
      requestedInfoIs<NumGeometries, SV>(hasValue(expectNumGeom)));
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

// Return a matcher for a `DAnyGeometry` that holds a geometry of type
// `expected`.
inline auto anyGeometryMemberIs =
    [](AnyGeometryMember expected) -> Matcher<DAnyGeometry> {
  return Property(&DAnyGeometry::getType, Eq(static_cast<uint8_t>(expected)));
};

// ____________________________________________________________________________
template <typename T>
inline T getGeometryOfTypeOrThrow(
    const std::string_view wkt, Loc sourceLocation = AD_CURRENT_SOURCE_LOC()) {
  using namespace ad_utility::detail;
  auto l = generateLocationTrace(sourceLocation);
  auto parseRes = parseWkt(wkt);
  if (!parseRes.parsedWkt_.has_value()) {
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
      parseRes.parsedWkt_.value());
}

// Return a matcher for a WKT literal of geometry type `T` whose metric area is
// within one percent of `expectedArea`, both when computed on the parsed
// geometry directly and via `ParsedWkt`. For a collection, additionally check
// that it contains `expectedNumPolygons` polygons.
template <typename T>
Matcher<std::string_view> metricAreaOfWktNear(double expectedArea,
                                              size_t expectedNumPolygons = 0) {
  using namespace ad_utility::detail;
  auto areaNear = DoubleNear(expectedArea, 0.01 * expectedArea);
  Matcher<T> parsedMatcher = AllOf(
      ResultOf(
          "metric area",
          [](const T& parsed) { return computeMetricArea(parsed); }, areaNear),
      ResultOf(
          "metric area via `ParsedWkt`",
          [](const T& parsed) { return computeMetricArea(ParsedWkt{parsed}); },
          areaNear));
  if constexpr (std::is_same_v<T, Collection<CoordType>>) {
    parsedMatcher =
        AllOf(parsedMatcher, ResultOf(
                                 "number of polygons",
                                 [](const T& parsed) {
                                   return collectionToS2Polygons(parsed).size();
                                 },
                                 expectedNumPolygons));
  }
  return ResultOf(
      "parsed geometry",
      [](std::string_view wkt) { return getGeometryOfTypeOrThrow<T>(wkt); },
      parsedMatcher);
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
inline auto geoPointOrWktMatcher = liftOptionalMatcher<GeoPointOrWkt>(
    [](GeoPointOrWkt expected) -> Matcher<GeoPointOrWkt> {
      return std::visit(
          [&](auto& contained) -> Matcher<GeoPointOrWkt> {
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

// Return a matcher for a `DBox` whose corners are near those of `expected`.
//
// NOTE: `Property` cannot be used because the getters of `DBox` are
// overloaded.
inline auto utilBoxNear = [](DBox expected) -> Matcher<DBox> {
  return AllOf(
      ResultOf(
          "lower left", [](const DBox& box) { return box.getLowerLeft(); },
          utilPointNear(expected.getLowerLeft())),
      ResultOf(
          "upper right", [](const DBox& box) { return box.getUpperRight(); },
          utilPointNear(expected.getUpperRight())));
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
      return AllOf(
          AD_FIELD(ParseResult, parsedWkt_, parsedWktNear(expected.parsedWkt_)),
          AD_FIELD(ParseResult, wktType_, Eq(expected.wktType_)),
          AD_FIELD(ParseResult, actualCrs_, Eq(expected.actualCrs_)),
          AD_FIELD(ParseResult, sourceCrs_, Eq(expected.sourceCrs_)));
    });

// ____________________________________________________________________________
struct ExpectedGeometryN {
  size_t n_;
  std::string wkt_;
  std::optional<ParsedWkt> expected_;
};

};  // namespace geoInfoTestHelpers

#endif  // QLEVER_TEST_GEOMETRYINFOTESTHELPERS_H
