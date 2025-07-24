// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
#define QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H

#include <spatialjoin/BoxIds.h>
#include <util/geo/Geo.h>

<<<<<<< HEAD
#include <range/v3/numeric/accumulate.hpp>
=======
#include <array>
#include <string_view>
>>>>>>> 26997d7633ce47bb22bd7f40dcb74db7d50f67d3

#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Literal.h"
#include "util/Exception.h"
#include "util/Log.h"
#include "util/TypeTraits.h"

// This file contains functions used for parsing and processing WKT geometries
// using `pb_util`. To avoid unnecessarily compiling expensive modules, this
// file should not be included in header files.

namespace ad_utility::detail {

using namespace ::util::geo;
using CoordType = double;
using ParsedWkt =
    std::variant<Point<CoordType>, Line<CoordType>, Polygon<CoordType>,
                 MultiPoint<CoordType>, MultiLine<CoordType>,
                 MultiPolygon<CoordType>, Collection<CoordType>>;
using ParseResult = std::pair<WKTType, std::optional<ParsedWkt>>;

// Removes the datatype and quotation marks from a given literal
inline std::string removeDatatype(const std::string_view& wkt) {
  auto lit = ad_utility::triple_component::Literal::fromStringRepresentation(
      std::string{wkt});
  return std::string{asStringViewUnsafe(lit.getContent())};
}

// Tries to extract the geometry type and parse the geometry given by a WKT
// literal with quotes and datatype using `pb_util`
inline ParseResult parseWkt(const std::string_view& wkt) {
  auto wktLiteral = removeDatatype(wkt);
  std::optional<ParsedWkt> parsed = std::nullopt;
  auto type = getWKTType(wktLiteral);
  using enum WKTType;
  try {
    switch (type) {
      case POINT:
        parsed = pointFromWKT<CoordType>(wktLiteral);
        break;
      case LINESTRING:
        parsed = lineFromWKT<CoordType>(wktLiteral);
        break;
      case POLYGON:
        parsed = polygonFromWKT<CoordType>(wktLiteral);
        break;
      case MULTIPOINT:
        parsed = multiPointFromWKT<CoordType>(wktLiteral);
        break;
      case MULTILINESTRING:
        parsed = multiLineFromWKT<CoordType>(wktLiteral);
        break;
      case MULTIPOLYGON:
        parsed = multiPolygonFromWKT<CoordType>(wktLiteral);
        break;
      case COLLECTION:
        parsed = collectionFromWKT<CoordType>(wktLiteral);
        break;
      case NONE:
      default:
        break;
    }
  } catch (const std::runtime_error& error) {
    AD_LOG_DEBUG << "Error parsing WKT `" << wkt << "`: " << error.what()
                 << std::endl;
  }

  return {type, std::move(parsed)};
}

// Convert a point from `pb_util` to a `GeoPoint`
inline GeoPoint utilPointToGeoPoint(const Point<CoordType>& point) {
  return {point.getY(), point.getX()};
}

// Compute the centroid of a parsed geometry and return it as a `GeoPoint`
// wrapped inside a `Centroid` struct.
inline std::optional<Centroid> centroidAsGeoPoint(const ParsedWkt& geometry) {
  auto uPoint = std::visit([](auto& val) { return centroid(val); }, geometry);
  try {
    return utilPointToGeoPoint(uPoint);
  } catch (const CoordinateOutOfRangeException& ex) {
    AD_LOG_DEBUG << "Cannot compute centroid due to invalid "
                    "coordinates. Error: "
                 << ex.what() << std::endl;
    return std::nullopt;
  }
}

// Compute the bounding box of a parsed geometry and return it as a pair of two
// `GeoPoint`s wrapped inside a `BoundingBox` struct.
inline std::optional<BoundingBox> boundingBoxAsGeoPoints(
    const ParsedWkt& geometry) {
  auto bb = std::visit([](auto& val) { return getBoundingBox(val); }, geometry);
  try {
    auto lowerLeft = utilPointToGeoPoint(bb.getLowerLeft());
    auto upperRight = utilPointToGeoPoint(bb.getUpperRight());
    return BoundingBox{lowerLeft, upperRight};
  } catch (const CoordinateOutOfRangeException& ex) {
    AD_LOG_DEBUG << "Cannot compute bounding box due to "
                    "invalid coordinates. Error: "
                 << ex.what() << std::endl;
    return std::nullopt;
  }
}

// Convert a `GeoPoint` to a point as required by `pb_util`.
inline Point<CoordType> geoPointToUtilPoint(const GeoPoint& point) {
  return {point.getLng(), point.getLat()};
}

// Serialize a bounding box given by a pair of `GeoPoint`s to a WKT literal
// (without quotes or datatype).
inline std::string boundingBoxAsWkt(const GeoPoint& lowerLeft,
                                    const GeoPoint& upperRight) {
  Box<CoordType> box{geoPointToUtilPoint(lowerLeft),
                     geoPointToUtilPoint(upperRight)};
  return getWKT(box);
}

// Convert a `BoundingBox` struct holding two `GeoPoint`s to a `Box` struct as
// required by `pb_util`.
inline Box<CoordType> boundingBoxToUtilBox(const BoundingBox& boundingBox) {
  return {geoPointToUtilPoint(boundingBox.lowerLeft_),
          geoPointToUtilPoint(boundingBox.upperRight_)};
}

// Constexpr helper to add the required suffixes to the OGC simple features IRI
// prefix.
template <detail::constexpr_str_cat_impl::ConstexprString suffix>
inline constexpr std::string_view addSfPrefix() {
  return constexprStrCat<SF_PREFIX, suffix>();
}

// Concrete IRIs, built at compile time, for the supported geometry types.
static constexpr std::array<std::optional<std::string_view>, 8> SF_WKT_TYPE_IRI{
    std::nullopt,  // Invalid geometry
    addSfPrefix<"Point">(),
    addSfPrefix<"LineString">(),
    addSfPrefix<"Polygon">(),
    addSfPrefix<"MultiPoint">(),
    addSfPrefix<"MultiLineString">(),
    addSfPrefix<"MultiPolygon">(),
    addSfPrefix<"GeometryCollection">()};

// Lookup the IRI for a given WKT type in the array of prepared IRIs.
inline std::optional<std::string_view> wktTypeToIri(uint8_t type) {
  if (type < 8) {
    return SF_WKT_TYPE_IRI.at(type);
  }
  return std::nullopt;
}

<<<<<<< HEAD
// ____________________________________________________________________________
inline double computeMetricLengthPolygon(const Polygon<CoordType>& geom) {
  return latLngLen<CoordType>(geom.getOuter());
}

// ____________________________________________________________________________
template <typename T>
inline double computeMetricLengthMulti(
    const std::vector<T>& geom, std::function<double(const T&)> lenFunction) {
  return ::ranges::accumulate(::ranges::transform_view(geom, lenFunction), 0);
}

// ____________________________________________________________________________
inline double computeMetricLengthAnyGeom(const AnyGeometry<CoordType>& geom) {
  switch (geom.getType()) {
    case 0:  // Point
      return 0.0;
    case 1:  // Line
      return latLngLen<CoordType>(geom.getLine());
    case 2:  // Polygon
      return computeMetricLengthPolygon(geom.getPolygon());
    case 3:  // MultiLine
      return computeMetricLengthMulti<Line<CoordType>>(geom.getMultiLine(),
                                                       latLngLen<CoordType>);
    case 4:  // MultiPolygon
      return computeMetricLengthMulti<Polygon<CoordType>>(
          geom.getMultiPolygon(), computeMetricLengthPolygon);
    case 5:  // Collection
      return computeMetricLengthMulti<AnyGeometry<CoordType>>(
          geom.getCollection(), computeMetricLengthAnyGeom);
    case 6:  // MultiPoint
      return 0.0;
    default:
      AD_FAIL();
  }
}

// ____________________________________________________________________________
inline MetricLength computeMetricLength(const ParsedWkt& geometry) {
  return {std::visit(
      [](const auto& geom) -> double {
        using T = std::decay_t<decltype(geom)>;
        // This is redundant with `computeMetricLengthAnyGeom`, however the
        // decision can be made at compile time here, unlike with `AnyGeometry`
        // which is a runtime construct.
        if constexpr (SameAsAny<T, Point<CoordType>, MultiPoint<CoordType>>) {
          return 0.0;
        } else if constexpr (std::is_same_v<T, Line<CoordType>>) {
          return latLngLen<CoordType>(geom);
        } else if constexpr (std::is_same_v<T, Polygon<CoordType>>) {
          return computeMetricLengthPolygon(geom);
        } else if constexpr (std::is_same_v<T, MultiLine<CoordType>>) {
          return computeMetricLengthMulti<Line<CoordType>>(
              geom, latLngLen<CoordType>);
        } else if constexpr (std::is_same_v<T, MultiPolygon<CoordType>>) {
          return computeMetricLengthMulti<Polygon<CoordType>>(
              geom, computeMetricLengthPolygon);
        } else if constexpr (std::is_same_v<T, Collection<CoordType>>) {
          return computeMetricLengthMulti<AnyGeometry<CoordType>>(
              geom, computeMetricLengthAnyGeom);
        } else {
          // Check that there are no further geometry types
          static_assert(alwaysFalse<T>);
        }
      },
      geometry)};
}
=======
// Reverse projection applied by `sj::WKTParser`: convert coordinates from web
// mercator int32 to normal lat-long double coordinates.
inline util::geo::DPoint projectInt32WebMercToDoubleLatLng(
    const util::geo::I32Point& p) {
  return util::geo::webMercToLatLng<double>(
      static_cast<double>(p.getX()) / PREC,
      static_cast<double>(p.getY()) / PREC);
};

// Same as above, but for a bounding box.
inline util::geo::DBox projectInt32WebMercToDoubleLatLng(
    const util::geo::I32Box& box) {
  return {projectInt32WebMercToDoubleLatLng(box.getLowerLeft()),
          projectInt32WebMercToDoubleLatLng(box.getUpperRight())};
};
>>>>>>> 26997d7633ce47bb22bd7f40dcb74db7d50f67d3

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
