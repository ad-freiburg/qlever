// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
#define QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H

#include <spatialjoin/BoxIds.h>
#include <util/geo/Geo.h>

#include <array>
#include <string_view>
#include <type_traits>

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
    return Centroid{utilPointToGeoPoint(uPoint)};
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
  return {geoPointToUtilPoint(boundingBox.lowerLeft()),
          geoPointToUtilPoint(boundingBox.upperRight())};
}

// Constexpr helper to add the required suffixes to the OGC simple features IRI
// prefix.
template <const std::string_view& suffix>
constexpr std::string_view addSfPrefix() {
  return constexprStrCat<SF_PREFIX, suffix>();
}

namespace detail::geoStrings {
constexpr inline std::string_view point = "Point";
constexpr inline std::string_view linestring = "LineString";
constexpr inline std::string_view polygon = "Polygon";
constexpr inline std::string_view multipoint = "MultiPoint";
constexpr inline std::string_view multiLineString = "MultiLineString";
constexpr inline std::string_view multiPolygon = "MultiPolygon";
constexpr inline std::string_view geometryCollection = "GeometryCollection";
}  // namespace detail::geoStrings
inline constexpr auto SF_WKT_TYPE_IRI = []() {
  using namespace detail::geoStrings;
  return std::array<std::optional<std::string_view>, 8>{
      std::nullopt,  // Invalid geometry
      addSfPrefix<point>(),
      addSfPrefix<linestring>(),
      addSfPrefix<polygon>(),
      addSfPrefix<multipoint>(),
      addSfPrefix<multiLineString>(),
      addSfPrefix<multiPolygon>(),
      addSfPrefix<geometryCollection>()};
}();

// Lookup the IRI for a given WKT type in the array of prepared IRIs.
inline std::optional<std::string_view> wktTypeToIri(uint8_t type) {
  if (type < 8) {
    return SF_WKT_TYPE_IRI.at(type);
  }
  return std::nullopt;
}

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

// Generate a WKT literal for the custom `AnyGeometry` container type from
// `pb_util`, which can dynamically hold any geometry type.
inline std::string getWktAnyGeometry(const AnyGeometry<CoordType>& geom) {
  switch (geom.getType()) {
    case 0:
      return getWKT(geom.getPoint());
    case 1:
      return getWKT(geom.getLine());
    case 2:
      return getWKT(geom.getPolygon());
    case 3:
      return getWKT(geom.getMultiLine());
    case 4:
      return getWKT(geom.getMultiPolygon());
    case 5:
      return getWKT(geom.getCollection());
    case 6:
      return getWKT(geom.getMultiPoint());
    default:
      AD_FAIL();
  }
}

// Extract the n-th geometry from a parsed geometry collection using a 1-based
// index `n`.
inline std::optional<std::string> getGeometryN(const ParsedWkt& geom,
                                               int64_t n) {
  if (n < 1) {
    return std::nullopt;
  }
  // TODO: Should 1 on a non-collection-type return the geometry itself.
  // TODO 0- or 1-indexed?

  static_assert(!isVector<Line<CoordType>>);
  static_assert(isVector<Collection<CoordType>>);

  return std::visit(
      [n](const auto& g) -> std::optional<std::string> {
        using T = std::decay_t<decltype(g)>;

        // Is this geometry a collection type?
        if constexpr (isVector<T>) {
          // Index range check
          if (n - 1 >= static_cast<int64_t>(g.size())) {
            return std::nullopt;
          }

          if constexpr (std::is_same_v<T, Collection<CoordType>>) {
            return getWktAnyGeometry(g.at(n - 1));
          } else {
            return getWKT(g.at(n - 1));
          }
        } else {
          static_assert(!std::is_same_v<T, AnyGeometry<CoordType>>);

          // For non collection types, only index 1 is defined and returns the
          // geometry itself.
          if (n == 1) {
            return getWKT(g);
          }
          return std::nullopt;
        }
      },
      geom);
}

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
