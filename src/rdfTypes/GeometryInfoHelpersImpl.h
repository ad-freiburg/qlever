// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
#define QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H

#include <util/geo/Geo.h>

#include <range/v3/numeric/accumulate.hpp>

#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Literal.h"
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

// ____________________________________________________________________________
inline std::string removeDatatype(const std::string_view& wkt) {
  auto lit = ad_utility::triple_component::Literal::fromStringRepresentation(
      std::string{wkt});
  return std::string{asStringViewUnsafe(lit.getContent())};
}

// ____________________________________________________________________________
inline ParseResult parseWkt(const std::string_view& wkt) {
  auto wktLiteral = removeDatatype(wkt);
  std::optional<ParsedWkt> parsed = std::nullopt;
  auto type = getWKTType(wktLiteral);
  switch (type) {
    case WKTType::POINT: {
      parsed = pointFromWKT<CoordType>(wktLiteral);
      break;
    }
    case WKTType::LINESTRING: {
      parsed = lineFromWKT<CoordType>(wktLiteral);
      break;
    }
    case WKTType::POLYGON: {
      parsed = polygonFromWKT<CoordType>(wktLiteral);
      break;
    }
    case WKTType::MULTIPOINT: {
      parsed = multiPointFromWKT<CoordType>(wktLiteral);
      break;
    }
    case WKTType::MULTILINESTRING: {
      parsed = multiLineFromWKT<CoordType>(wktLiteral);
      break;
    }
    case WKTType::MULTIPOLYGON: {
      parsed = multiPolygonFromWKT<CoordType>(wktLiteral);
      break;
    }
    case WKTType::COLLECTION: {
      parsed = collectionFromWKT<CoordType>(wktLiteral);
      break;
    }
    case WKTType::NONE:
      break;
  }

  return {type, parsed};
}

// ____________________________________________________________________________
inline GeoPoint utilPointToGeoPoint(const Point<CoordType>& point) {
  return {point.getY(), point.getX()};
}

// ____________________________________________________________________________
inline Centroid centroidAsGeoPoint(const ParsedWkt& geometry) {
  auto uPoint = std::visit([](auto& val) { return centroid(val); }, geometry);
  return utilPointToGeoPoint(uPoint);
}

// ____________________________________________________________________________
inline BoundingBox boundingBoxAsGeoPoints(const ParsedWkt& geometry) {
  auto bb = std::visit([](auto& val) { return getBoundingBox(val); }, geometry);
  auto lowerLeft = utilPointToGeoPoint(bb.getLowerLeft());
  auto upperRight = utilPointToGeoPoint(bb.getUpperRight());
  return {lowerLeft, upperRight};
}

// ____________________________________________________________________________
inline Point<CoordType> geoPointToUtilPoint(const GeoPoint& point) {
  return {point.getLng(), point.getLat()};
}

// ____________________________________________________________________________
inline std::string boundingBoxAsWkt(const GeoPoint& lowerLeft,
                                    const GeoPoint& upperRight) {
  Box<CoordType> box{geoPointToUtilPoint(lowerLeft),
                     geoPointToUtilPoint(upperRight)};
  return getWKT(box);
}

// ____________________________________________________________________________
template <detail::constexpr_str_cat_impl::ConstexprString suffix>
inline constexpr std::string_view addSfPrefix() {
  return constexprStrCat<SF_PREFIX, suffix>();
}

static constexpr std::optional<std::string_view> SF_WKT_TYPE_IRI[8]{
    std::nullopt,
    addSfPrefix<"Point">(),
    addSfPrefix<"LineString">(),
    addSfPrefix<"Polygon">(),
    addSfPrefix<"MultiPoint">(),
    addSfPrefix<"MultiLineString">(),
    addSfPrefix<"MultiPolygon">(),
    addSfPrefix<"GeometryCollection">()};

// ____________________________________________________________________________
inline std::optional<std::string_view> wktTypeToIri(uint8_t type) {
  if (type < 8) {
    return SF_WKT_TYPE_IRI[type];
  }
  return std::nullopt;
}

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

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
