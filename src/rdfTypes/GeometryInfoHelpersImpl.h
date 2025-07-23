// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
#define QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H

#include <util/geo/Geo.h>

#include <type_traits>

#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Literal.h"
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

// ____________________________________________________________________________
inline GeoPoint utilPointToGeoPoint(const Point<CoordType>& point) {
  return {point.getY(), point.getX()};
}

// ____________________________________________________________________________
inline std::optional<Centroid> centroidAsGeoPoint(const ParsedWkt& geometry) {
  auto uPoint = std::visit([](auto& val) { return centroid(val); }, geometry);
  try {
    return utilPointToGeoPoint(uPoint);
  } catch (const CoordinateOutOfRangeException& ex) {
    LOG(DEBUG) << "Cannot compute centroid due to invalid coordinates. Error: "
               << ex.what() << std::endl;
    return std::nullopt;
  }
}

// ____________________________________________________________________________
inline std::optional<BoundingBox> boundingBoxAsGeoPoints(
    const ParsedWkt& geometry) {
  auto bb = std::visit([](auto& val) { return getBoundingBox(val); }, geometry);
  try {
    auto lowerLeft = utilPointToGeoPoint(bb.getLowerLeft());
    auto upperRight = utilPointToGeoPoint(bb.getUpperRight());
    return BoundingBox{lowerLeft, upperRight};
  } catch (const CoordinateOutOfRangeException& ex) {
    LOG(DEBUG)
        << "Cannot compute bounding box due to invalid coordinates. Error: "
        << ex.what() << std::endl;
    return std::nullopt;
  }
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

// ____________________________________________________________________________
inline std::optional<std::string> getGeometryN(std::string_view wkt,
                                               int64_t n) {
  if (n < 0) {
    return std::nullopt;
  }

  auto [type, parsed] = parseWkt(wkt);
  if (!parsed.has_value()) {
    return std::nullopt;
  }

  // A linestring is represented as a vector but is not a collection type.
  if (type == WKTType::LINESTRING) {
    return std::nullopt;
  }

  return std::visit(
      [n](const auto& geom) -> std::optional<std::string> {
        using T = std::decay_t<decltype(geom)>;
        if constexpr (isVector<T>) {
          if (static_cast<uint64_t>(n) >= geom.size()) {
            return std::nullopt;
          }
          if constexpr (std::is_same_v<T, Collection<CoordType>>) {
            return getWktAnyGeometry(geom[n]);
          } else {
            return getWKT(geom[n]);
          }
        } else {
          return std::nullopt;
        }
      },
      parsed.value());
}

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
