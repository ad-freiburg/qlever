// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
#define QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H

#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
// TODO link s2?
#include <spatialjoin/BoxIds.h>
#include <util/geo/Geo.h>

#include <array>
#include <iostream>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "global/Constants.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Literal.h"
#include "util/Exception.h"
#include "util/Log.h"

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

// Adds quotation marks and the `geo:wktLiteral` datatype to a given string
inline std::string addDatatype(const std::string_view wkt) {
  auto lit = ad_utility::triple_component::Literal::literalWithoutQuotes(wkt);
  auto dt = ad_utility::triple_component::Iri::fromIrirefWithoutBrackets(
      GEO_WKT_LITERAL);
  lit.addDatatype(dt);
  return lit.toStringRepresentation();
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

// TODO
constexpr double EARTH_RADIUS_METERS = 6'371'000.0;
constexpr double STERADIAN_TO_M2 = EARTH_RADIUS_METERS * EARTH_RADIUS_METERS;

// Helper to convert a libspatialjoin `Ring` to an `S2Loop`
inline std::unique_ptr<S2Loop> makeS2Loop(const Ring<CoordType>& ring) {
  std::vector<S2Point> points;
  for (const auto& latlon : ring) {
    S2LatLng s2latlng = S2LatLng::FromDegrees(latlon.getY(), latlon.getX());
    points.push_back(s2latlng.ToPoint());
  }

  // Ensure that there are no zero-length edges (that is edges with twice the
  // same point), as this will lead to an exception from `S2Loop`.
  std::vector<S2Point> cleaned;
  for (size_t i = 0; i < points.size(); ++i) {
    if (i == 0 || points.at(i) != points.at(i - 1)) {
      cleaned.push_back(points.at(i));
    }
  }
  if (cleaned.front() == cleaned.back()) {
    cleaned.pop_back();
  }

  auto loop = std::make_unique<S2Loop>(cleaned);
  loop->Normalize();
  if (!loop->IsValid()) {
    throw InvalidPolygonError();
  }
  return loop;
}

inline S2Polygon makeS2Polygon(const Polygon<CoordType>& polygon) {
  std::vector<std::unique_ptr<S2Loop>> loops;

  // Outer boundary
  loops.push_back(makeS2Loop(polygon.getOuter()));

  // Holes
  for (const auto& hole : polygon.getInners()) {
    loops.push_back(makeS2Loop(hole));
  }

  S2Polygon s2polygon;
  s2polygon.InitNested(std::move(loops));
  return s2polygon;
}

inline double computeMetricAreaS2Polygon(const S2Polygon& polygon) {
  return polygon.GetArea() * STERADIAN_TO_M2;
}

// Compute the area of a polygon in square meters on earth using s2
inline double computeMetricAreaPolygon(const Polygon<CoordType>& polygon) {
  return computeMetricAreaS2Polygon(makeS2Polygon(polygon));
}

// Compute the area of a multipolygon in square meters on earth using s2
inline double computeMetricAreaMultiPolygon(
    const MultiPolygon<CoordType>& polygons) {
  // Empty multipolygon has empty area
  if (polygons.empty()) {
    return 0.0;
  }
  // Multipolygon with one member has exactly area of this member
  if (polygons.size() == 1) {
    return computeMetricAreaPolygon(polygons.at(0));
  }
  // For a multipolygon with multiple members, we need to compute the union of
  // the polygons to determine their area.
  // TODO<ullingerc>: the number of steps could be reduced by building the union
  // in pairs (like divide-and-conquer) or using `S2Builder`
  auto unionPolygon = makeS2Polygon(polygons.at(0));
  for (size_t i = 1; i < polygons.size(); ++i) {
    unionPolygon.InitToUnion(unionPolygon, makeS2Polygon(polygons.at(i)));
  }
  return computeMetricAreaS2Polygon(unionPolygon);
};

// Extract all (potentially nested) polygons from a geometry collection. This is
// used to calculate area as points and lines have no area and are therefore
// neutral to the area of a collection.
inline MultiPolygon<CoordType> collectionToMultiPolygon(
    const Collection<CoordType>& collection) {
  MultiPolygon<CoordType> polygons;
  for (const auto& anyGeom : collection) {
    if (anyGeom.getType() == 2) {
      // Member is single polygon
      polygons.push_back(anyGeom.getPolygon());
    } else if (anyGeom.getType() == 4) {
      // Member is multipolygon
      for (const auto& polygon : anyGeom.getMultiPolygon()) {
        polygons.push_back(polygon);
      }
    } else if (anyGeom.getType() == 5) {
      // Member is a nested collection
      for (const auto& polygon :
           collectionToMultiPolygon(anyGeom.getCollection())) {
        polygons.push_back(polygon);
      }
    }
  }
  return polygons;
}

// Compute the area in square meters of a geometry collection on earth using s2
inline double computeMetricAreaCollection(
    const Collection<CoordType>& collection) {
  return computeMetricAreaMultiPolygon(collectionToMultiPolygon(collection));
}

// Compute the area in square meters of a geometry on earth using s2
inline double computeMetricArea(const ParsedWkt& geometry) {
  return std::visit(
      [](const auto& geom) -> double {
        using T = std::decay_t<decltype(geom)>;
        if constexpr (SameAsAny<T, Point<CoordType>, MultiPoint<CoordType>,
                                Line<CoordType>, MultiLine<CoordType>>) {
          return 0.0;
        } else if constexpr (std::is_same_v<T, Polygon<CoordType>>) {
          return computeMetricAreaPolygon(geom);
        } else if constexpr (std::is_same_v<T, MultiPolygon<CoordType>>) {
          return computeMetricAreaMultiPolygon(geom);
        } else if constexpr (std::is_same_v<T, Collection<CoordType>>) {
          return computeMetricAreaCollection(geom);
        } else {
          // Check that there are no further geometry types
          static_assert(alwaysFalse<T>);
        }
      },
      geometry);
}

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_UTIL_GEOMETRYINFOHELPERSIMPL_H
