// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_RDFTYPES_GEOMETRYINFOHELPERSIMPL_H
#define QLEVER_SRC_RDFTYPES_GEOMETRYINFOHELPERSIMPL_H

#include <absl/functional/function_ref.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2loop.h>
#include <s2/s2point.h>
#include <s2/s2polygon.h>
#include <spatialjoin/BoxIds.h>
#include <util/geo/Geo.h>

#include <array>
#include <iostream>
#include <memory>
#include <range/v3/numeric/accumulate.hpp>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "global/Constants.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Literal.h"
#include "util/Exception.h"
#include "util/GeoConverters.h"
#include "util/Log.h"
#include "util/TypeTraits.h"

// This file contains functions used for parsing and processing WKT geometries
// using `pb_util`. To avoid unnecessarily compiling expensive modules, this
// file should not be included in header files.

namespace ad_utility::detail {

using namespace ::util::geo;
using namespace geometryConverters;
using ParsedWkt =
    std::variant<Point<CoordType>, Line<CoordType>, Polygon<CoordType>,
                 MultiPoint<CoordType>, MultiLine<CoordType>,
                 MultiPolygon<CoordType>, Collection<CoordType>>;
using ParseResult = std::pair<WKTType, std::optional<ParsedWkt>>;
using DAnyGeometry = util::geo::AnyGeometry<CoordType>;

template <typename T>
CPP_concept WktSingleGeometryType =
    SameAsAny<T, Point<CoordType>, Line<CoordType>, Polygon<CoordType>>;

template <typename T>
CPP_concept WktCollectionType =
    SameAsAny<T, MultiPoint<CoordType>, MultiLine<CoordType>,
              MultiPolygon<CoordType>, Collection<CoordType>>;

static_assert(!std::is_same_v<Line<CoordType>, MultiPoint<CoordType>>);
static_assert(!isVector<Line<CoordType>>);
static_assert(isVector<Collection<CoordType>>);

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
  return std::move(lit).toStringRepresentation();
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
      case LINESTRING: {
        auto line = lineFromWKT<CoordType>(wktLiteral);
        if (line.empty()) {
          throw std::runtime_error("Cannot parse line from WKT");
        }
        parsed = line;
        break;
      }
      case POLYGON: {
        auto polygon = polygonFromWKT<CoordType>(wktLiteral);
        if (polygon.getOuter().empty()) {
          throw std::runtime_error("Cannot parse polygon from WKT");
        }
        parsed = polygon;
        break;
      }
      case MULTIPOINT: {
        auto multipoint = multiPointFromWKT<CoordType>(wktLiteral);
        if (multipoint.empty()) {
          throw std::runtime_error("Cannot parse multipoint from WKT");
        }
        parsed = multipoint;
        break;
      }
      case MULTILINESTRING: {
        auto multiline = multiLineFromWKT<CoordType>(wktLiteral);
        if (multiline.empty()) {
          throw std::runtime_error("Cannot parse multiline from WKT");
        }
        parsed = multiline;
        break;
      }
      case MULTIPOLYGON: {
        auto multipolygon = multiPolygonFromWKT<CoordType>(wktLiteral);
        if (multipolygon.empty()) {
          throw std::runtime_error("Cannot parse multipolygon from WKT");
        }
        parsed = multipolygon;
        break;
      }
      case COLLECTION: {
        auto collection = collectionFromWKT<CoordType>(wktLiteral);
        if (collection.empty()) {
          throw std::runtime_error("Cannot parse collection from WKT");
        }
        parsed = collection;
        break;
      }
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

// Counts the number of geometries in a geometry collection.
inline uint32_t countChildGeometries(const ParsedWkt& geom) {
  return std::visit(
      [](const auto& g) -> uint32_t {
        using T = std::decay_t<decltype(g)>;
        if constexpr (WktCollectionType<T>) {
          return static_cast<uint32_t>(g.size());
        } else {
          static_assert(WktSingleGeometryType<T>);
          return 1;
        }
      },
      geom);
}

// Helper enum for readable handling of the geometry type identifiers used by
// `AnyGeometry`.
enum class AnyGeometryMember : uint8_t {
  POINT,
  LINE,
  POLYGON,
  MULTILINE,
  MULTIPOLYGON,
  COLLECTION,
  MULTIPOINT
};

// Helper to convert the dynamic container `AnyGeometry` to the `ParsedWkt`
// variant type
CPP_template(typename Visitor, typename T)(
    requires SimilarTo<
        T, DAnyGeometry>) inline auto visitAnyGeometry(Visitor visitor,
                                                       T&& geom) {
  using enum AnyGeometryMember;
  // `AnyGeometry` is a class from `pb_util`. It does not operate on an enum,
  // this is why we use our own enum here. The correct matching of the integer
  // identifiers for the geometry types with this enum is tested in
  // `GeometryInfoTest.cpp`.
  switch (AnyGeometryMember{geom.getType()}) {
    case POINT:
      return visitor(AD_FWD(geom).getPoint());
    case LINE:
      return visitor(AD_FWD(geom).getLine());
    case POLYGON:
      return visitor(AD_FWD(geom).getPolygon());
    case MULTILINE:
      return visitor(AD_FWD(geom).getMultiLine());
    case MULTIPOLYGON:
      return visitor(AD_FWD(geom).getMultiPolygon());
    case COLLECTION:
      return visitor(AD_FWD(geom).getCollection());
    case MULTIPOINT:
      return visitor(AD_FWD(geom).getMultiPoint());
    default:
      AD_FAIL();
  }
}

// Helper to implement the computation of metric length for the different
// geometry types.
struct MetricLengthVisitor {
  double operator()(const Point<CoordType>&) const { return 0.0; }

  double operator()(const Line<CoordType>& geom) const {
    return latLngLen<CoordType>(geom);
  }

  // Compute the length of the outer boundary of a polygon.
  double operator()(const Polygon<CoordType>& geom) const {
    return latLngLen<CoordType>(geom.getOuter());
  }

  // Compute the length of a multi-geometry by adding up the lengths of its
  // members.
  CPP_template(typename T)(requires ad_utility::SimilarToAny<
                           T, MultiLine<CoordType>, MultiPolygon<CoordType>,
                           MultiPoint<CoordType>, Collection<CoordType>>) double
  operator()(const T& multiGeom) const {
    // This overload only handles the geometry types implemented by vectors.
    static_assert(ad_utility::similarToInstantiation<T, std::vector>);

    return ::ranges::accumulate(
        ::ranges::transform_view(multiGeom, MetricLengthVisitor{}), 0);
  }

  // Compute the length for the custom container type `AnyGeometry` from
  // `pb_util`. It can dynamically hold any geometry type.
  CPP_template(typename T)(
      requires ad_utility::SimilarTo<T, AnyGeometry<CoordType>>) double
  operator()(const T& geom) const {
    return visitAnyGeometry(MetricLengthVisitor{}, geom);
  }

  // Compute the length for a parsed WKT geometry.
  MetricLength operator()(const ParsedWkt& geometry) const {
    return MetricLength{std::visit(MetricLengthVisitor{}, geometry)};
  }
};
static constexpr MetricLengthVisitor computeMetricLength;

// Extract all (potentially nested) polygons from a geometry collection. This is
// used to calculate area as points and lines have no area and are therefore
// neutral to the area of a collection.
using S2PolygonVec = std::vector<std::unique_ptr<S2Polygon>>;
inline S2PolygonVec collectionToS2Polygons(
    const Collection<CoordType>& collection) {
  S2PolygonVec polygons;
  for (const auto& anyGeom : collection) {
    if (anyGeom.getType() == 2) {
      // Member is single polygon
      polygons.push_back(makeS2Polygon(anyGeom.getPolygon()));
    } else if (anyGeom.getType() == 4) {
      // Member is multipolygon
      for (const auto& polygon : anyGeom.getMultiPolygon()) {
        polygons.push_back(makeS2Polygon(polygon));
      }
    } else if (anyGeom.getType() == 5) {
      // Member is a nested collection
      for (auto& polygon : collectionToS2Polygons(anyGeom.getCollection())) {
        polygons.push_back(std::move(polygon));
      }
    }
  }
  return polygons;
}

// Helper to implement the computation of metric area for the different
// geometry types.
struct MetricAreaVisitor {
  // Given an `S2Polygon` compute the area and convert it to approximated
  // square meters on earth.
  double operator()(std::unique_ptr<S2Polygon> polygon) {
    AD_CORRECTNESS_CHECK(polygon != nullptr);
    return S2Earth::SteradiansToSquareMeters(polygon->GetArea());
  }

  double operator()(const Polygon<CoordType>& polygon) const {
    return MetricAreaVisitor{}(makeS2Polygon(polygon));
  }

  double operator()(S2PolygonVec polygons) const {
    return MetricAreaVisitor{}(
        S2Polygon::DestructiveUnion(std::move(polygons)));
  }

  double operator()(const MultiPolygon<CoordType>& polygons) const {
    // Empty multipolygon has empty area
    if (polygons.empty()) {
      return 0.0;
    }
    // Multipolygon with one member has exactly area of this member
    if (polygons.size() == 1) {
      return MetricAreaVisitor{}(polygons.at(0));
    }
    // For a multipolygon with multiple members, we need to compute the union of
    // the polygons to determine their area.
    return MetricAreaVisitor{}(
        ::ranges::to_vector(polygons | ql::views::transform(makeS2Polygon)));
  }

  // Compute the area in square meters of a geometry collection
  double operator()(const Collection<CoordType>& collection) const {
    return MetricAreaVisitor{}(collectionToS2Polygons(collection));
  }

  // The remaining geometry types always return the area zero
  CPP_template(typename T)(
      requires SameAsAny<T, Point<CoordType>, MultiPoint<CoordType>,
                         Line<CoordType>, MultiLine<CoordType>>) double
  operator()(const T&) const {
    return 0.0;
  }

  double operator()(const ParsedWkt& geom) const {
    return std::visit(MetricAreaVisitor{}, geom);
  };
};

static constexpr MetricAreaVisitor computeMetricArea;

// Helper to convert an instance of the `GeoPointOrWkt` variant to `ParseResult`
// containing a geometry for `pb_util`.
struct ParseGeoPointOrWktVisitor {
  ParseResult operator()(const GeoPoint& point) const {
    return {WKTType::POINT, geoPointToUtilPoint(point)};
  }

  ParseResult operator()(const std::string& wkt) const { return parseWkt(wkt); }

  ParseResult operator()(const GeoPointOrWkt& geoPointOrWkt) const {
    return std::visit(ParseGeoPointOrWktVisitor{}, geoPointOrWkt);
  }

  template <typename T>
  ParseResult operator()(const std::optional<T>& geoPointOrWkt) const {
    if (!geoPointOrWkt.has_value()) {
      return {WKTType::NONE, std::nullopt};
    }
    return std::visit(ParseGeoPointOrWktVisitor{}, geoPointOrWkt.value());
  }
};

static constexpr ParseGeoPointOrWktVisitor parseGeoPointOrWkt;

// Helper to convert a geometry from `pb_util` to a WKT string.
struct UtilGeomToWktVisitor {
  // Visitor for `std::optional` inputs.
  template <typename T>
  std::optional<std::string> operator()(const std::optional<T>& opt) const {
    if (!opt.has_value()) {
      return std::nullopt;
    }
    return UtilGeomToWktVisitor{}(opt.value());
  }

  // Visitor for the `ParsedWkt` variant.
  std::optional<std::string> operator()(const ParsedWkt& variant) const {
    return std::visit(UtilGeomToWktVisitor{}, variant);
  }

  // Visitor for each of the `pb_util` geometry types.
  CPP_template(typename T)(
      requires SimilarToAnyTypeIn<T, ParsedWkt>) std::optional<std::string>
  operator()(const T& geom) const {
    return getWKT(geom);
  }

  // Visitor for the custom container type `AnyGeometry`.
  std::optional<std::string> operator()(
      const AnyGeometry<CoordType>& geom) const {
    return visitAnyGeometry(UtilGeomToWktVisitor{}, geom);
  }
};

static constexpr UtilGeomToWktVisitor utilGeomToWkt;

// Helper to extract the n-th geometry from a parsed `pb_util` geometry. Note
// that this is 1-indexed and non-collection types return themselves at index 1.
struct GeometryNVisitor {
  // Visitor for collection types.
  CPP_template(typename T)(
      requires WktCollectionType<T>) std::optional<ParsedWkt>
  operator()(const T& geom, int64_t n) const {
    // Index range check.
    if (n < 1 || n - 1 >= static_cast<int64_t>(geom.size())) {
      return std::nullopt;
    }
    // If the geometry type is a collection (thus holds `AnyGeometry`
    // containers), strip the `AnyGeometry` container and convert it to a
    // `ParsedWkt` variant.
    if constexpr (std::is_same_v<T, DCollection>) {
      return visitAnyGeometry(
          [](auto& contained) { return ParsedWkt{std::move(contained)}; },
          geom.at(n - 1));
    } else {
      return geom.at(n - 1);
    }
  }

  // Visitor for single geometry types.
  CPP_template(typename T)(
      requires WktSingleGeometryType<T>) std::optional<ParsedWkt>
  operator()(const T& geom, int64_t n) const {
    // For non collection types, only index 1 is defined and returns the
    // geometry itself.
    if (n == 1) {
      return geom;
    }
    return std::nullopt;
  }

  // Visitor for `ParsedWkt` variant.
  std::optional<ParsedWkt> operator()(const ParsedWkt& geom, int64_t n) const {
    return std::visit(
        [n](const auto& contained) { return GeometryNVisitor{}(contained, n); },
        geom);
  }

  // Visitor for `std::optional`.
  template <typename T>
  std::optional<ParsedWkt> operator()(const std::optional<T>& geom,
                                      int64_t n) const {
    if (!geom.has_value()) {
      return std::nullopt;
    }
    return GeometryNVisitor{}(geom.value(), n);
  }

  // Visitor for `GeoPointOrWkt`.
  std::optional<ParsedWkt> operator()(const GeoPointOrWkt& geom,
                                      int64_t n) const {
    auto [type, parsed] = parseGeoPointOrWkt(geom);
    return GeometryNVisitor{}(parsed, n);
  }
};

static constexpr GeometryNVisitor getGeometryN;

}  // namespace ad_utility::detail

#endif  // QLEVER_SRC_RDFTYPES_GEOMETRYINFOHELPERSIMPL_H
