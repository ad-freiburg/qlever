// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "util/GeometryInfo.h"

#include <util/geo/Geo.h>

#include <cstdint>

#include "parser/GeoPoint.h"
#include "parser/Literal.h"
#include "parser/NormalizedString.h"
#include "util/GeoSparqlHelpers.h"
#include "util/geo/Point.h"

namespace ad_utility {
namespace detail {

using namespace ::util::geo;
using CoordType = double;
using ParsedWkt =
    std::variant<Point<CoordType>, Line<CoordType>, Polygon<CoordType>,
                 MultiPoint<CoordType>, MultiLine<CoordType>,
                 MultiPolygon<CoordType>, Collection<CoordType>>;

std::pair<WKTType, std::optional<ParsedWkt>> parseWkt(std::string wktLiteral) {
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

GeoPoint utilPointToGeoPoint(Point<CoordType>& point) {
  return GeoPoint(point.getY(), point.getX());
}

GeoPoint centroidAsGeoPoint(ParsedWkt& geometry) {
  auto uPoint = std::visit([](auto& val) { return centroid(val); }, geometry);
  return utilPointToGeoPoint(uPoint);
};

std::pair<GeoPoint, GeoPoint> boundingBoxAsGeoPoints(ParsedWkt& geometry) {
  auto bb = std::visit([]<typename T>(T& val) { return getBoundingBox(val); },
                       geometry);
  auto lowerLeft = utilPointToGeoPoint(bb.getLowerLeft());
  auto upperRight = utilPointToGeoPoint(bb.getUpperRight());
  return {lowerLeft, upperRight};
}

}  // namespace detail

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromWktLiteral(const std::string_view& wkt) {
  // TODO<ullingerc> Remove unneccessary string copying
  auto lit = ad_utility::triple_component::Literal::fromStringRepresentation(
      std::string(wkt));
  auto wktLiteral = std::string(asStringViewUnsafe(lit.getContent()));

  // Parse WKT and compute info
  using namespace detail;
  auto [type, parsed] = parseWkt(wktLiteral);
  auto [p1, p2] = boundingBoxAsGeoPoints(parsed.value());
  auto c = centroidAsGeoPoint(parsed.value());
  // ... further precomputations ...

  return GeometryInfo{type,
                      {p1.toBitRepresentation(), p2.toBitRepresentation()},
                      c.toBitRepresentation()};
  // Also return "parsed" for further use (eg. saving to special vocab)?
}

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromGeoPoint(
    [[maybe_unused]] const GeoPoint& point) {
  return GeometryInfo{
      util::geo::WKTType::POINT,
      {point.toBitRepresentation(), point.toBitRepresentation()},
      point.toBitRepresentation()};
}

}  // namespace ad_utility
