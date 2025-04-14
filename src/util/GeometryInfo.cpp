// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "util/GeometryInfo.h"

#include <util/geo/Geo.h>

#include <cstdint>

#include "parser/GeoPoint.h"
#include "parser/Literal.h"
#include "parser/NormalizedString.h"
#include "util/Exception.h"
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

// ____________________________________________________________________________
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

// ____________________________________________________________________________
GeoPoint utilPointToGeoPoint(Point<CoordType>& point) {
  return GeoPoint(point.getY(), point.getX());
}

// ____________________________________________________________________________
GeoPoint centroidAsGeoPoint(ParsedWkt& geometry) {
  auto uPoint = std::visit([](auto& val) { return centroid(val); }, geometry);
  return utilPointToGeoPoint(uPoint);
};

// ____________________________________________________________________________
std::pair<GeoPoint, GeoPoint> boundingBoxAsGeoPoints(ParsedWkt& geometry) {
  auto bb = std::visit([]<typename T>(T& val) { return getBoundingBox(val); },
                       geometry);
  auto lowerLeft = utilPointToGeoPoint(bb.getLowerLeft());
  auto upperRight = utilPointToGeoPoint(bb.getUpperRight());
  return {lowerLeft, upperRight};
}

}  // namespace detail

// ____________________________________________________________________________
GeometryInfo::GeometryInfo(uint8_t wktType,
                           std::pair<GeoPoint, GeoPoint> boundingBox,
                           GeoPoint centroid) {
  // The WktType only has 8 different values and we have 4 unused bits for the
  // ValueId datatype of the centroid (it is always a point). Therefore we fold
  // the attributes together. On OSM planet this will save approx. 1 GiB in
  // index size.
  uint64_t typeBits = static_cast<uint64_t>(wktType) << ValueId::numDataBits;
  AD_CORRECTNESS_CHECK((wktType & bitMaskCentroid) == 0);
  uint64_t centroidBits = centroid.toBitRepresentation();
  AD_CORRECTNESS_CHECK((centroidBits & bitMaskCentroid) == 0);
  geometryTypeAndCentroid_ = typeBits | centroidBits;

  // TODO check that first is lowerleft & second is upper right
  // TODO make use of 2x4 unused datatype bits in boundingBox
  boundingBox_ =
      std::pair<uint64_t, uint64_t>{boundingBox.first.toBitRepresentation(),
                                    boundingBox.second.toBitRepresentation()};
};

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromWktLiteral(const std::string_view& wkt) {
  // TODO<ullingerc> Remove unnecessary string copying
  auto lit = ad_utility::triple_component::Literal::fromStringRepresentation(
      std::string(wkt));
  auto wktLiteral = std::string(asStringViewUnsafe(lit.getContent()));

  // Parse WKT and compute info
  using namespace detail;
  auto [type, parsed] = parseWkt(wktLiteral);
  auto [p1, p2] = boundingBoxAsGeoPoints(parsed.value());
  auto c = centroidAsGeoPoint(parsed.value());
  // ... further precomputations ...

  return GeometryInfo{type, {p1, p2}, c};
  // Also return "parsed" for further use (eg. saving to special vocab)?
}

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromGeoPoint(
    [[maybe_unused]] const GeoPoint& point) {
  return GeometryInfo{util::geo::WKTType::POINT, {point, point}, point};
}

// ____________________________________________________________________________
uint8_t GeometryInfo::getWktType() const {
  return static_cast<uint8_t>(
      (geometryTypeAndCentroid_ & bitMaskGeometryType) >> ValueId::numDataBits);
}

// ____________________________________________________________________________
GeoPoint GeometryInfo::getCentroid() const {
  return GeoPoint::fromBitRepresentation(geometryTypeAndCentroid_ &
                                         bitMaskCentroid);
}

// ____________________________________________________________________________
std::pair<GeoPoint, GeoPoint> GeometryInfo::getBoundingBox() const {
  return {GeoPoint::fromBitRepresentation(boundingBox_.first),
          GeoPoint::fromBitRepresentation(boundingBox_.second)};
}

}  // namespace ad_utility
