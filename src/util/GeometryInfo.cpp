// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include "util/GeometryInfo.h"

#include <spatialjoin/WKTParse.h>
#include <util/geo/Geo.h>

#include <cstdint>

#include "parser/Literal.h"
#include "parser/NormalizedString.h"
#include "util/geo/Point.h"

using namespace ::util::geo;
using CoordType = double;
using ParsedWkt =
    std::variant<Point<CoordType>, Line<CoordType>, Polygon<CoordType>,
                 MultiPoint<CoordType>, MultiLine<CoordType>,
                 MultiPolygon<CoordType>, Collection<CoordType>>;

// ____________________________________________________________________________
GeometryInfo GeometryInfo::fromWktLiteral(const std::string_view& wkt) {
  // TODO<ullingerc> Remove unneccessary string copying
  auto lit = ad_utility::triple_component::Literal::fromStringRepresentation(
      std::string(wkt));
  auto wktLiteral = std::string(asStringViewUnsafe(lit.getContent()));

  ParsedWkt parsed;

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

  auto x = std::visit([]<typename T>(T& val) { return centroid(val); }, parsed);
  std::cout << "Centroid:" << x.getX() << " " << x.getY() << std::endl;

  return GeometryInfo{type};
}
