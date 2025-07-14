// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_GEOSPARQLHELPERS_H
#define QLEVER_GEOSPARQLHELPERS_H

#include <absl/strings/str_cat.h>

#include <limits>
#include <optional>
#include <string_view>

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/SparqlExpressionTypes.h"
#include "global/Constants.h"
#include "global/ValueId.h"
#include "index/LocalVocabEntry.h"
#include "parser/NormalizedString.h"
#include "rdfTypes/GeoPoint.h"
#include "rdfTypes/GeometryInfo.h"
#include "rdfTypes/Iri.h"
#include "rdfTypes/Literal.h"

namespace ad_utility {

namespace detail {

static constexpr double invalidCoordinate =
    std::numeric_limits<double>::quiet_NaN();

static constexpr double kilometerToMile = 0.62137119;

// TODO: Make the SPARQL expressions work for function pointers or
// std::function.

// Extract coordinates from a well-known text literal.
std::pair<double, double> parseWktPoint(const std::string_view point);

// Calculate geographic distance between points in kilometers using s2geometry.
double wktDistImpl(GeoPoint point1, GeoPoint point2);

// Convert kilometers to other supported units. If `unit` is `std::nullopt` it
// is treated as kilometers.
double kilometerToUnit(double kilometers,
                       std::optional<UnitOfMeasurement> unit);

// Convert value from any supported unit to kilometers. If `unit` is
// `std::nullopt` it is treated as kilometers.
double valueInUnitToKilometer(double valueInUnit,
                              std::optional<UnitOfMeasurement> unit);

// Convert a unit IRI string (without quotes or brackets) to unit.
UnitOfMeasurement iriToUnitOfMeasurement(const std::string_view& uri);

const auto wktLiteralIri =
    triple_component::Iri::fromIrirefWithoutBrackets(GEO_WKT_LITERAL);

}  // namespace detail

// Return the longitude coordinate from a WKT point.
class WktLongitude {
 public:
  double operator()(const std::optional<GeoPoint>& point) const {
    if (!point.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return point.value().getLng();
  }
};

// Return the latitude coordinate from a WKT point.
class WktLatitude {
 public:
  double operator()(const std::optional<GeoPoint>& point) const {
    if (!point.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return point.value().getLat();
  }
};

// Compute the distance between two WKT points.
class WktDistGeoPoints {
 public:
  double operator()(
      const std::optional<GeoPoint>& point1,
      const std::optional<GeoPoint>& point2,
      const std::optional<UnitOfMeasurement>& unit = std::nullopt) const {
    if (!point1.has_value() || !point2.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    return detail::kilometerToUnit(
        detail::wktDistImpl(point1.value(), point2.value()), unit);
  }
};

// Compute the distance between two WKT points in meters.
class WktMetricDistGeoPoints {
 public:
  double operator()(const std::optional<GeoPoint>& point1,
                    const std::optional<GeoPoint>& point2) const {
    return WktDistGeoPoints{}(point1, point2, UnitOfMeasurement::METERS);
  }
};

// Get the centroid of a geometry.
class WktCentroid {
 public:
  ValueId operator()(const std::optional<Centroid>& geom) const {
    if (!geom.has_value()) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromGeoPoint(geom.value().centroid_);
  }
};

// Get the bounding box of a geometry.
class WktEnvelope {
 public:
  sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<BoundingBox>& boundingBox) const {
    if (!boundingBox.has_value()) {
      return ValueId::makeUndefined();
    }
    using namespace triple_component;
    auto lit = Literal::literalWithoutQuotes(boundingBox.value().asWkt());
    lit.addDatatype(detail::wktLiteralIri);
    return {LiteralOrIri{lit}};
  }
};

// Get a single coordinate of the bounding box.
template <BoundingCoordinate RequestedCoordinate>
class WktBoundingCoordinate {
 public:
  ValueId operator()(const std::optional<BoundingBox>& boundingBox) const {
    if (!boundingBox.has_value()) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromDouble(
        boundingBox.value().getBoundingCoordinate<RequestedCoordinate>());
  }
};

// Compute the distance between two WKT points in meters.
class WktGeometryType {
 public:
  sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<GeometryType>& geometryType) const {
    if (!geometryType.has_value()) {
      return ValueId::makeUndefined();
    }

    auto typeIri = geometryType.value().asIri();
    if (!typeIri.has_value()) {
      return ValueId::makeUndefined();
    }

    // The geometry type should be returned as an xsd:anyURI literal according
    // to the GeoSPARQL standard.
    using namespace triple_component;
    auto lit = Literal::literalWithoutQuotes(typeIri.value());
    lit.addDatatype(Iri::fromIrirefWithoutBrackets(XSD_ANYURI_TYPE));
    return {LiteralOrIri{lit}};
  }
};

// A generic operation for all geometric relation functions, like
// `geof:sfIntersects`.
template <SpatialJoinType Relation>
class WktGeometricRelation {
 public:
  ValueId operator()(
      // TODO<ullingerc> For implementation, use a new appropriate value getter
      // for geometry literals and points.
      [[maybe_unused]] const std::optional<GeoPoint>& geoLeft,
      [[maybe_unused]] const std::optional<GeoPoint>& geoRight) const {
    AD_THROW(
        "Geometric relations via the `geof:sfIntersects` ... functions are "
        "currently only implemented for a subset of all possible queries. More "
        "details on GeoSPARQL support can be found on the QLever Wiki.");
  }
};

}  // namespace ad_utility

#endif  // QLEVER_GEOSPARQLHELPERS_H
