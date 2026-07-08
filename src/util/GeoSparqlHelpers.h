// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_GEOSPARQLHELPERS_H
#define QLEVER_GEOSPARQLHELPERS_H

#include <absl/strings/str_cat.h>

#include <cmath>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <variant>

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
#include "util/UnitOfMeasurement.h"

namespace ad_utility {

namespace detail {

static constexpr double invalidCoordinate =
    std::numeric_limits<double>::quiet_NaN();

// TODO: Make the SPARQL expressions work for function pointers or
// std::function.

// Extract coordinates from a well-known text literal.
std::pair<double, double> parseWktPoint(const std::string_view point);

// Calculate geographic distance between points in kilometers using s2geometry.
double wktDistImpl(qlever::GeoPoint point1, qlever::GeoPoint point2);

// Helper to avoid including `GeometryInfoHelpersImpl.h`
std::optional<std::string> geometryNAsWkt(qlever::GeoPointOrWkt wkt, int64_t n);

// Simplify a WKT geometry using `pb_util`. The returned WKT string has neither
// quotation marks nor a datatype yet.
std::optional<std::string> simplifyWkt(qlever::GeoPointOrWkt wkt,
                                       double tolerance);

const auto wktLiteralIri =
    qlever::triple_component::Iri::fromIrirefWithoutBrackets(GEO_WKT_LITERAL);

// Calculate geographic distance between geometries in meters using `pb_util`.
std::optional<double> wktDistLibSpatialJoinImpl(const qlever::GeoPointOrWkt& a,
                                                const qlever::GeoPointOrWkt& b);

}  // namespace detail

// Return the longitude coordinate from a WKT point.
class WktLongitude {
 public:
  double operator()(const std::optional<qlever::GeoPoint>& point) const {
    if (!point.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return point.value().getLng();
  }
};

// Return the latitude coordinate from a WKT point.
class WktLatitude {
 public:
  double operator()(const std::optional<qlever::GeoPoint>& point) const {
    if (!point.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return point.value().getLat();
  }
};

// Compute the distance between two WKT geometries.
class WktDist {
 public:
  double operator()(
      const std::optional<qlever::GeoPointOrWkt>& geom1,
      const std::optional<qlever::GeoPointOrWkt>& geom2,
      const std::optional<UnitOfMeasurement>& unit = std::nullopt) const {
    if (!geom1.has_value() || !geom2.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    auto dist = detail::wktDistLibSpatialJoinImpl(geom1.value(), geom2.value());
    if (!dist.has_value()) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    return detail::kilometerToUnit(dist.value() / 1000.0, unit);
  }
};

// Compute the distance between two WKT points in meters.
class WktMetricDist {
 public:
  double operator()(const std::optional<qlever::GeoPointOrWkt>& geom1,
                    const std::optional<qlever::GeoPointOrWkt>& geom2) const {
    return WktDist{}(geom1, geom2, UnitOfMeasurement::METERS);
  }
};

// Compute the length of a WKT geometry.
class WktLength {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::MetricLength>& len,
      const std::optional<UnitOfMeasurement>& unit = std::nullopt) const {
    if (!len.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    return qlever::ValueId::makeFromDouble(
        detail::kilometerToUnit(len.value().length() / 1000.0, unit));
  }
};

// Compute the length of a WKT geometry in meters.
class WktMetricLength {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::MetricLength>& len) const {
    if (!len.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    return qlever::ValueId::makeFromDouble(len.value().length());
  }
};

// Get the centroid of a geometry.
class WktCentroid {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::Centroid>& geom) const {
    if (!geom.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    return qlever::ValueId::makeFromGeoPoint(geom.value().centroid());
  }
};

// Get the bounding box of a geometry.
class WktEnvelope {
 public:
  qlever::sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<qlever::BoundingBox>& boundingBox) const {
    if (!boundingBox.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    using qlever::triple_component::Iri;
    using qlever::triple_component::LiteralOrIri;
    auto lit = qlever::triple_component::Literal::literalWithoutQuotes(
        boundingBox.value().asWkt());
    lit.addDatatype(detail::wktLiteralIri);
    return {LiteralOrIri{std::move(lit)}};
  }
};

// Get one of the two bounding box corners as `GeoPoint`s.
template <qlever::BoundingBoxCorner RequestedCorner>
class WktEnvelopeCorner {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::BoundingBox>& boundingBox) const {
    if (!boundingBox.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    if constexpr (RequestedCorner == qlever::BoundingBoxCorner::LOWER_LEFT) {
      return qlever::ValueId::makeFromGeoPoint(boundingBox.value().lowerLeft());
    } else {
      static_assert(RequestedCorner == qlever::BoundingBoxCorner::UPPER_RIGHT);
      return qlever::ValueId::makeFromGeoPoint(
          boundingBox.value().upperRight());
    }
  }
};

// Get a single coordinate of the bounding box.
template <qlever::BoundingCoordinate RequestedCoordinate>
class WktBoundingCoordinate {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::BoundingBox>& boundingBox) const {
    if (!boundingBox.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    return qlever::ValueId::makeFromDouble(
        boundingBox.value().getBoundingCoordinate<RequestedCoordinate>());
  }
};

// Get the geometry type of WKT literal using `GeometryInfo`.
class WktGeometryType {
 public:
  qlever::sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<qlever::GeometryType>& geometryType) const {
    if (!geometryType.has_value()) {
      return qlever::ValueId::makeUndefined();
    }

    auto typeIri = geometryType.value().asIri();
    if (!typeIri.has_value()) {
      return qlever::ValueId::makeUndefined();
    }

    // The geometry type should be returned as an xsd:anyURI literal according
    // to the GeoSPARQL standard.
    using qlever::triple_component::Iri;
    using qlever::triple_component::LiteralOrIri;
    auto lit = qlever::triple_component::Literal::literalWithoutQuotes(
        typeIri.value());
    lit.addDatatype(Iri::fromIrirefWithoutBrackets(XSD_ANYURI_TYPE));
    return {LiteralOrIri{std::move(lit)}};
  }
};

// Get the WKT for the n-th element (1-indexed) of the given WKT.
class WktGeometryN {
 public:
  qlever::sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<qlever::GeoPointOrWkt>& wkt,
      const std::optional<int64_t>& n) const {
    using qlever::triple_component::Iri;
    using qlever::triple_component::LiteralOrIri;
    if (!wkt.has_value() || !n.has_value()) {
      return qlever::ValueId::makeUndefined();
    }

    auto resultWkt = detail::geometryNAsWkt(wkt.value(), n.value());

    if (!resultWkt.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    auto lit = qlever::triple_component::Literal::literalWithoutQuotes(
        resultWkt.value());
    lit.addDatatype(detail::wktLiteralIri);
    return {LiteralOrIri{std::move(lit)}};
  }
};

// Simplify a WKT geometry using `pb_util`. Tolerance, interpreted in the
// coordinate units of the geometry.
class WktSimplify {
 public:
  template <typename NumericVariant>
  qlever::sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<qlever::GeoPointOrWkt>& geom,
      const NumericVariant& tolerance) const {
    using qlever::triple_component::Iri;
    using qlever::triple_component::LiteralOrIri;
    if (!geom.has_value()) {
      return qlever::ValueId::makeUndefined();
    }

    // Extract the tolerance as a `double`.
    auto tol = std::visit(
        [](const auto& value) -> std::optional<double> {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_arithmetic_v<T>) {
            return static_cast<double>(value);
          } else {
            return std::nullopt;
          }
        },
        tolerance);
    if (!tol.has_value() || tol.value() <= 0 || !std::isfinite(tol.value())) {
      return qlever::ValueId::makeUndefined();
    }

    auto resultWkt = detail::simplifyWkt(geom.value(), tol.value());
    if (!resultWkt.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    auto lit = qlever::triple_component::Literal::literalWithoutQuotes(
        resultWkt.value());
    lit.addDatatype(detail::wktLiteralIri);
    return {LiteralOrIri{std::move(lit)}};
  }
};

// A generic operation for all geometric relation functions, like
// `geof:sfIntersects`.
template <SpatialJoinType Relation>
class WktGeometricRelation {
 public:
  qlever::ValueId operator()(
      // TODO<ullingerc> For implementation, use a new appropriate value getter
      // for geometry literals and points.
      [[maybe_unused]] const std::optional<qlever::GeoPoint>& geoLeft,
      [[maybe_unused]] const std::optional<qlever::GeoPoint>& geoRight) const {
    AD_THROW(
        "Geometric relations via the `geof:sfIntersects` ... functions are "
        "currently only implemented for a subset of all possible queries. More "
        "details on GeoSPARQL support can be found on the QLever Wiki.");
  }
};

// Get the number of geometries in a WKT literal.
class WktNumGeometries {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::NumGeometries>& numGeom) const {
    if (!numGeom.has_value()) {
      return qlever::ValueId::makeUndefined();
    }
    return qlever::ValueId::makeFromInt(numGeom.value().numGeometries());
  }
};

// Compute the area of a WKT geometry.
class WktArea {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::MetricArea>& area,
      const std::optional<UnitOfMeasurement>& unit = std::nullopt) const {
    if (!area.has_value() ||
        (unit.has_value() && !detail::isAreaUnit(unit.value()))) {
      return qlever::ValueId::makeUndefined();
    }
    double val = detail::squareMeterToUnit(area.value().area(), unit);
    if (std::isnan(val)) {
      return qlever::ValueId::makeUndefined();
    }
    return qlever::ValueId::makeFromDouble(val);
  }
};

// Compute the area of a WKT geometry in square meters.
class WktMetricArea {
 public:
  qlever::ValueId operator()(
      const std::optional<qlever::MetricArea>& area) const {
    if (!area.has_value() || std::isnan(area.value().area())) {
      return qlever::ValueId::makeUndefined();
    }
    return qlever::ValueId::makeFromDouble(area.value().area());
  }
};

}  // namespace ad_utility

#endif  // QLEVER_GEOSPARQLHELPERS_H
