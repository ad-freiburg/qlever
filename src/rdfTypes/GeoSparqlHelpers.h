// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Hannah Bast <bast@cs.uni-freiburg.de>,
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_RDFTYPES_GEOSPARQLHELPERS_H
#define QLEVER_SRC_RDFTYPES_GEOSPARQLHELPERS_H

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

// If `filter` is a syntactically valid DE-9IM filter pattern (i.e. exactly 9
// characters, each one of `0`-`2`, `T`/`t`, `F`/`f`, or `*`, see
// `De9imFilterString`), return it as a `De9imFilterString`, else
// `std::nullopt`. Note: this does not check whether the pattern can match
// disjoint geometries, see `de9imFilterCanMatchDisjoint` below for that.
//
// Defined here (rather than in `parser/SpatialQuery.{h,cpp}`, its original
// home) because it is called both from `SpatialQuery.cpp` (part of the
// `sparqlParser` library) and from `GeoExpression.cpp` (part of the
// `sparqlExpressions` library); `sparqlParser` depends on `sparqlExpressions`,
// not the other way around, so no single one of those libraries can hold the
// only definition. `rdfTypes`, which this header belongs to, is a dependency
// of both.
std::optional<De9imFilterString> parseDe9imFilterString(
    std::string_view filter);

// Whether the given (syntactically valid) DE-9IM `filter` could match a
// disjoint pair of geometries. Patterns for which this holds (e.g.
// `*********` or the literal disjoint pattern `FF*FF****`) are unsupported:
// the pinned `libspatialjoin` never enumerates disjoint candidate pairs to
// its callback (see `Sweeper::doDE9IMCheck`), regardless of the configured
// filter, so accepting such a pattern would silently omit matching disjoint
// pairs from the result.
//
// The DE-9IM matrix entries are ordered II, IB, IE, BI, BB, BE, EI, EB, EE. A
// pair of geometries is disjoint iff II, IB, BI, and BB (indices 0, 1, 3, 4)
// are all `F`. A filter character only excludes `F` if it is a digit, `T`, or
// `t`; `*` and `F`/`f` both admit it. If all four of these positions admit
// `F`, the pattern could match a disjoint pair.
bool de9imFilterCanMatchDisjoint(const De9imFilterString& filter);

namespace ad_utility {

namespace detail {

static constexpr double invalidCoordinate =
    std::numeric_limits<double>::quiet_NaN();

// TODO: Make the SPARQL expressions work for function pointers or
// std::function.

// Extract coordinates from a well-known text literal.
std::pair<double, double> parseWktPoint(const std::string_view point);

// Calculate geographic distance between points in kilometers using s2geometry.
double wktDistImpl(GeoPoint point1, GeoPoint point2);

// Helper to avoid including `GeometryInfoHelpersImpl.h`
std::optional<std::string> geometryNAsWkt(GeoPointOrWkt wkt, int64_t n);

// Simplify a WKT geometry using `pb_util`. The returned WKT string has neither
// quotation marks nor a datatype yet.
std::optional<std::string> simplifyWkt(GeoPointOrWkt wkt, double tolerance);

const auto wktLiteralIri =
    triple_component::Iri::fromIrirefWithoutBrackets(GEO_WKT_LITERAL);

// Calculate geographic distance between geometries in meters using `pb_util`.
std::optional<double> wktDistLibSpatialJoinImpl(const GeoPointOrWkt& a,
                                                const GeoPointOrWkt& b);

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

// Compute the distance between two WKT geometries.
class WktDist {
 public:
  double operator()(
      const std::optional<GeoPointOrWkt>& geom1,
      const std::optional<GeoPointOrWkt>& geom2,
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
  double operator()(const std::optional<GeoPointOrWkt>& geom1,
                    const std::optional<GeoPointOrWkt>& geom2) const {
    return WktDist{}(geom1, geom2, UnitOfMeasurement::METERS);
  }
};

// Compute the length of a WKT geometry.
class WktLength {
 public:
  ValueId operator()(
      const std::optional<MetricLength>& len,
      const std::optional<UnitOfMeasurement>& unit = std::nullopt) const {
    if (!len.has_value()) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromDouble(
        detail::kilometerToUnit(len.value().length() / 1000.0, unit));
  }
};

// Compute the length of a WKT geometry in meters.
class WktMetricLength {
 public:
  ValueId operator()(const std::optional<MetricLength>& len) const {
    if (!len.has_value()) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromDouble(len.value().length());
  }
};

// Get the centroid of a geometry.
class WktCentroid {
 public:
  ValueId operator()(const std::optional<Centroid>& geom) const {
    if (!geom.has_value()) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromGeoPoint(geom.value().centroid());
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
    return {LiteralOrIri{std::move(lit)}};
  }
};

// Get one of the two bounding box corners as `GeoPoint`s.
template <BoundingBoxCorner RequestedCorner>
class WktEnvelopeCorner {
 public:
  ValueId operator()(const std::optional<BoundingBox>& boundingBox) const {
    if (!boundingBox.has_value()) {
      return ValueId::makeUndefined();
    }
    if constexpr (RequestedCorner == BoundingBoxCorner::LOWER_LEFT) {
      return ValueId::makeFromGeoPoint(boundingBox.value().lowerLeft());
    } else {
      static_assert(RequestedCorner == BoundingBoxCorner::UPPER_RIGHT);
      return ValueId::makeFromGeoPoint(boundingBox.value().upperRight());
    }
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

// Get the geometry type of WKT literal using `GeometryInfo`.
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
    return {LiteralOrIri{std::move(lit)}};
  }
};

// Get the WKT for the n-th element (1-indexed) of the given WKT.
class WktGeometryN {
 public:
  sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<GeoPointOrWkt>& wkt,
      const std::optional<int64_t>& n) const {
    using namespace triple_component;
    if (!wkt.has_value() || !n.has_value()) {
      return ValueId::makeUndefined();
    }

    auto resultWkt = detail::geometryNAsWkt(wkt.value(), n.value());

    if (!resultWkt.has_value()) {
      return ValueId::makeUndefined();
    }
    auto lit = Literal::literalWithoutQuotes(resultWkt.value());
    lit.addDatatype(detail::wktLiteralIri);
    return {LiteralOrIri{std::move(lit)}};
  }
};

// Simplify a WKT geometry using `pb_util`. Tolerance, interpreted in the
// coordinate units of the geometry.
class WktSimplify {
 public:
  template <typename NumericVariant>
  sparqlExpression::IdOrLiteralOrIri operator()(
      const std::optional<GeoPointOrWkt>& geom,
      const NumericVariant& tolerance) const {
    using namespace triple_component;
    if (!geom.has_value()) {
      return ValueId::makeUndefined();
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
      return ValueId::makeUndefined();
    }

    auto resultWkt = detail::simplifyWkt(geom.value(), tol.value());
    if (!resultWkt.has_value()) {
      return ValueId::makeUndefined();
    }
    auto lit = Literal::literalWithoutQuotes(resultWkt.value());
    lit.addDatatype(detail::wktLiteralIri);
    return {LiteralOrIri{std::move(lit)}};
  }
};

// A generic operation for all geometric relation functions, like
// `geof:sfIntersects`.
template <SpatialJoinType::Enum Relation>
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
        "details on GeoSPARQL support can be found in the QLever Docs "
        "(https://docs.qlever.dev/geosparql/).");
  }
};

// The `geof:relate` function, which checks two geometries against an
// arbitrary DE-9IM intersection pattern (the third argument). Currently this is
// a dummy implementation only present to allow query rewriting to a
// `SpatialJoin`.
class WktDe9imRelation {
 public:
  ValueId operator()(
      [[maybe_unused]] const std::optional<GeoPoint>& geoLeft,
      [[maybe_unused]] const std::optional<GeoPoint>& geoRight,
      [[maybe_unused]] const std::optional<std::string>& pattern) const {
    AD_THROW(
        "The `geof:relate` function is currently only implemented for a "
        "subset of all possible queries. More details on GeoSPARQL support "
        "can be found in the QLever Docs "
        "(https://docs.qlever.dev/geosparql/).");
  }
};

// Get the number of geometries in a WKT literal.
class WktNumGeometries {
 public:
  ValueId operator()(const std::optional<NumGeometries>& numGeom) const {
    if (!numGeom.has_value()) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromInt(numGeom.value().numGeometries());
  }
};

// Compute the area of a WKT geometry.
class WktArea {
 public:
  ValueId operator()(
      const std::optional<MetricArea>& area,
      const std::optional<UnitOfMeasurement>& unit = std::nullopt) const {
    if (!area.has_value() ||
        (unit.has_value() && !detail::isAreaUnit(unit.value()))) {
      return ValueId::makeUndefined();
    }
    double val = detail::squareMeterToUnit(area.value().area(), unit);
    if (std::isnan(val)) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromDouble(val);
  }
};

// Compute the area of a WKT geometry in square meters.
class WktMetricArea {
 public:
  ValueId operator()(const std::optional<MetricArea>& area) const {
    if (!area.has_value() || std::isnan(area.value().area())) {
      return ValueId::makeUndefined();
    }
    return ValueId::makeFromDouble(area.value().area());
  }
};

}  // namespace ad_utility

#endif  // QLEVER_SRC_RDFTYPES_GEOSPARQLHELPERS_H
