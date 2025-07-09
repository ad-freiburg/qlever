// Copyright 2021 - 2025
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>
//          Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#include <type_traits>

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "global/Constants.h"
#include "util/GeoSparqlHelpers.h"
#include "util/GeometryInfo.h"

namespace sparqlExpression {
namespace detail {

NARY_EXPRESSION(
    LongitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLongitude, true>, GeoPointValueGetter>);
NARY_EXPRESSION(
    LatitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLatitude, true>, GeoPointValueGetter>);

NARY_EXPRESSION(
    CentroidExpression, 1,
    FV<ad_utility::WktCentroid, GeometryInfoValueGetter<ad_utility::Centroid>>);

NARY_EXPRESSION(DistExpression, 2,
                FV<NumericIdWrapper<ad_utility::WktDistGeoPoints, true>,
                   GeoPointValueGetter>);
NARY_EXPRESSION(MetricDistExpression, 2,
                FV<NumericIdWrapper<ad_utility::WktMetricDistGeoPoints, true>,
                   GeoPointValueGetter>);
NARY_EXPRESSION(
    DistWithUnitExpression, 3,
    FV<NumericIdWrapper<ad_utility::WktDistGeoPoints, true>,
       GeoPointValueGetter, GeoPointValueGetter, UnitOfMeasurementValueGetter>);

NARY_EXPRESSION(EnvelopeExpression, 1,
                FV<ad_utility::WktEnvelope,
                   GeometryInfoValueGetter<ad_utility::BoundingBox>>);

NARY_EXPRESSION(GeometryTypeExpression, 1,
                FV<ad_utility::WktGeometryType,
                   GeometryInfoValueGetter<ad_utility::GeometryType>>);

template <SpatialJoinType Relation>
NARY_EXPRESSION(
    GeoRelationExpression, 2,
    FV<ad_utility::WktGeometricRelation<Relation>, GeoPointValueGetter>);

}  // namespace detail

using namespace detail;

// _____________________________________________________________________________
SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LatitudeExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LongitudeExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2) {
  return std::make_unique<DistExpression>(std::move(child1), std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeMetricDistExpression(SparqlExpression::Ptr child1,
                                               SparqlExpression::Ptr child2) {
  return std::make_unique<MetricDistExpression>(std::move(child1),
                                                std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeDistWithUnitExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2,
    std::optional<SparqlExpression::Ptr> child3) {
  // Unit is optional
  if (child3.has_value()) {
    return std::make_unique<DistWithUnitExpression>(
        std::move(child1), std::move(child2), std::move(child3.value()));
  } else {
    return std::make_unique<DistExpression>(std::move(child1),
                                            std::move(child2));
  }
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeCentroidExpression(SparqlExpression::Ptr child) {
  return std::make_unique<CentroidExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeEnvelopeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<EnvelopeExpression>(std::move(child));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeGeometryTypeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<GeometryTypeExpression>(std::move(child));
}

// _____________________________________________________________________________
template <SpatialJoinType Relation>
SparqlExpression::Ptr makeGeoRelationExpression(SparqlExpression::Ptr child1,
                                                SparqlExpression::Ptr child2) {
  return std::make_unique<GeoRelationExpression<Relation>>(std::move(child1),
                                                           std::move(child2));
}

namespace {

// Helper to check if `expr` is a `SparqlExpression` on the `geof:sf[Relation]`
// function, given the templated `Relation`.
template <SpatialJoinType Relation>
std::optional<GeoFunctionCall> getGeoRelationExpressionParameters(
    const SparqlExpression& expr) {
  // Is this `expr` a call to `geof:sf[Relation](?x, ?y)`?
  auto geoRelExpr = dynamic_cast<const GeoRelationExpression<Relation>*>(&expr);
  if (geoRelExpr == nullptr) {
    return std::nullopt;
  }

  // Extract variables
  auto p1 = geoRelExpr->children()[0]->getVariableOrNullopt();
  if (!p1.has_value()) {
    return std::nullopt;
  }
  auto p2 = geoRelExpr->children()[1]->getVariableOrNullopt();
  if (!p2.has_value()) {
    return std::nullopt;
  }

  return GeoFunctionCall{Relation, p1.value(), p2.value()};
}

}  // namespace

// _____________________________________________________________________________
std::optional<GeoFunctionCall> getGeoFunctionExpressionParameters(
    const SparqlExpression& expr) {
  // Check against all possible geo relation types
  std::optional<GeoFunctionCall> res;
  using enum SpatialJoinType;

  // TODO<C++26 reflection> get all values of `SpatialJoinType` enum
  if ((res = getGeoRelationExpressionParameters<INTERSECTS>(expr))) {
    return res;
  } else if ((res = getGeoRelationExpressionParameters<CONTAINS>(expr))) {
    return res;
  } else if ((res = getGeoRelationExpressionParameters<COVERS>(expr))) {
    return res;
  } else if ((res = getGeoRelationExpressionParameters<CROSSES>(expr))) {
    return res;
  } else if ((res = getGeoRelationExpressionParameters<TOUCHES>(expr))) {
    return res;
  } else if ((res = getGeoRelationExpressionParameters<EQUALS>(expr))) {
    return res;
  } else if ((res = getGeoRelationExpressionParameters<OVERLAPS>(expr))) {
    return res;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<GeoDistanceCall> getGeoDistanceExpressionParameters(
    const SparqlExpression& expr) {
  using namespace ad_utility::use_type_identity;
  using DistArgs = std::tuple<Variable, Variable, UnitOfMeasurement>;

  // Helper lambda to extract a unit of measurement from a SparqlExpression (IRI
  // or literal with xsd:anyURI datatype)
  auto extractUnit =
      [&](const SparqlExpression* ptr) -> std::optional<UnitOfMeasurement> {
    // Unit given as IRI
    auto unitExpr = dynamic_cast<const IriExpression*>(ptr);
    if (unitExpr != nullptr) {
      return UnitOfMeasurementValueGetter::litOrIriToUnit(
          LiteralOrIri{unitExpr->value()});
    }

    // Unit given as literal expression
    auto unitExpr2 = dynamic_cast<const StringLiteralExpression*>(ptr);
    if (unitExpr2 != nullptr) {
      return UnitOfMeasurementValueGetter::litOrIriToUnit(
          LiteralOrIri{unitExpr2->value()});
    }

    return std::nullopt;
  };

  // Helper lambda to extract the variables and the distance unit from a
  // distance function call
  auto extractArguments = [&](auto ti) -> std::optional<DistArgs> {
    // Check if the argument is a distance function expression
    using T = typename decltype(ti)::type;
    auto distExpr = dynamic_cast<const T*>(&expr);
    if (distExpr == nullptr) {
      return std::nullopt;
    }

    // Extract variables
    auto p1 = distExpr->children()[0]->getVariableOrNullopt();
    if (!p1.has_value()) {
      return std::nullopt;
    }
    auto p2 = distExpr->children()[1]->getVariableOrNullopt();
    if (!p2.has_value()) {
      return std::nullopt;
    }

    // Extract unit
    auto unit = UnitOfMeasurement::KILOMETERS;
    if constexpr (std::is_same_v<T, MetricDistExpression>) {
      unit = UnitOfMeasurement::METERS;
    } else if constexpr (std::is_same_v<T, DistWithUnitExpression>) {
      // If the unit is not fixed, derive it from the user-specified IRI
      auto unitOrNullopt = extractUnit(distExpr->children()[2].get());
      if (!unitOrNullopt.has_value()) {
        return std::nullopt;
      }
      unit = unitOrNullopt.value();
    }

    return DistArgs{p1.value(), p2.value(), unit};
  };

  // Try all possible distance expression types
  auto distVars = extractArguments(ti<DistExpression>);
  if (!distVars.has_value()) {
    distVars = extractArguments(ti<MetricDistExpression>);
  }
  if (!distVars.has_value()) {
    distVars = extractArguments(ti<DistWithUnitExpression>);
  }
  if (!distVars.has_value()) {
    return std::nullopt;
  }

  const auto& [v1, v2, unit] = distVars.value();
  return GeoDistanceCall{{SpatialJoinType::WITHIN_DIST, v1, v2}, unit};
}

}  // namespace sparqlExpression

// Explicit instantiations for the different geometric relations to avoid linker
// problems
using Ptr = sparqlExpression::SparqlExpression::Ptr;

#ifdef QL_INSTANTIATE_GEO_RELATION_EXPR
#error "Macro QL_INSTANTIATE_GEO_RELATION_EXPR already defined"
#endif
#define QL_INSTANTIATE_GEO_RELATION_EXPR(joinType)                            \
  template Ptr                                                                \
      sparqlExpression::makeGeoRelationExpression<SpatialJoinType::joinType>( \
          Ptr, Ptr);

QL_INSTANTIATE_GEO_RELATION_EXPR(INTERSECTS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CONTAINS);
QL_INSTANTIATE_GEO_RELATION_EXPR(COVERS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CROSSES);
QL_INSTANTIATE_GEO_RELATION_EXPR(TOUCHES);
QL_INSTANTIATE_GEO_RELATION_EXPR(EQUALS);
QL_INSTANTIATE_GEO_RELATION_EXPR(OVERLAPS);
