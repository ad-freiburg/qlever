// Copyright 2021 - 2026 The QLever Authors, in particular:
//
// 2021 - 2025 Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>, UFR
// 2021 - 2025 Hannah Bast <bast@cs.uni-freiburg.de>, UFR
// 2021 - 2026 Christoph Ullinger <ullingec@informatik.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "backports/type_traits.h"
#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "rdfTypes/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {

NARY_EXPRESSION(
    LongitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLongitude, true>, GeoPointValueGetter>);
NARY_EXPRESSION(
    LatitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLatitude, true>, GeoPointValueGetter>);

NARY_EXPRESSION(
    DistExpression, 2,
    FV<NumericIdWrapper<ad_utility::WktDist, true>, GeoPointOrWktValueGetter>);
NARY_EXPRESSION(MetricDistExpression, 2,
                FV<NumericIdWrapper<ad_utility::WktMetricDist, true>,
                   GeoPointOrWktValueGetter>);
NARY_EXPRESSION(
    DistWithUnitExpression, 3,
    FV<NumericIdWrapper<ad_utility::WktDist, true>, GeoPointOrWktValueGetter,
       GeoPointOrWktValueGetter, UnitOfMeasurementValueGetter>);

NARY_EXPRESSION(
    GeometryNExpression, 2,
    FV<ad_utility::WktGeometryN, GeoPointOrWktValueGetter, IntValueGetter>);

NARY_EXPRESSION(
    SimplifyGeometryExpression, 2,
    FV<ad_utility::WktSimplify, GeoPointOrWktValueGetter, NumericValueGetter>);

template <SpatialJoinType::Enum Relation>
NARY_EXPRESSION(
    GeoRelationExpression, 2,
    FV<ad_utility::WktGeometricRelation<Relation>, GeoPointValueGetter>);

// The actual `geof:relate` expression is currently unimplemented (see
// `WktDe9imRelation` in `GeoSparqlHelpers.h`), it is only usable via query
// rewriting to a `SpatialJoin`. The value getters below are thus dummies for
// now.
NARY_EXPRESSION(De9imRelationExpression, 3,
                FV<ad_utility::WktDe9imRelation, GeoPointValueGetter,
                   GeoPointValueGetter, StringValueGetter>);

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
SparqlExpression::Ptr makeGeometryNExpression(SparqlExpression::Ptr child1,
                                              SparqlExpression::Ptr child2) {
  return std::make_unique<GeometryNExpression>(std::move(child1),
                                               std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeSimplifyGeometryExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2) {
  return std::make_unique<SimplifyGeometryExpression>(std::move(child1),
                                                      std::move(child2));
}

// _____________________________________________________________________________
template <SpatialJoinType::Enum Relation>
SparqlExpression::Ptr makeGeoRelationExpression(SparqlExpression::Ptr child1,
                                                SparqlExpression::Ptr child2) {
  return std::make_unique<GeoRelationExpression<Relation>>(std::move(child1),
                                                           std::move(child2));
}

// _____________________________________________________________________________
SparqlExpression::Ptr makeDe9imRelationExpression(
    SparqlExpression::Ptr child1, SparqlExpression::Ptr child2,
    SparqlExpression::Ptr child3) {
  return std::make_unique<De9imRelationExpression>(
      std::move(child1), std::move(child2), std::move(child3));
}

// Helpers to extract information for `FILTER` rewriting.
namespace {

// Extract a `TripleComponent` (a variable or a fixed value) from an argument
// expression of a geo relation or distance function. Returns `std::nullopt`
// if `expr` is neither a variable nor a constant literal expression.
std::optional<TripleComponent> extractGeometryOperand(
    const SparqlExpression& expr) {
  if (auto var = expr.getVariableOrNullopt()) {
    return TripleComponent{std::move(var).value()};
  }
  if (const auto* id = dynamic_cast<const IdExpression*>(&expr)) {
    return TripleComponent{id->value()};
  }
  if (const auto* lit = dynamic_cast<const StringLiteralExpression*>(&expr)) {
    return TripleComponent{lit->value()};
  }
  return std::nullopt;
}

// Helper to check if `expr` is a `SparqlExpression` on the `geof:sf[Relation]`
// function, given the templated `Relation`.
template <SpatialJoinType::Enum Relation>
std::optional<GeoFunctionCall> getGeoRelationExpressionParameters(
    const SparqlExpression& expr) {
  // Is this `expr` a call to `geof:sf[Relation](?x, ?y)`?
  auto geoRelExpr = dynamic_cast<const GeoRelationExpression<Relation>*>(&expr);
  if (geoRelExpr == nullptr) {
    return std::nullopt;
  }

  // Extract variables or fixed values.
  auto p1 = extractGeometryOperand(*geoRelExpr->children()[0]);
  if (!p1.has_value()) {
    return std::nullopt;
  }
  auto p2 = extractGeometryOperand(*geoRelExpr->children()[1]);
  if (!p2.has_value()) {
    return std::nullopt;
  }

  return GeoFunctionCall{Relation, std::move(p1).value(),
                         std::move(p2).value()};
}

}  // namespace

// _____________________________________________________________________________
std::optional<GeoFunctionCall> getGeoFunctionExpressionParameters(
    const SparqlExpression& expr) {
  // Check against all possible geo relation types
  std::optional<GeoFunctionCall> res;
  using enum SpatialJoinType::Enum;

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
  } else if ((res = getGeoRelationExpressionParameters<WITHIN>(expr))) {
    return res;
  }
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<De9imRelationCall> getDe9imRelationExpressionParameters(
    const SparqlExpression& expr) {
  // Is this `expr` a call to `geof:relate(?x, ?y, "<pattern>")`?
  auto de9imExpr = dynamic_cast<const De9imRelationExpression*>(&expr);
  if (de9imExpr == nullptr) {
    return std::nullopt;
  }

  // Extract variables or fixed values.
  auto p1 = extractGeometryOperand(*de9imExpr->children()[0]);
  if (!p1.has_value()) {
    return std::nullopt;
  }
  auto p2 = extractGeometryOperand(*de9imExpr->children()[1]);
  if (!p2.has_value()) {
    return std::nullopt;
  }

  // Extract and validate the DE-9IM filter pattern
  auto patternLiteral =
      getLiteralFromLiteralExpression(de9imExpr->children()[2].get());
  if (!patternLiteral.has_value()) {
    return std::nullopt;
  }
  auto pattern = parseDe9imFilterString(
      asStringViewUnsafe(patternLiteral.value().getContent()));
  if (!pattern.has_value() || de9imFilterCanMatchDisjoint(pattern.value())) {
    return std::nullopt;
  }

  return De9imRelationCall{
      {SpatialJoinType::DE9IM, std::move(p1).value(), std::move(p2).value()},
      pattern.value()};
}

// _____________________________________________________________________________
std::optional<GeoDistanceCall> getGeoDistanceExpressionParameters(
    const SparqlExpression& expr) {
  using namespace ad_utility::use_type_identity;
  using DistArgs =
      std::tuple<TripleComponent, TripleComponent, UnitOfMeasurement>;

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

    // Extract variables or fixed values.
    auto p1 = extractGeometryOperand(*distExpr->children()[0]);
    if (!p1.has_value()) {
      return std::nullopt;
    }
    auto p2 = extractGeometryOperand(*distExpr->children()[1]);
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
#define QL_INSTANTIATE_GEO_RELATION_EXPR(joinType)          \
  template Ptr sparqlExpression::makeGeoRelationExpression< \
      SpatialJoinType::Enum::joinType>(Ptr, Ptr)

QL_INSTANTIATE_GEO_RELATION_EXPR(INTERSECTS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CONTAINS);
QL_INSTANTIATE_GEO_RELATION_EXPR(COVERS);
QL_INSTANTIATE_GEO_RELATION_EXPR(CROSSES);
QL_INSTANTIATE_GEO_RELATION_EXPR(TOUCHES);
QL_INSTANTIATE_GEO_RELATION_EXPR(EQUALS);
QL_INSTANTIATE_GEO_RELATION_EXPR(OVERLAPS);
QL_INSTANTIATE_GEO_RELATION_EXPR(WITHIN);
