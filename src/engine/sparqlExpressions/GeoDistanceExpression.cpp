// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// The distance expressions (`geof:distance`, `geof:metricDistance`) and the
// extraction of their parameters for `FILTER` rewriting. They are split from
// `GeoExpression.cpp` to reduce the memory needed for compiling and statically
// analyzing a single translation unit.

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/GeoExpressionHelpers.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "rdfTypes/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {

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
}  // namespace detail

using namespace detail;

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
