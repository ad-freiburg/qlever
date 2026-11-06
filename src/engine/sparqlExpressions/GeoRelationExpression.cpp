// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

// The geometric relation expressions (`geof:sfIntersects`, `geof:relate`,
// etc.) and the extraction of their parameters for `FILTER` rewriting. They are
// split from `GeoExpression.cpp` to reduce the memory needed for compiling and
// statically analyzing a single translation unit.

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/GeoExpressionHelpers.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "rdfTypes/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {

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
  auto pattern = parseDe9imFilterString(patternLiteral.value().getContent());
  if (!pattern.has_value() || de9imFilterCanMatchDisjoint(pattern.value())) {
    return std::nullopt;
  }

  return De9imRelationCall{
      {SpatialJoinType::DE9IM, std::move(p1).value(), std::move(p2).value()},
      pattern.value()};
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
