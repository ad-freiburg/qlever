// Copyright 2021 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"

#include "engine/SpatialJoin.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {
NARY_EXPRESSION(
    LongitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLongitude, true>, GeoPointValueGetter>);
NARY_EXPRESSION(
    LatitudeExpression, 1,
    FV<NumericIdWrapper<ad_utility::WktLatitude, true>, GeoPointValueGetter>);
NARY_EXPRESSION(DistExpression, 2,
                FV<NumericIdWrapper<ad_utility::WktDistGeoPoints, true>,
                   GeoPointValueGetter>);

}  // namespace detail

using namespace detail;
SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2) {
  return std::make_unique<DistExpression>(std::move(child1), std::move(child2));
}

std::optional<GeoFunctionCall> getGeoFunctionExpressionParameters(
    const SparqlExpression& expr) {
  // TODO<ullingerc> After merge of #1938 handle distance unit and
  // metricDistance function

  // Currently only DistExpression
  auto distExpr = dynamic_cast<const DistExpression*>(&expr);
  if (distExpr == nullptr) {
    return std::nullopt;
  }
  auto p1 =
      dynamic_cast<LiteralExpression<Variable>*>(distExpr->children()[0].get());
  if (p1 == nullptr) {
    return std::nullopt;
  }
  auto p2 =
      dynamic_cast<LiteralExpression<Variable>*>(distExpr->children()[1].get());
  if (p2 == nullptr) {
    return std::nullopt;
  }
  return GeoFunctionCall{SpatialJoinType::WITHIN_DIST, p1->value(),
                         p2->value()};
}

SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LatitudeExpression>(std::move(child));
}
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LongitudeExpression>(std::move(child));
}

}  // namespace sparqlExpression
