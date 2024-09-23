// Copyright 2021 - 2024
// University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"

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

SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LatitudeExpression>(std::move(child));
}
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child) {
  return std::make_unique<LongitudeExpression>(std::move(child));
}

}  // namespace sparqlExpression
