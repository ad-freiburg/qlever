//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "util/GeoSparqlHelpers.h"

namespace sparqlExpression {
namespace detail {
NARY_EXPRESSION(LongitudeExpression, 1,
                FV<NumericIdWrapper<decltype(ad_utility::wktLongitude), true>,
                   LiteralFromIdGetter>);
NARY_EXPRESSION(LatitudeExpression, 1,
                FV<NumericIdWrapper<decltype(ad_utility::wktLatitude), true>,
                   LiteralFromIdGetter>);
NARY_EXPRESSION(DistExpression, 2,
                FV<NumericIdWrapper<decltype(ad_utility::wktDist), true>,
                   LiteralFromIdGetter>);

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
