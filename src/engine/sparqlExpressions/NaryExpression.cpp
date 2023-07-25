//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/NaryExpression.h"

#include "engine/sparqlExpressions/NaryExpressionImpl.h"

namespace sparqlExpression {
namespace detail {

INSTANTIATE_NARY(2, FV<decltype(orLambda), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Union>);

INSTANTIATE_NARY(2, FV<decltype(andLambda), EffectiveBooleanValueGetter>,
                 SET<SetOfIntervals::Intersection>);

INSTANTIATE_NARY(1,
                 FV<NumericIdWrapper<decltype(ad_utility::wktLongitude), true>,
                    StringValueGetter>);
INSTANTIATE_NARY(1,
                 FV<NumericIdWrapper<decltype(ad_utility::wktLatitude), true>,
                    StringValueGetter>);
INSTANTIATE_NARY(2, FV<NumericIdWrapper<decltype(ad_utility::wktDist), true>,
                       StringValueGetter>);

INSTANTIATE_NARY(1, FV<decltype(extractYear), DateValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(extractMonth), DateValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(extractDay), DateValueGetter>);
INSTANTIATE_NARY(1, FV<std::identity, StringValueGetter>);
INSTANTIATE_NARY(1, FV<decltype(strlen), StringValueGetter>);

}  // namespace detail

SparqlExpression::Ptr makeOrExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeDayExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeMonthExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeYearExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeStrExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeStrlenExpression(SparqlExpression::Ptr child);

}  // namespace sparqlExpression
