// Copyright 2021 - 2023, University of Freiburg,
// Chair of Algorithms and Data Structures
// Authors: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>
//          Hannah Bast <bast@cs.uni-freiburg.de>

#pragma once

#include <charconv>
#include <concepts>
#include <cstdlib>

#include "engine/sparqlExpressions/SparqlExpression.h"

// Factory functions for all kinds of expressions that only have other
// expressions as arguments. The actual types and implementations of the
// expressions are hidden in the respective `.cpp` file to reduce compile times.
namespace sparqlExpression {

SparqlExpression::Ptr makeAddExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeAndExpression(SparqlExpression::Ptr child1,
                                        SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeDivideExpression(SparqlExpression::Ptr child1,
                                           SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeMultiplyExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeOrExpression(SparqlExpression::Ptr child1,
                                       SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeSubtractExpression(SparqlExpression::Ptr child1,
                                             SparqlExpression::Ptr child2);

SparqlExpression::Ptr makeUnaryMinusExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeUnaryNegateExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeRoundExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeAbsExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeCeilExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeFloorExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeDistExpression(SparqlExpression::Ptr child1,
                                         SparqlExpression::Ptr child2);
SparqlExpression::Ptr makeLatitudeExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLongitudeExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeDayExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeMonthExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeYearExpression(SparqlExpression::Ptr child);

SparqlExpression::Ptr makeStrExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeStrlenExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeSubstrExpression(SparqlExpression::Ptr string,
                                           SparqlExpression::Ptr start,
                                           SparqlExpression::Ptr length);

SparqlExpression::Ptr makeUppercaseExpression(SparqlExpression::Ptr child);
SparqlExpression::Ptr makeLowercaseExpression(SparqlExpression::Ptr child);
}  // namespace sparqlExpression
