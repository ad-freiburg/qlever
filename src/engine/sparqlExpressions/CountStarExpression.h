//  Copyright 2024, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/sparqlExpressions/SparqlExpression.h"

// Return a `SparqlExpression::Ptr` that implements the `COUNT [DISTINCT} *`
// function.
namespace sparqlExpression {
SparqlExpression::Ptr makeCountStarExpression(bool distinct);
}
