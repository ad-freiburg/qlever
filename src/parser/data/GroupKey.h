//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Julian Mundhahs (mundhahj@informatik.uni-freiburg.de)
#pragma once

#include <variant>

#include "../../engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "../Alias.h"
#include "Variable.h"

using GroupKey =
    std::variant<sparqlExpression::SparqlExpressionPimpl, Alias, Variable>;
