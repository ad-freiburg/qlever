//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "parser/data/Variable.h"
#include "util/HashMap.h"
#include "util/TransparentFunctors.h"

// A hash map from variables to the column index of that variable in a table,
// used in several places (e.g. the `Operation` class, the `SparqlExpression`
// module, etc.).
using VariableToColumnMap = ad_utility::HashMap<Variable, size_t>;

// Return a vector that contains the contents of the `VariableToColumnMap` in
// ascending order of the column indices.
std::vector<std::pair<Variable, size_t>> copySortedByColumnIndex(
    VariableToColumnMap map);
