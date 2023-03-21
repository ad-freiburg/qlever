//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "global/Id.h"
#include "parser/data/Variable.h"
#include "util/HashMap.h"
#include "util/TransparentFunctors.h"

// TODO<joka921> Possibly move this inside a namespace.

// A hash map from variables to the column index of that variable in a table,
// used in several places (e.g. the `Operation` class, the `SparqlExpression`
// module, etc.).
using VariableToColumnMap = ad_utility::HashMap<Variable, size_t>;

struct ColumnIndexAndTypeInfo {
  size_t columnIndex_;
  bool mightContainUndef_;
};

// TODO<joka921> Comment the helper function
inline auto makeDefinedColumn = [](size_t columnIndex) {
  return ColumnIndexAndTypeInfo{columnIndex, false};
};

using VariableToColumnMapWithTypeInfo =
    ad_utility::HashMap<Variable, ColumnIndexAndTypeInfo>;

enum class BinOpType { Join, OptionalJoin, Union };
VariableToColumnMapWithTypeInfo makeVarToColMapForJoinOperations(
    const VariableToColumnMapWithTypeInfo& leftVars,
    const VariableToColumnMapWithTypeInfo& rightVars,
    std::vector<std::array<ColumnIndex, 2>> joinColumns, BinOpType);
// Return a vector that contains the contents of the `VariableToColumnMap` in
// ascending order of the column indices.
std::vector<std::pair<Variable, size_t>> copySortedByColumnIndex(
    VariableToColumnMap map);
