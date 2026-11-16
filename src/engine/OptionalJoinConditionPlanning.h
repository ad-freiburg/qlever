// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Nino Frey, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_OPTIONALJOINCONDITIONPLANNING_H
#define QLEVER_SRC_ENGINE_OPTIONALJOINCONDITIONPLANNING_H

#include <absl/functional/function_ref.h>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "rdfTypes/Variable.h"
#include "util/HashSet.h"

// By the SPARQL standard, `A OPTIONAL { B FILTER(expr) }` is
// `LeftJoin(A, B, expr)`, so `expr` has to be evaluated on the joined rows
// and not on the rows of `B` alone. This module decides for which FILTERs
// this makes a difference.

// Return true iff `filter`, one of the outermost FILTERs of an OPTIONAL
// group, has to be evaluated on the joined rows. This is the case if it uses
// a variable that can have a different value in the joined row than in the
// row of the group:
// - `variablesOfLeftSide`: the variables of the patterns that precede the
//   OPTIONAL. Only they can get a value from the left side.
// - `isAlwaysDefinedInGroup`: whether a variable is never UNDEF in the result
//   of the group. Then both sides of the join agree on its value.
//
// Throw a `NotSupportedException` if the argument of an `EXISTS` in `filter`
// uses such a variable, because the `ExistsJoin` is computed on the group
// alone.
bool isPartOfOptionalJoinCondition(
    const sparqlExpression::SparqlExpressionPimpl& filter,
    const ad_utility::HashSet<Variable>& variablesOfLeftSide,
    absl::FunctionRef<bool(const Variable&)> isAlwaysDefinedInGroup);

#endif  // QLEVER_SRC_ENGINE_OPTIONALJOINCONDITIONPLANNING_H
