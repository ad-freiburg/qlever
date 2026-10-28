// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
#define QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H

#include <absl/functional/function_ref.h>

#include <memory>

#include "parser/data/SparqlFilter.h"
#include "rdfTypes/Variable.h"

class QueryExecutionContext;
class SpatialJoin;
namespace parsedQuery {
class GraphPattern;
}

// This module contains utilities for query rewriting, e.g. optimizing cartesian
// product and filter by replacing it with an appropriate special join.

// Try to rewrite `filter` into an equivalent `SpatialJoin` operation, if
// `filter` is a suitable GeoSPARQL filter with a variable on at least one side.
// Returns `nullptr` if `filter` is not such a filter. A side of the filter that
// was a fixed value is bound to a single-row `VALUES` child using an internal
// variable from `generateUniqueVarName`.
std::shared_ptr<SpatialJoin> rewriteFilterToSpatialJoin(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName);

// Replace each variable that is directly compared to a `geof:distance` or
// `geof:metricDistance` call in one of the filters of `pattern` by its constant
// value, if the variable has the same numeric value in every result row of
// `pattern`. This is the case if it is bound by a `VALUES` with the same value
// in every row or by a `BIND` of a literal, which is a direct child of
// `pattern` or of a group nested in it. This allows
// `rewriteFilterToSpatialJoin` to rewrite such filters. Bindings in an outer
// group, `OPTIONAL`, `MINUS` or `UNION` are not considered: they are invisible
// to the filters of `pattern`, may leave the variable unbound or may bind it to
// different values.
void substituteConstantMaxDistances(parsedQuery::GraphPattern& pattern);

#endif  // QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
