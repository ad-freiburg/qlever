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

// Split each FILTER in `filters` with a conjunct (of `&&`) that is a fact (see
// `engine/filterFacts`) into its conjuncts. Then this conjunct can be rewritten
// on its own (e.g. to a `SpatialJoin`), and the other conjuncts are applied as
// regular FILTERs.
void splitFiltersWithFacts(std::vector<SparqlFilter>& filters);

#endif  // QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
