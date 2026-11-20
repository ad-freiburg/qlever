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
namespace filterFacts {
struct Definitions;
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

// Split each FILTER in `filters` with a conjunct (of `&&`) that is a fact (see
// `engine/filterFacts`) into its conjuncts. Then this conjunct can be rewritten
// on its own (e.g. to a `SpatialJoin`), and the other conjuncts are applied as
// regular FILTERs. A conjunct that is a fact only with the definitions `defs`
// of its group (e.g. `geof:distance(?a, ?b) <= ?max` with `VALUES ?max {
// 500 }`) is replaced by an equivalent FILTER without them.
//
// NOTE: If the fact is not exact (e.g. for different values of `?max` in
// different rows), the conjunct is kept in addition to this FILTER.
void rewriteFiltersWithFacts(std::vector<SparqlFilter>& filters,
                             const filterFacts::Definitions& defs);

#endif  // QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
