// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
#define QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H

#include <absl/functional/function_ref.h>

#include <memory>

#include "engine/SpatialJoinConfig.h"
#include "parser/data/SparqlFilter.h"

class QueryExecutionContext;
class QueryExecutionTree;

// This module contains utilities for query rewriting, e.g. optimizing cartesian
// product and filter by replacing it with an appropriate special join.

// Result of `rewriteFilterToSpatialJoinConfig`. `childLeft_`/`childRight_` are
// set for a side of `config_` that was a fixed value (not a variable) in the
// original filter: an already-built one-row `VALUES` tree that binds the
// fresh internal variable `config_.left_`/`right_` to that value. A
// `std::nullopt` child means that side is an ordinary variable, which the
// query planner still has to connect via the join graph, exactly as before.
struct SpatialJoinRewriteResult {
  SpatialJoinConfiguration config_;
  std::optional<std::shared_ptr<QueryExecutionTree>> childLeft_;
  std::optional<std::shared_ptr<QueryExecutionTree>> childRight_;
};

// Generate a spatial join configuration for a given filter, if this filter is
// suitable for such an optimization. `generateUniqueVarName` is used to
// obtain a fresh internal variable for each side of the filter that is a
// fixed value rather than a variable.
std::optional<SpatialJoinRewriteResult> rewriteFilterToSpatialJoinConfig(
    const SparqlFilter& filter, QueryExecutionContext* qec,
    absl::FunctionRef<Variable()> generateUniqueVarName);

#endif  // QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
