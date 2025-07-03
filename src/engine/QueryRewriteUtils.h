// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
#define QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H

#include "engine/SpatialJoinConfig.h"
#include "parser/data/SparqlFilter.h"

// This module contains utilities for query rewriting, e.g. optimizing cartesian
// product and filter by replacing it with an appropriate special join.

// Generate a spatial join configuration for a given filter, if this filter is
// suitable for such an optimization.
std::optional<SpatialJoinConfiguration> rewriteFilterToSpatialJoinConfig(
    const SparqlFilter& filter);

#endif  // QLEVER_SRC_ENGINE_QUERYREWRITEUTILS_H
