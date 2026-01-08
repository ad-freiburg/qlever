// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "rdfTypes/Variable.h"
#include "util/UnitOfMeasurement.h"

// This header declares utilities required during query planning for rewriting
// parts of queries. The implementation of the
// `getGeoFunctionExpressionParameters` and `getGeoDistanceExpressionParameters`
// functions can be found in `GeoExpression.cpp`. Additionally note the
// `getGeoDistanceFilter` function from `RelationalExpressions.cpp` for
// extracting information from expressions of the form `geof:distance(?a, ?b) <=
// constant`.

namespace sparqlExpression {

// Helper struct for `getGeoFunctionExpressionParameters`
struct GeoFunctionCall {
  SpatialJoinType function_;
  Variable left_;
  Variable right_;
};

// Helper to extract spatial join parameters from a parsed `geof:` function
// call. Returns `std::nullopt` if the given `SparqlExpression` is not a
// supported geo function or `geof:distance`/`geof:metricDistance` which is
// handled by the `getGeoDistanceExpressionParameters` function below.
// Note: this function must be implemented in in `GeoExpression.cpp`, because
// the definitions of the different geo expressions are hidden in that cpp file
// and are therefore invisible elsewhere.
std::optional<GeoFunctionCall> getGeoFunctionExpressionParameters(
    const SparqlExpression& expr);

// Helper struct for `getGeoDistanceExpressionParameters`
struct GeoDistanceCall : public GeoFunctionCall {
  UnitOfMeasurement unit_;
};

// Same as `getGeoFunctionExpressionParameters`, but with special handling for
// the unit of measurement associated with a distance. Also implemented in in
// `GeoExpression.cpp`.
std::optional<GeoDistanceCall> getGeoDistanceExpressionParameters(
    const SparqlExpression& expr);

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H
