// Copyright 2025, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Christoph Ullinger <ullingec@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "parser/TripleComponent.h"
#include "util/HashMap.h"
#include "util/UnitOfMeasurement.h"

// This header declares utilities required during query planning for rewriting
// parts of queries. The implementation of the
// `getGeoFunctionExpressionParameters` and `getGeoDistanceExpressionParameters`
// functions can be found in `GeoExpression.cpp`. Additionally note the
// `getGeoDistanceFilter` function from `RelationalExpressions.cpp` for
// extracting information from expressions of the form `geof:distance(?a, ?b) <=
// constant`.

namespace sparqlExpression {

// Helper struct for `getGeoFunctionExpressionParameters`. `left_` and `right_`
// are either a variable or a fixed value (a literal given directly in the
// expression).
struct GeoFunctionCall {
  SpatialJoinType function_;
  TripleComponent left_;
  TripleComponent right_;
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

// Helper struct for `getDe9imRelationExpressionParameters`
struct De9imRelationCall : public GeoFunctionCall {
  De9imFilterString pattern_;
};

// Same as `getGeoFunctionExpressionParameters`, but for the `geof:relate`
// function, which additionally carries a DE-9IM filter pattern as its third
// argument. Also implemented in `GeoExpression.cpp`.
std::optional<De9imRelationCall> getDe9imRelationExpressionParameters(
    const SparqlExpression& expr);

// If `expr` has two children, one of which is a call `geof:distance(?a, ?b)`
// (or one of the other distance functions), for example a filter
// `geof:distance(?a, ?b) <= ?x` or `?x > geof:distance(?a, ?b)`, replace the
// other child `?x` and the variable unit of a distance with unit by their
// constant from `constants`, if they have one. This allows for the rewrite of
// the filter to a `SpatialJoin` (see `getGeoDistanceFilter`). Implemented in
// `GeoExpression.cpp`.
//
// NOTE: Only these positions are substituted (and not all variables in
// arbitrary filters), because some expressions store their variable a second
// time outside of their children (for example `PrefixRegexExpression`).
void substituteConstantsInGeoDistanceFilter(
    SparqlExpression& expr,
    const ad_utility::HashMap<Variable, TripleComponent>& constants);

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H
