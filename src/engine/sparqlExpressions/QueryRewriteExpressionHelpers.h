// Copyright 2025 The QLever Authors, in particular:
//
// 2025 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H

#include <absl/functional/function_ref.h>

#include "engine/SpatialJoinConfig.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "parser/TripleComponent.h"
#include "rdfTypes/GeoRectangle.h"
#include "util/UnitOfMeasurement.h"

// This header declares utilities required during query planning for rewriting
// parts of queries. The implementations of
// `getGeoFunctionExpressionParameters`, `getDe9imRelationExpressionParameters`
// and `geoRectangleOfConstantGeometry` can be found in
// `GeoRelationExpression.cpp`, that of `getGeoDistanceExpressionParameters` in
// `GeoDistanceExpression.cpp`. Expressions of the form
// `geof:distance(?a, ?b) <= constant` are recognized in `engine/filterFacts`.

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
//
// NOTE: This function must be implemented in `GeoRelationExpression.cpp`,
// because the definitions of the geo relation expressions are hidden in that
// cpp file and are therefore invisible elsewhere.
std::optional<GeoFunctionCall> getGeoFunctionExpressionParameters(
    const SparqlExpression& expr);

// The geographic bounding rectangle of a constant geometry operand of a geo
// function call (see `GeoFunctionCall`): a `GeoPoint` `ValueId` yields a
// point rectangle, a WKT literal the bounding box of its parsed geometry.
// Returns `std::nullopt` for variables and non-geometry values.
std::optional<ad_utility::GeoRectangle> geoRectangleOfConstantGeometry(
    const TripleComponent& operand);

// Helper struct for `getGeoDistanceExpressionParameters`
struct GeoDistanceCall : public GeoFunctionCall {
  UnitOfMeasurement unit_;
};

// Same as `getGeoFunctionExpressionParameters`, but with special handling for
// the unit of measurement associated with a distance. Also implemented in
// `GeoDistanceExpression.cpp`. A unit given as a variable is resolved with
// `constantOfVariable`, which returns the constant value of a variable if it is
// known, by default never. An IRI or literal that is not a unit gives
// `UNKNOWN`.
std::optional<GeoDistanceCall> getGeoDistanceExpressionParameters(
    const SparqlExpression& expr,
    absl::FunctionRef<std::optional<TripleComponent>(const Variable&)>
        constantOfVariable =
            [](const Variable&) { return std::optional<TripleComponent>{}; });

// Helper struct for `getDe9imRelationExpressionParameters`
struct De9imRelationCall : public GeoFunctionCall {
  De9imFilterString pattern_;
};

// Same as `getGeoFunctionExpressionParameters`, but for the `geof:relate`
// function, which additionally carries a DE-9IM filter pattern as its third
// argument. Also implemented in `GeoRelationExpression.cpp`.
std::optional<De9imRelationCall> getDe9imRelationExpressionParameters(
    const SparqlExpression& expr);

}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_QUERYREWRITEEXPRESSIONHELPERS_H
