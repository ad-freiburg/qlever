// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_GEOEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_GEOEXPRESSIONHELPERS_H

#include <optional>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "parser/TripleComponent.h"

// Helpers that are shared by `GeoDistanceExpression.cpp` and
// `GeoRelationExpression.cpp` to extract information for `FILTER` rewriting.

namespace sparqlExpression::detail {

// Extract a `TripleComponent` (a variable or a fixed value) from an argument
// expression of a geo relation or distance function. Returns `std::nullopt`
// if `expr` is neither a variable nor a constant literal expression.
inline std::optional<TripleComponent> extractGeometryOperand(
    const SparqlExpression& expr) {
  if (auto var = expr.getVariableOrNullopt()) {
    return TripleComponent{std::move(var).value()};
  }
  if (const auto* id = dynamic_cast<const IdExpression*>(&expr)) {
    return TripleComponent{id->value()};
  }
  if (const auto* lit = dynamic_cast<const StringLiteralExpression*>(&expr)) {
    return TripleComponent{lit->value()};
  }
  return std::nullopt;
}

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_GEOEXPRESSIONHELPERS_H
