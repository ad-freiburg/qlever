// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/filterFacts/Canonicalizer.h"

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/NaryExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// Return the value of `expression` if it is a number (an `Int` or `Double`
// `Id`, e.g. `500` or `0.5`), and `std::nullopt` otherwise (e.g. for the
// string "500").
std::optional<double> getNumber(const SparqlExpression& expression) {
  const auto* constant =
      dynamic_cast<const sparqlExpression::IdExpression*>(&expression);
  if (constant == nullptr) {
    return std::nullopt;
  }
  return TripleComponent{constant->value()}.getNumber();
}

// Return the canonical form of a single conjunct: `distance < number` and
// `distance <= number` become a `Constraint`, everything else is `Opaque`.
Conjunct toConjunct(const SparqlExpression& expression) {
  // Match `distance op number` for a distance in a unit of length, return
  // `Opaque` for every other expression.
  auto op = sparqlExpression::getComparison(expression);
  if (op != Comparison::LT && op != Comparison::LE) {
    return Opaque{&expression};
  }
  auto call = sparqlExpression::getGeoDistanceExpressionParameters(
      *expression.children()[0]);
  auto number = getNumber(*expression.children()[1]);
  if (!call.has_value() || !ad_utility::detail::isLengthUnit(call->unit_) ||
      !number.has_value()) {
    return Opaque{&expression};
  }

  // Convert the number from the unit of the distance to meters.
  //
  // NOTE: For meters and kilometers, the conversion is exact (in particular, a
  // number in meters is not converted to kilometers and back, which may change
  // it). For the other units, it is exact only up to the rounding of the
  // conversion factor.
  double meters = number.value();
  if (call->unit_ == UnitOfMeasurement::KILOMETERS) {
    meters *= 1000;
  } else if (call->unit_ != UnitOfMeasurement::METERS) {
    meters =
        ad_utility::detail::valueInUnitToKilometer(meters, call->unit_) * 1000;
  }
  return Constraint{Distance{call->left_, call->right_}, op.value(), meters};
}

// Split `expression` at `&&` and append the canonical form of each conjunct to
// `result`.
void split(const SparqlExpression& expression, std::vector<Conjunct>& result) {
  if (sparqlExpression::isAndExpression(expression)) {
    for (const auto& child : expression.children()) {
      split(*child, result);
    }
  } else {
    result.push_back(toConjunct(expression));
  }
}

}  // namespace

// _____________________________________________________________________________
std::vector<Conjunct> canonicalize(const SparqlExpression& filter) {
  std::vector<Conjunct> result;
  split(filter, result);
  return result;
}

}  // namespace filterFacts
