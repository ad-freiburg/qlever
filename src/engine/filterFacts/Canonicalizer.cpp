// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Christoph Ullinger <ullingec@cs.uni-freiburg.de>, UFR
//
// UFR = University of Freiburg, Chair of Algorithms and Data Structures
//
// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#include "engine/filterFacts/Canonicalizer.h"

#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

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
  auto constant = expression.children()[1]->getConstantValue();
  auto number = constant.has_value() ? constant->getNumber() : std::nullopt;
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

}  // namespace

// _____________________________________________________________________________
std::vector<Conjunct> canonicalize(
    const sparqlExpression::SparqlExpressionPimpl& filter) {
  return ::ranges::to<std::vector>(
      filter.splitConjunction() |
      ql::views::transform([](const auto& conjunct) {
        return toConjunct(*conjunct.getPimpl());
      }));
}

}  // namespace filterFacts
