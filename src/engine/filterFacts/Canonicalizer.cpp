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
#include "engine/sparqlExpressions/SparqlExpression.h"
#include "util/Algorithm.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// Return the canonical form of a single conjunct: `distance < number` and
// `distance <= number` become a `DistanceComparison`, everything else is
// `Opaque`.
std::variant<DistanceComparison, Opaque> toCanonicalForm(
    const SparqlExpression& expression) {
  // Match `distance op number` for a distance in a unit of length, return
  // `Opaque` for every other expression.
  auto op = sparqlExpression::getComparison(expression);
  if (op != Comparison::LT && op != Comparison::LE) {
    return Opaque{};
  }
  auto call = sparqlExpression::getGeoDistanceExpressionParameters(
      *expression.children()[0]);
  auto constant = expression.children()[1]->getConstantValue();
  auto number = constant.has_value() ? constant->getNumber() : std::nullopt;
  if (!call.has_value() || !ad_utility::detail::isLengthUnit(call->unit_) ||
      !number.has_value()) {
    return Opaque{};
  }

  // Convert the number from the unit of the distance to meters.
  //
  // NOTE: A number in meters is not converted to kilometers and back (which may
  // change it). For the other units, the conversion is exact only up to the
  // rounding of the conversion factor.
  double meters = call->unit_ == UnitOfMeasurement::METERS
                      ? number.value()
                      : ad_utility::detail::valueInUnitToKilometer(
                            number.value(), call->unit_) *
                            1000;
  return DistanceComparison{Distance{call->left_, call->right_}, op.value(),
                            meters};
}

}  // namespace

// _____________________________________________________________________________
std::vector<Conjunct> canonicalize(
    const sparqlExpression::SparqlExpressionPimpl& filter) {
  return ad_utility::transform(
      filter.splitConjunction(),
      [](sparqlExpression::SparqlExpressionPimpl conjunct) {
        auto canonical = toCanonicalForm(*conjunct.getPimpl());
        return Conjunct{std::move(conjunct), std::move(canonical)};
      });
}

}  // namespace filterFacts
