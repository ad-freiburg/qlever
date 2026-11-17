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
#include "parser/ParsedQuery.h"
#include "parser/VariableCounter.h"
#include "util/Algorithm.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// For `BIND(expression AS ?v)`, return true if a FILTER of the group that
// contains `?v` may substitute `expression` for `?v`. This holds if
// `expression` is deterministic, contains no `EXISTS`, and only contains
// variables from `alwaysBound` (the variables that are bound in every row
// before the `BIND`). Then `expression` has the same value at the FILTER as
// at the `BIND` in every row.
//
// NOTE: For example, a variable that is only bound after the `BIND` is unbound
// at the `BIND` (so `?v` is unbound), but bound at the FILTER.
bool isEquivalentToTarget(const SparqlExpression& expression,
                          const ad_utility::HashSet<Variable>& alwaysBound) {
  std::vector<const SparqlExpression*> exists;
  expression.getExistsExpressions(exists);
  return expression.isDeterministic() && exists.empty() &&
         ql::ranges::all_of(expression.containedVariables(),
                            [&alwaysBound](const Variable* variable) {
                              return alwaysBound.contains(*variable);
                            });
}

}  // namespace

// _____________________________________________________________________________
Definitions collectDefinitions(
    const std::vector<parsedQuery::GraphPatternOperation>& group) {
  Definitions defs;
  // The variables that are always bound by the patterns so far. Only triples,
  // `VALUES` and `BIND`s of constants count (ignoring nested groups etc. is
  // conservative).
  ad_utility::HashSet<Variable> alwaysBound;
  for (const auto& operation : group) {
    // Remove the `BIND`s whose target `operation` may bind again, that is, any
    // variable that `operation` contains (where the expression is undefined,
    // the target is then bound freely, so it is not equivalent to the
    // expression).
    //
    // NOTE: A later `BIND` cannot bind the target again (the target of a
    // `BIND` must not be in scope before it), so its expression may use the
    // target.
    if (!std::holds_alternative<parsedQuery::Bind>(operation)) {
      parsedQuery::VariableCounter variables;
      variables(operation);
      absl::erase_if(defs.expressions_, [&variables](const auto& definition) {
        return variables.counts().contains(definition.first);
      });
    }

    // Add the definitions of `operation`.
    if (const auto* bgp =
            std::get_if<parsedQuery::BasicGraphPattern>(&operation)) {
      bgp->collectAllContainedVariables(alwaysBound);
    } else if (const auto* values =
                   std::get_if<parsedQuery::Values>(&operation)) {
      const auto& [variables, rows] = values->_inlineValues;
      for (size_t i = 0; i < variables.size(); ++i) {
        std::vector<TripleComponent> column;
        for (const auto& row : rows) {
          column.push_back(row.at(i));
        }
        if (ql::ranges::any_of(column, &TripleComponent::isUndef)) {
          continue;
        }
        alwaysBound.insert(variables[i]);
        // If several `VALUES` clauses bind the same variable, each of them
        // contains all its values.
        defs.constants_.emplace(variables[i], std::move(column));
      }
    } else if (const auto* bind = std::get_if<parsedQuery::Bind>(&operation)) {
      // The target of a constant other than `UNDEF` is always bound to it,
      // like a `VALUES` clause with a single row. The target of any other
      // expression is not always bound (the expression may be undefined).
      auto constant = bind->_expression.getConstantValue();
      if (constant.has_value() &&
          !(constant->isId() && constant->getId().isUndefined())) {
        alwaysBound.insert(bind->_target);
        defs.constants_.emplace(bind->_target,
                                std::vector{std::move(constant).value()});
      } else if (const auto* expression = bind->_expression.getPimpl();
                 isEquivalentToTarget(*expression, alwaysBound)) {
        defs.expressions_.emplace(bind->_target, expression);
      }
    }
  }
  return defs;
}

namespace {

// Return `expression`, or the expression of the `BIND` in `defs` if it is a
// variable with such a definition.
const SparqlExpression& resolve(const SparqlExpression& expression,
                                const Definitions& defs) {
  if (auto variable = expression.getVariableOrNullopt()) {
    if (auto it = defs.expressions_.find(variable.value());
        it != defs.expressions_.end()) {
      return resolve(*it->second, defs);
    }
  }
  return expression;
}

// Return the possible values of `expression` if it is a number or a variable
// with only numbers in `defs` (and `std::nullopt` otherwise).
std::optional<std::vector<double>> getNumbers(
    const SparqlExpression& expression, const Definitions& defs) {
  const auto& resolved = resolve(expression, defs);

  // A variable with the numbers of a `VALUES` clause or a `BIND`.
  if (auto variable = resolved.getVariableOrNullopt()) {
    auto it = defs.constants_.find(variable.value());
    if (it == defs.constants_.end() || it->second.empty()) {
      return std::nullopt;
    }
    std::vector<double> numbers;
    for (const auto& constant : it->second) {
      auto number = constant.getNumber();
      if (!number.has_value()) {
        return std::nullopt;
      }
      numbers.push_back(number.value());
    }
    return numbers;
  }

  // A single number.
  auto constant = resolved.getConstantValue();
  auto number = constant.has_value() ? constant->getNumber() : std::nullopt;
  return number.has_value() ? std::optional{std::vector{number.value()}}
                            : std::nullopt;
}

// Return the canonical form of a single conjunct: `distance < number` and
// `distance <= number` become a `DistanceComparison`, everything else is
// `Opaque`.
std::variant<DistanceComparison, Opaque> toCanonicalForm(
    const SparqlExpression& expression, const Definitions& defs) {
  // Match `distance op number` for a distance in a unit of length, return
  // `Opaque` for every other expression.
  auto op = sparqlExpression::getComparison(expression);
  if (op != Comparison::LT && op != Comparison::LE) {
    return Opaque{};
  }
  auto call = sparqlExpression::getGeoDistanceExpressionParameters(
      resolve(*expression.children()[0], defs));
  auto numbers = getNumbers(*expression.children()[1], defs);
  if (!call.has_value() || !ad_utility::detail::isLengthUnit(call->unit_) ||
      !numbers.has_value()) {
    return Opaque{};
  }

  // Convert the largest number from the unit of the distance to meters.
  //
  // NOTE: A number in meters is not converted to kilometers and back (which may
  // change it). For the other units, the conversion is exact only up to the
  // rounding of the conversion factor.
  auto [min, max] = ql::ranges::minmax(numbers.value());
  double meters =
      call->unit_ == UnitOfMeasurement::METERS
          ? max
          : ad_utility::detail::valueInUnitToKilometer(max, call->unit_) * 1000;
  return DistanceComparison{Distance{call->left_, call->right_}, op.value(),
                            meters, min == max};
}

}  // namespace

// _____________________________________________________________________________
std::vector<Conjunct> canonicalize(
    const sparqlExpression::SparqlExpressionPimpl& filter,
    const Definitions& defs) {
  return ad_utility::transform(
      filter.splitConjunction(),
      [&defs](sparqlExpression::SparqlExpressionPimpl conjunct) {
        auto canonical = toCanonicalForm(*conjunct.getPimpl(), defs);
        return Conjunct{std::move(conjunct), std::move(canonical)};
      });
}

}  // namespace filterFacts
