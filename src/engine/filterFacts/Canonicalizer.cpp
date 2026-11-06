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
#include "parser/ParsedQuery.h"
#include "parser/VariableCounter.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// Return true iff the target of `BIND(expression AS ?v)` is equivalent to
// `expression`, given the variables that are always bound before the `BIND`.
bool isSubstitutable(const SparqlExpression& expression,
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
      // The target of a constant is always bound to it, like a `VALUES`
      // clause with a single row. The target of any other expression is not
      // always bound (the expression may be undefined).
      if (auto constant = bind->_expression.getConstantValue()) {
        alwaysBound.insert(bind->_target);
        defs.constants_.emplace(bind->_target,
                                std::vector{std::move(constant).value()});
      } else if (const auto* expression = bind->_expression.getPimpl();
                 isSubstitutable(*expression, alwaysBound)) {
        defs.expressions_.emplace(bind->_target, expression);
      }
    }
  }
  return defs;
}

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
