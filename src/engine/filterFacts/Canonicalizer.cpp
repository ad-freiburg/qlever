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
bool canSubstituteForTarget(
    const sparqlExpression::SparqlExpressionPimpl& expression,
    const ad_utility::HashSet<Variable>& alwaysBound) {
  return expression.isDeterministic() &&
         expression.getExistsExpressions().empty() &&
         ql::ranges::all_of(expression.containedVariables(),
                            [&alwaysBound](const Variable* variable) {
                              return alwaysBound.contains(*variable);
                            });
}

// Remove the expressions of the `BIND`s from `defs` whose target is contained
// in `operation`, because `operation` may bind the target in the rows where the
// expression is undefined.
//
// NOTE 1: For example, in `?a <p> ?x . BIND(?x + 1 AS ?v) ?b <q> ?v`, the
// `BIND` leaves `?v` unbound in a row where `?x` is not a number, and the
// triple `?b <q> ?v` then binds `?v` to any object of `<q>`. So a FILTER may
// no longer substitute `?x + 1` for `?v`.
//
// NOTE 2: A later `BIND` cannot bind the target again (the target of a `BIND`
// must not be in scope before it), so its expression may use the target.
//
// NOTE 3: The variables of a named cached result are not known at parse time
// (its body is empty), so it may bind any target.
void removeRedefinedTargets(const parsedQuery::GraphPatternOperation& operation,
                            Definitions& defs) {
  if (defs.expressions_.empty() ||
      std::holds_alternative<parsedQuery::Bind>(operation)) {
    return;
  }
  if (std::holds_alternative<parsedQuery::NamedCachedResult>(operation)) {
    defs.expressions_.clear();
    return;
  }
  parsedQuery::VariableCounter variables;
  variables(operation);
  ql::erase_if(defs.expressions_, [&variables](const auto& definition) {
    return variables.counts().contains(definition.first);
  });
}

// Add the values of each variable of `values` without `UNDEF` in its column to
// `defs`, and the variable to `alwaysBound`. If `defs` already has values for
// the variable, keep only the values that are in both (the `VALUES` clauses
// are joined).
void addValues(const parsedQuery::Values& values, Definitions& defs,
               ad_utility::HashSet<Variable>& alwaysBound) {
  const auto& [variables, rows] = values._inlineValues;
  for (size_t i = 0; i < variables.size(); ++i) {
    auto column =
        ad_utility::transform(rows, [i](const auto& row) { return row.at(i); });
    if (ql::ranges::any_of(column, &TripleComponent::isUndef)) {
      continue;
    }
    alwaysBound.insert(variables[i]);
    auto [it, isNew] = defs.constants_.try_emplace(variables[i], column);
    if (!isNew) {
      ql::erase_if(it->second, [&column](const auto& value) {
        return !ad_utility::contains(column, value);
      });
    }
  }
}

// Add the definition of `bind` to `defs`. The target of a constant other than
// `UNDEF` is always bound to it, like a `VALUES` clause with a single row. The
// target of any other expression is not always bound (the expression may be
// undefined), so its expression is only added if a FILTER may substitute it
// for the target.
void addBind(const parsedQuery::Bind& bind, Definitions& defs,
             ad_utility::HashSet<Variable>& alwaysBound) {
  auto constant = bind._expression.getConstantValue();
  if (constant.has_value() &&
      !(constant->isId() && constant->getId().isUndefined())) {
    alwaysBound.insert(bind._target);
    defs.constants_.emplace(bind._target,
                            std::vector{std::move(constant).value()});
  } else if (canSubstituteForTarget(bind._expression, alwaysBound)) {
    defs.expressions_.emplace(bind._target, bind._expression.getPimpl());
  }
}

}  // namespace

// _____________________________________________________________________________
Definitions collectDefinitions(
    ql::span<const parsedQuery::GraphPatternOperation> group) {
  Definitions defs;
  // The variables that are always bound by the patterns so far. Only triples,
  // `VALUES` and `BIND`s of constants count (ignoring nested groups etc. is
  // conservative).
  ad_utility::HashSet<Variable> alwaysBound;
  for (const auto& operation : group) {
    removeRedefinedTargets(operation, defs);
    if (const auto* bgp =
            std::get_if<parsedQuery::BasicGraphPattern>(&operation)) {
      bgp->collectAllContainedVariables(alwaysBound);
    } else if (const auto* values =
                   std::get_if<parsedQuery::Values>(&operation)) {
      addValues(*values, defs, alwaysBound);
    } else if (const auto* bind = std::get_if<parsedQuery::Bind>(&operation)) {
      addBind(*bind, defs, alwaysBound);
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

// Return the values of `variable` in `defs` (from `VALUES` or a `BIND` of a
// constant), and `std::nullopt` if it has none.
std::optional<std::vector<TripleComponent>> getConstants(
    const Variable& variable, const Definitions& defs) {
  auto it = defs.constants_.find(variable);
  if (it == defs.constants_.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

// Return the possible values of `expression` if it is a constant or a variable
// with values in `defs`, and `std::nullopt` otherwise.
std::optional<std::vector<TripleComponent>> getConstants(
    const SparqlExpression& expression, const Definitions& defs) {
  const auto& resolved = resolve(expression, defs);
  if (auto variable = resolved.getVariableOrNullopt()) {
    return getConstants(variable.value(), defs);
  }
  auto constant = resolved.getConstantValue();
  if (!constant.has_value()) {
    return std::nullopt;
  }
  return std::vector{std::move(constant).value()};
}

// Return the possible values of `expression` if it is a number (an `Int` or
// `Double` `Id`, e.g. `500` or `0.5`) or a variable with only numbers in
// `defs`, and `std::nullopt` otherwise (e.g. for the string "500").
std::optional<std::vector<double>> getNumbers(
    const SparqlExpression& expression, const Definitions& defs) {
  auto constants = getConstants(expression, defs);
  if (!constants.has_value()) {
    return std::nullopt;
  }
  std::vector<double> numbers;
  for (const auto& constant : constants.value()) {
    auto number = constant.getNumber();
    if (!number.has_value()) {
      return std::nullopt;
    }
    numbers.push_back(number.value());
  }
  return numbers;
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
      !numbers.has_value() || numbers->size() != 1) {
    return Opaque{};
  }

  // Convert the number from the unit of the distance to meters.
  //
  // NOTE: A number in meters is not converted to kilometers and back (which may
  // change it). For the other units, the conversion is exact only up to the
  // rounding of the conversion factor.
  double number = numbers->front();
  double meters =
      call->unit_ == UnitOfMeasurement::METERS
          ? number
          : ad_utility::detail::valueInUnitToKilometer(number, call->unit_) *
                1000;
  return DistanceComparison{Distance{call->left_, call->right_}, op.value(),
                            meters};
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
