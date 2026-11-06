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
#include "engine/sparqlExpressions/NowDatetimeExpression.h"
#include "engine/sparqlExpressions/QueryRewriteExpressionHelpers.h"
#include "engine/sparqlExpressions/RelationalExpressions.h"
#include "parser/ParsedQuery.h"
#include "util/UnitOfMeasurement.h"

namespace filterFacts {

using sparqlExpression::SparqlExpression;
using valueIdComparators::Comparison;

namespace {

// Return true iff `expression` contains `NOW()`.
bool containsNow(const SparqlExpression& expression) {
  if (dynamic_cast<const sparqlExpression::NowDatetimeExpression*>(
          &expression)) {
    return true;
  }
  return ql::ranges::any_of(expression.children(), [](const auto& child) {
    return containsNow(*child);
  });
}

// Return true iff the target of `BIND(expression AS ?v)` is equivalent to
// `expression`, given the variables that are always bound before the `BIND`.
bool isSubstitutable(const SparqlExpression& expression,
                     const ad_utility::HashSet<Variable>& alwaysBound) {
  std::vector<const SparqlExpression*> exists;
  expression.getExistsExpressions(exists);
  return expression.isDeterministic() && !containsNow(expression) &&
         exists.empty() &&
         ql::ranges::all_of(expression.containedVariables(),
                            [&alwaysBound](const Variable* variable) {
                              return alwaysBound.contains(*variable);
                            });
}

// Add the variables that `operation` may bind to `variables`. Return false if
// they are not known (for the less common kinds of operations), then any
// variable may be bound.
bool collectBoundVariables(const parsedQuery::GraphPatternOperation& operation,
                           ad_utility::HashSet<Variable>& variables) {
  namespace p = parsedQuery;
  auto collectAll = [&variables](const ParsedQuery::GraphPattern& pattern) {
    return ql::ranges::all_of(pattern._graphPatterns,
                              [&variables](const auto& child) {
                                return collectBoundVariables(child, variables);
                              });
  };
  return operation.visit([&variables, &collectAll](const auto& arg) {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, p::BasicGraphPattern>) {
      arg.collectAllContainedVariables(variables);
      return true;
    } else if constexpr (std::is_same_v<T, p::Values>) {
      variables.insert(arg._inlineValues._variables.begin(),
                       arg._inlineValues._variables.end());
      return true;
    } else if constexpr (std::is_same_v<T, p::Bind>) {
      variables.insert(arg._target);
      return true;
    } else if constexpr (std::is_same_v<T, p::Minus>) {
      return true;
    } else if constexpr (std::is_same_v<T, p::Optional>) {
      return collectAll(arg._child);
    } else if constexpr (std::is_same_v<T, p::GroupGraphPattern>) {
      if (const auto* graphVariable =
              std::get_if<p::GroupGraphPattern::GraphVar>(&arg.graphSpec_)) {
        variables.insert(graphVariable->first);
      }
      return collectAll(arg._child);
    } else if constexpr (std::is_same_v<T, p::Union>) {
      return collectAll(arg._child1) && collectAll(arg._child2);
    } else if constexpr (std::is_same_v<T, p::Subquery>) {
      const auto& visible = arg.get().getVisibleVariables();
      variables.insert(visible.begin(), visible.end());
      return true;
    } else {
      return false;
    }
  });
}

}  // namespace

// _____________________________________________________________________________
Definitions collectDefinitions(
    const std::vector<parsedQuery::GraphPatternOperation>& group) {
  Definitions defs;
  // The variables that are always bound by the patterns so far. Only triples
  // and `VALUES` count (ignoring nested groups etc. is conservative).
  ad_utility::HashSet<Variable> alwaysBound;
  for (const auto& operation : group) {
    // Remove the `BIND`s whose target `operation` may bind again (where the
    // expression is undefined, the target is then bound freely, so it is not
    // equivalent to the expression).
    ad_utility::HashSet<Variable> boundByOperation;
    bool isKnown = collectBoundVariables(operation, boundByOperation);
    absl::erase_if(defs.expressions_, [&](const auto& definition) {
      return !isKnown || boundByOperation.contains(definition.first);
    });

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
      // The target is not always bound (the expression may be undefined).
      const auto* expression = bind->_expression.getPimpl();
      if (isSubstitutable(*expression, alwaysBound)) {
        defs.expressions_.emplace(bind->_target, expression);
      }
    }
  }
  return defs;
}

namespace {

// Return the operator if `expression` is a comparison with `<` or `<=`.
std::optional<Comparison> getComparison(const SparqlExpression& expression) {
  if (dynamic_cast<const sparqlExpression::LessThanExpression*>(&expression)) {
    return Comparison::LT;
  } else if (dynamic_cast<const sparqlExpression::LessEqualExpression*>(
                 &expression)) {
    return Comparison::LE;
  }
  return std::nullopt;
}

// Return the value of `expression` if it is a number (`xsd:int` or
// `xsd:double`), and `std::nullopt` otherwise (e.g. for the string "500").
std::optional<double> getNumber(const SparqlExpression& expression) {
  const auto* constant =
      dynamic_cast<const sparqlExpression::IdExpression*>(&expression);
  if (constant == nullptr) {
    return std::nullopt;
  }
  const Id& id = constant->value();
  if (id.getDatatype() == Datatype::Int) {
    return static_cast<double>(id.getInt());
  } else if (id.getDatatype() == Datatype::Double) {
    return id.getDouble();
  }
  return std::nullopt;
}

// Return the canonical form of a single conjunct: `distance < number` and
// `distance <= number` become a `Constraint`, everything else is `Opaque`.
Conjunct toConjunct(const SparqlExpression& expression) {
  auto op = getComparison(expression);
  if (!op.has_value()) {
    return Opaque{&expression};
  }
  auto call = sparqlExpression::getGeoDistanceExpressionParameters(
      *expression.children()[0]);
  auto number = getNumber(*expression.children()[1]);
  if (!call.has_value() || !ad_utility::detail::isLengthUnit(call->unit_) ||
      !number.has_value()) {
    return Opaque{&expression};
  }

  // Convert the number from the unit of the distance to meters (in the same
  // way as `getGeoDistanceFilter`).
  double meters =
      ad_utility::detail::valueInUnitToKilometer(number.value(), call->unit_) *
      1000;
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
