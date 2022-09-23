//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "CheckUsePatternTrick.h"

#include <algorithm>

namespace checkUsePatternTrick {
// __________________________________________________________________________
bool isVariableContainedInGraphPattern(
    const Variable& variable, const ParsedQuery::GraphPattern& graphPattern,
    const SparqlTriple* tripleToIgnore) {
  for (const SparqlFilter& filter : graphPattern._filters) {
    if (filter._lhs == variable.name() || filter._rhs == variable.name()) {
      return true;
    }
  }
  auto check = [&](const parsedQuery::GraphPatternOperation& op) {
    return isVariableContainedInGraphPatternOperation(variable, op,
                                                      tripleToIgnore);
  };
  return std::ranges::any_of(graphPattern._graphPatterns, check);
}

namespace p = parsedQuery;

// __________________________________________________________________________
bool isVariableContainedInGraphPatternOperation(
    const Variable& variable,
    const parsedQuery::GraphPatternOperation& operation,
    const SparqlTriple* tripleToIgnore) {
  auto check = [&](const parsedQuery::GraphPattern& pattern) {
    return isVariableContainedInGraphPattern(variable, pattern, tripleToIgnore);
  };
  return operation.visit([&](auto&& arg) -> bool {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, p::Optional> ||
                  std::is_same_v<T, p::GroupGraphPattern> ||
                  std::is_same_v<T, p::Minus>) {
      return check(arg._child);
    } else if constexpr (std::is_same_v<T, p::Union>) {
      return check(arg._child1) || check(arg._child2);
    } else if constexpr (std::is_same_v<T, p::Subquery>) {
      // Subqueries always are SELECT clauses.
      const auto& selectClause = arg.get().selectClause();
      return ad_utility::contains(selectClause.getSelectedVariables(),
                                  variable);
    } else if constexpr (std::is_same_v<T, p::Bind>) {
      return ad_utility::contains(arg.containedVariables(), variable);
    } else if constexpr (std::is_same_v<T, p::BasicGraphPattern>) {
      return ad_utility::contains_if(
          arg._triples, [&](const SparqlTriple& triple) {
            if (&triple == tripleToIgnore) {
              return false;
            }
            return (
                triple._s == variable.name() ||
                // TODO<joka921> The check for the property path is rather
                // hacky. That class should be refactored.
                ad_utility::contains(triple._p.asString(), variable.name()) ||
                triple._o == variable.name());
          });
    } else if constexpr (std::is_same_v<T, p::Values>) {
      return ad_utility::contains(arg._inlineValues._variables,
                                  variable.name());
    } else {
      static_assert(std::is_same_v<T, p::TransPath>);
      // The `TransPath` is set up later in the query planning, when this
      // function should not be called anymore.
      AD_FAIL();
    }
    return false;
  });
}

// ____________________________________________________________________________
std::optional<PatternTrickTuple> checkUsePatternTrick(ParsedQuery* parsedQuery) {
  // Check if the query has the right number of variables for aliases and
  // group by.

  // TODO<joka921> We could actually make this work for CONSTRUCT clauses.
  if (!parsedQuery->hasSelectClause()) {
    return std::nullopt;
  }
  auto aliases = parsedQuery->selectClause().getAliases();
  if (parsedQuery->_groupByVariables.size() != 1 || aliases.size() > 1) {
    return std::nullopt;
  }

  // TODO<joka921> The returnsCounts is only used for the initial checks,
  // later on it can always be written as `countedVariable.has_value()`
  bool returnsCounts = aliases.size() == 1;

  // The variable that is the argument of the COUNT.
  std::optional<
      sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>
      countedVariable;
  if (returnsCounts) {
    // We have already verified above that there is exactly one alias.
    countedVariable = aliases.front()._expression.getVariableForCount();
    if (!countedVariable.has_value()) {
      return std::nullopt;
    }
  }
  AD_CHECK(returnsCounts == countedVariable.has_value());

  // We currently accept the pattern trick triple anywhere in the query.
  // TODO<joka921, hannah> Discuss whether it is also ok before an OPTIONAL or
  // MINUS etc.
  // TODO<joka921> This loop can be made much easier using ranges and view once
  // they are supported by clang.
  for (auto& pattern : parsedQuery->children()) {
    auto* curPattern = std::get_if<p::BasicGraphPattern>(&pattern);
    if (!curPattern) {
      continue;
    }

    // Try to find a triple that either has `ql:has-predicate` as the predicate,
    // or consists of three variables, and fulfills all the other preconditions
    // for the pattern trick.
    for (size_t i = 0; i < curPattern->_triples.size(); i++) {
      const SparqlTriple& triple = curPattern->_triples[i];
      struct PatternTrickData {
        Variable predicateVariable_;
        Variable allowedCountVariable_;
        std::vector<Variable> variablesNotAllowedInRestOfQuery_;
        // TODO<joka921> implement CountAvailablePredicates for the nonDistinct case.
        bool countMustBeDistinct_;
      };

      const auto patternTrickDataIfTripleIsPossible =
          [&]() -> std::optional<PatternTrickData> {
        if ((triple._p._iri == HAS_PREDICATE_PREDICATE) && isVariable(triple._s) &&
            isVariable(triple._o)) {
          Variable predicateVariable{triple._o.getString()};
            return PatternTrickData{predicateVariable,
                                    Variable{triple._s.getString()},
                                    {predicateVariable},
                                    true};
        } else if (isVariable(triple._s) && isVariable(triple._p) &&
                   isVariable(triple._o)) {
          Variable predicateVariable{triple._p.getIri()};
          return PatternTrickData{
              predicateVariable,
              Variable{triple._s.getString()},
              {predicateVariable, Variable{triple._o.getString()}},
              true};
        } else {
          return std::nullopt;
        }
      }();

      if (!patternTrickDataIfTripleIsPossible.has_value()) {
        continue;
      }
      const PatternTrickData& patternTrickData =
          patternTrickDataIfTripleIsPossible.value();

      if (parsedQuery->_groupByVariables[0] !=
          patternTrickData.predicateVariable_) {
        continue;
      }

      // If the query returns a COUNT then the part of the `patternTrickData`
      // that refers to the COUNT has to match.
      if (returnsCounts && !(countedVariable.value().variable_ ==
                                 patternTrickData.allowedCountVariable_ &&
                             patternTrickData.countMustBeDistinct_ ==
                                 countedVariable.value().isDistinct_)) {
        continue;
      }

      // Check that the pattern trick triple is the only place in the query
      // where the predicate variable (and the object variable in the three
      // variables case) occurs.
      if (std::ranges::any_of(
              patternTrickData.variablesNotAllowedInRestOfQuery_,
              [&](const Variable& variable) {
                return isVariableContainedInGraphPattern(
                    variable, parsedQuery->_rootGraphPattern, &triple);
              })) {
        continue;
      }

      LOG(DEBUG) << "Using the pattern trick to answer the query." << endl;

      PatternTrickTuple patternTrickTuple {Variable{triple._s.getString()}, patternTrickData.predicateVariable_};
      // Remove the triple from the graph. Note that this invalidates the
      // reference `triple`, so we perform this step at the very end.
      curPattern->_triples.erase(curPattern->_triples.begin() + i);
      return patternTrickTuple;
    }
  }
  // No suitable triple for the pattern trick was found.
  return std::nullopt;
}
}  // namespace checkUsePatternTrick
