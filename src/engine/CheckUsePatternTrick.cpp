//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "CheckUsePatternTrick.h"

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
  return std::any_of(graphPattern._graphPatterns.begin(),
                     graphPattern._graphPatterns.end(), check);
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
            if (triple._s == variable.name() ||
                // TODO<joka921> The check for the property path is rather
                // hacky. That class should be refactored.
                ad_utility::contains(triple._p.asString(), variable.name()) ||
                triple._o == variable.name()) {
              return true;
            }
            return false;
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
bool checkUsePatternTrick(ParsedQuery* pq, SparqlTriple* patternTrickTriple) {
  // Check if the query has the right number of variables for aliases and
  // group by.
  if (!pq->hasSelectClause()) {
    return false;
  }
  const auto& selectClause = pq->selectClause();
  auto aliases = selectClause.getAliases();
  if (pq->_groupByVariables.size() != 1 || aliases.size() > 1) {
    return false;
  }

  bool returnsCounts = aliases.size() == 1;

  // These will only be set if the query returns the count of predicates
  // The variable the COUNT alias counts.
  std::string countedVariable;
  bool countedVariableIsDistinct;

  if (returnsCounts) {
    // We have already verified above that there is exactly one alias.
    const Alias& alias = aliases.front();
    auto countVariable =
        alias._expression.getVariableForDistinctCountOrNullopt();
    if (!countVariable.has_value()) {
      return false;
    }
    countedVariable = countVariable.value().variable_.name();
    countedVariableIsDistinct = countVariable.value().isDistinct_;
  }

  if (pq->children().empty()) {
    return false;
  }

  // We currently accept the pattern trick triple anywhere in the query.
  // TODO<joka921, hannah> Discuss whether it is also ok before an OPTIONAL or
  // MINUS etc.
  // TODO<joka921> This loop can be made much easier using ranges and view once
  // they are supported by clang.
  for (auto& pattern : pq->children()) {
    auto* curPattern = std::get_if<p::BasicGraphPattern>(&pattern);
    if (!curPattern) {
      continue;
    }

    for (size_t i = 0; i < curPattern->_triples.size(); i++) {
      const SparqlTriple& t = curPattern->_triples[i];
      // Check that the triples predicates is the HAS_PREDICATE_PREDICATE.
      // Also check that the triples object or subject matches the aliases input
      // variable and the group by variable.

      struct S {
        Variable predicateVariable_;
        Variable allowedCountVariable_;
        std::vector<Variable> variablesNotAllowedInRestOfQuery_;
        bool countMustBeDistinct_;
      };

      const auto isTriplePossible = [&]() -> std::optional<S> {
        if ((t._p._iri == HAS_PREDICATE_PREDICATE) && isVariable(t._o)) {
          Variable predicateVariable{t._o.getString()};
          if (isVariable(t._s)) {
            return S{predicateVariable,
                     Variable{t._s.getString()},
                     {predicateVariable},
                     true};
          } else {
            return S{predicateVariable,
                     predicateVariable,
                     {predicateVariable},
                     false};
          }
        } else if (isVariable(t._s) && isVariable(t._p) && isVariable(t._o)) {
          Variable predicateVariable{t._p.getIri()};
          return S{predicateVariable,
                   Variable{t._s.getString()},
                   {predicateVariable, Variable{t._o.getString()}},
                   true};
        } else {
          return std::nullopt;
        }
      }();

      if (!isTriplePossible.has_value()) {
        continue;
      }
      const S& data = isTriplePossible.value();

      if (pq->_groupByVariables[0] != data.predicateVariable_) {
        continue;
      }

      if (returnsCounts &&
          (countedVariable != data.allowedCountVariable_.name() ||
           data.countMustBeDistinct_ != countedVariableIsDistinct)) {
        continue;
      }

      // Check that the pattern trick triple is the only place in the query
      // where the predicate variable (and the object variable in the three
      // variables case) occurs.
      if (std::ranges::any_of(data.variablesNotAllowedInRestOfQuery_,
                              [&](const Variable& variable) {
                                return isVariableContainedInGraphPattern(
                                    variable, pq->_rootGraphPattern, &t);
                              })) {
        continue;
      }

      LOG(DEBUG) << "Using the pattern trick to answer the query." << endl;

      *patternTrickTriple = t;
      // Remove the triple from the graph. Note that this invalidates the
      // reference `t`, so we perform this step at the very end.
      curPattern->_triples.erase(curPattern->_triples.begin() + i);
      return true;
    }
  }
  // No suitable triple for the pattern trick was found.
  return false;
}
}  // namespace checkUsePatternTrick
