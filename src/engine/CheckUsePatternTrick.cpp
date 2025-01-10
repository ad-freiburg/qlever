//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "./CheckUsePatternTrick.h"

#include <ranges>
#include <type_traits>

#include "backports/algorithm.h"
#include "parser/GraphPatternOperation.h"

namespace checkUsePatternTrick {
// __________________________________________________________________________
bool isVariableContainedInGraphPattern(
    const Variable& variable, const ParsedQuery::GraphPattern& graphPattern,
    const SparqlTriple* tripleToIgnore) {
  if (ql::ranges::any_of(
          graphPattern._filters, [&variable](const SparqlFilter& filter) {
            return filter.expression_.isVariableContained(variable);
          })) {
    return true;
  }
  auto check = [&](const parsedQuery::GraphPatternOperation& op) {
    return isVariableContainedInGraphPatternOperation(variable, op,
                                                      tripleToIgnore);
  };
  return ql::ranges::any_of(graphPattern._graphPatterns, check);
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
            return (triple.s_ == variable ||
                    // Complex property paths are not allowed to contain
                    // variables in SPARQL, so this check is sufficient.
                    // TODO<joka921> Still make the interface of the
                    // `PropertyPath` class typesafe.
                    triple.p_.asString() == variable.name() ||
                    triple.o_ == variable);
          });
    } else if constexpr (std::is_same_v<T, p::Values>) {
      return ad_utility::contains(arg._inlineValues._variables, variable);
    } else if constexpr (std::is_same_v<T, p::Service>) {
      return ad_utility::contains(arg.visibleVariables_, variable);
    } else {
      static_assert(
          std::is_same_v<T, p::TransPath> || std::is_same_v<T, p::PathQuery> ||
          std::is_same_v<T, p::Describe> || std::is_same_v<T, p::SpatialQuery>);
      // The `TransPath` is set up later in the query planning, when this
      // function should not be called anymore.
      AD_FAIL();
    }
  });
}

using ValuesClause = std::optional<parsedQuery::Values>;
// TODO<joka921> How many possible return values do we need here.
bool addValuesClauseToPattern(parsedQuery::GraphPatternOperation& operation,
                              const ValuesClause& clause);

// __________________________________________________________________________
void addValuesClause(ParsedQuery::GraphPattern& graphPattern,
                     const ValuesClause& values, bool recurse) {
  // TODO<joka921> Do we want to do this, or do we only want this if the values
  // clause hasn't been handled downstream.
  /*
  bool containedInFilter = ql::ranges::any_of(
      graphPattern._filters, [&values](const SparqlFilter& filter) {
        return ql::ranges::any_of(
            values._inlineValues._variables, [&filter](const Variable& var) {
              return filter.expression_.isVariableContained(var);
            });
      });
      */
  [[maybe_unused]] const bool containedInFilter = false;
  auto check = [&](parsedQuery::GraphPatternOperation& op) {
    return addValuesClauseToPattern(op, values);
  };
  // TODO<joka921> We have to figure out the correct positioning of the values
  // clause, s.t. we don't get cartesian products because of optimization
  // barriers like bind/Optional/Minus etc.
  std::optional<size_t> insertPosition;
  if (values.has_value()) {
    for (const auto& [i, pattern] :
         ::ranges::views::enumerate(graphPattern._graphPatterns)) {
      if (check(pattern)) {
        insertPosition = i;
      }
    }
  }

  if (!recurse) {
    return;
  }
  if (insertPosition.has_value()) {
    graphPattern._graphPatterns.insert(
        graphPattern._graphPatterns.begin() + insertPosition.value(),
        values.value());
  }

  std::vector<ValuesClause> foundClauses;
  for (const auto& pattern : graphPattern._graphPatterns) {
    if (auto* foundValues = std::get_if<parsedQuery::Values>(&pattern)) {
      foundClauses.push_back(*foundValues);
    }
  }
  for (const auto& foundValue : foundClauses) {
    addValuesClause(graphPattern, foundValue, false);
  }

  if (foundClauses.empty()) {
    for (auto& pattern : graphPattern._graphPatterns) {
      addValuesClauseToPattern(pattern, std::nullopt);
    }
  }
}

// __________________________________________________________________________
bool addValuesClauseToPattern(parsedQuery::GraphPatternOperation& operation,
                              const ValuesClause& result) {
  auto check = [&](parsedQuery::GraphPattern& pattern) {
    addValuesClause(pattern, result);
    return false;
  };
  const std::vector<Variable> emptyVars{};
  const auto& variables =
      result.has_value() ? result.value()._inlineValues._variables : emptyVars;
  auto anyVar = [&](auto f) { return ql::ranges::any_of(variables, f); };
  return operation.visit([&](auto&& arg) -> bool {
    using T = std::decay_t<decltype(arg)>;
    if constexpr (std::is_same_v<T, p::Optional> ||
                  std::is_same_v<T, p::GroupGraphPattern> ||
                  std::is_same_v<T, p::Minus>) {
      return check(arg._child);
    } else if constexpr (std::is_same_v<T, p::Union>) {
      check(arg._child1);
      check(arg._child2);
      return false;
    } else if constexpr (std::is_same_v<T, p::Subquery>) {
      // Subqueries always are SELECT clauses.
      const auto& selectClause = arg.get().selectClause();

      if (anyVar([&selectClause](const auto& var) {
            return ad_utility::contains(selectClause.getSelectedVariables(),
                                        var);
          })) {
        return check(arg.get()._rootGraphPattern);
      } else {
        // Also recurse into the subquery, but not with the given `VALUES`
        // clause.
        addValuesClause(arg.get()._rootGraphPattern, std::nullopt);
        return false;
      }
    } else if constexpr (std::is_same_v<T, p::Bind>) {
      return ql::ranges::any_of(variables, [&](const auto& variable) {
        return ad_utility::contains(arg.containedVariables(), variable);
      });
    } else if constexpr (std::is_same_v<T, p::BasicGraphPattern>) {
      return ad_utility::contains_if(
          arg._triples, [&](const SparqlTriple& triple) {
            return anyVar([&](const auto& variable) {
              return (triple.s_ == variable ||
                      // Complex property paths are not allowed to contain
                      // variables in SPARQL, so this check is sufficient.
                      // TODO<joka921> Still make the interface of the
                      // `PropertyPath` class typesafe.
                      triple.p_.asString() == variable.name() ||
                      triple.o_ == variable);
            });
          });
    } else if constexpr (std::is_same_v<T, p::Values>) {
      return anyVar([&](const auto& variable) {
        return ad_utility::contains(arg._inlineValues._variables, variable);
      });
    } else if constexpr (std::is_same_v<T, p::Service>) {
      return anyVar([&](const auto& variable) {
        return ad_utility::contains(arg.visibleVariables_, variable);
      });
    } else {
      static_assert(
          std::is_same_v<T, p::TransPath> || std::is_same_v<T, p::PathQuery> ||
          std::is_same_v<T, p::Describe> || std::is_same_v<T, p::SpatialQuery>);
      // TODO<joka921> This is just an optimization, so we can always just omit
      // it, but it would be nice to also apply this optimization for those
      // types of queries.
      return false;
    }
  });
}

// Internal helper function.
// Modify the `triples` s.t. the patterns for `subAndPred.subject_` will
// appear in a column with the variable `subAndPred.predicate_` when
// evaluating and joining all the triples. This can be either done by
// retrieving one of the additional columns where the patterns are stored in
// the PSO and POS permutation or, if no triple suitable for adding this
// column exists, by adding a triple `?subject ql:has-pattern ?predicate`.
static void rewriteTriplesForPatternTrick(const PatternTrickTuple& subAndPred,
                                          std::vector<SparqlTriple>& triples) {
  // The following lambda tries to find a triple in the `triples` that has the
  // subject variable of the pattern trick in its `triplePosition` (which is
  // either the subject or the object) and a fixed predicate (no variable). If
  // such a triple is found, it is modified s.t. it also scans the
  // `additionalScanColumn` which has to be the index of the column where the
  // patterns of the `triplePosition` are stored in the POS and PSO
  // permutation. Return true iff such a triple was found and replaced.
  auto findAndRewriteMatchingTriple = [&subAndPred, &triples](
                                          auto triplePosition,
                                          size_t additionalScanColumn) {
    auto matchingTriple = ql::ranges::find_if(
        triples, [&subAndPred, triplePosition](const SparqlTriple& t) {
          return std::invoke(triplePosition, t) == subAndPred.subject_ &&
                 t.p_.isIri() && !isVariable(t.p_);
        });
    if (matchingTriple == triples.end()) {
      return false;
    }
    matchingTriple->additionalScanColumns_.emplace_back(additionalScanColumn,
                                                        subAndPred.predicate_);
    return true;
  };

  if (findAndRewriteMatchingTriple(&SparqlTriple::s_,
                                   ADDITIONAL_COLUMN_INDEX_SUBJECT_PATTERN)) {
    return;
  } else if (findAndRewriteMatchingTriple(
                 &SparqlTriple::o_, ADDITIONAL_COLUMN_INDEX_OBJECT_PATTERN)) {
    return;
  } else {
    // We could not find a suitable triple to append the additional column, we
    // therefore add an explicit triple `?s ql:has_pattern ?p`
    triples.emplace_back(subAndPred.subject_,
                         std::string{HAS_PATTERN_PREDICATE},
                         subAndPred.predicate_);
  }
}

// Helper function for `checkUsePatternTrick`.
// Check if any of the triples in the `graphPattern` has the form `?s
// ql:has-predicate ?p` or `?s ?p ?o` and that the other conditions for the
// pattern trick are fulfilled (nameley that the variables `?p` and if present
// `?o` don't appear elsewhere in the `parsedQuery`. If such a triple is
// found, the query is modified such that it behaves as if the triple was
// replace by
// `?s ql:has-pattern ?p`. See the documentation of
// `rewriteTriplesForPatternTrick` above.
static std::optional<PatternTrickTuple> findPatternTrickTuple(
    p::BasicGraphPattern* graphPattern, const ParsedQuery* parsedQuery,
    const std::optional<
        sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>&
        countedVariable) {
  // Try to find a triple that either has `ql:has-predicate` as the predicate,
  // or consists of three variables, and fulfills all the other preconditions
  // for the pattern trick.
  auto& triples = graphPattern->_triples;
  for (auto it = triples.begin(); it != triples.end(); ++it) {
    auto patternTrickTuple =
        isTripleSuitableForPatternTrick(*it, parsedQuery, countedVariable);
    if (!patternTrickTuple.has_value()) {
      continue;
    }
    triples.erase(it);
    rewriteTriplesForPatternTrick(patternTrickTuple.value(), triples);
    return patternTrickTuple;
  }
  return std::nullopt;
}

// ____________________________________________________________________________
std::optional<PatternTrickTuple> checkUsePatternTrick(
    ParsedQuery* parsedQuery) {
  // Check if the query has the right number of variables for aliases and
  // group by.

  const auto& aliases = parsedQuery->getAliases();
  if (parsedQuery->_groupByVariables.size() != 1 || aliases.size() > 1) {
    return std::nullopt;
  }

  // The variable that is the argument of the COUNT.
  std::optional<
      sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>
      countedVariable;
  if (aliases.size() == 1) {
    // We have already verified above that there is exactly one alias.
    countedVariable = aliases.front()._expression.getVariableForCount();
    if (!countedVariable.has_value()) {
      return std::nullopt;
    }
  }

  // We currently accept the pattern trick triple anywhere in the query.
  // TODO<joka921> This loop can be made much easier using ranges and view
  // once they are supported by clang.
  for (auto& pattern : parsedQuery->children()) {
    auto* curPattern = std::get_if<p::BasicGraphPattern>(&pattern);
    if (!curPattern) {
      continue;
    }

    auto patternTrickTuple =
        findPatternTrickTuple(curPattern, parsedQuery, countedVariable);
    if (patternTrickTuple.has_value()) {
      return patternTrickTuple;
    }
  }
  // No suitable triple for the pattern trick was found.
  return std::nullopt;
}

// _____________________________________________________________________________
std::optional<PatternTrickTuple> isTripleSuitableForPatternTrick(
    const SparqlTriple& triple, const ParsedQuery* parsedQuery,
    const std::optional<
        sparqlExpression::SparqlExpressionPimpl::VariableAndDistinctness>&
        countedVariable) {
  struct PatternTrickData {
    Variable predicateVariable_;
    Variable allowedCountVariable_;
    std::vector<Variable> variablesNotAllowedInRestOfQuery_;
    // TODO<joka921> implement CountAvailablePredicates for the nonDistinct
    // case.
    bool countMustBeDistinct_;
  };

  const auto patternTrickDataIfTripleIsPossible =
      [&]() -> std::optional<PatternTrickData> {
    if ((triple.p_._iri == HAS_PREDICATE_PREDICATE) && isVariable(triple.s_) &&
        isVariable(triple.o_) && triple.s_ != triple.o_) {
      Variable predicateVariable{triple.o_.getVariable()};
      return PatternTrickData{predicateVariable,
                              triple.s_.getVariable(),
                              {predicateVariable},
                              true};
    } else if (isVariable(triple.s_) && isVariable(triple.p_) &&
               isVariable(triple.o_)) {
      // Check that the three variables are pairwise distinct.
      std::vector<string> variables{triple.s_.getVariable().name(),
                                    triple.o_.getVariable().name(),
                                    triple.p_.asString()};
      ql::ranges::sort(variables);
      if (std::unique(variables.begin(), variables.end()) != variables.end()) {
        return std::nullopt;
      }

      Variable predicateVariable{triple.p_.getIri()};
      return PatternTrickData{predicateVariable,
                              triple.s_.getVariable(),
                              {predicateVariable, triple.o_.getVariable()},
                              true};
    } else {
      return std::nullopt;
    }
  }();

  if (!patternTrickDataIfTripleIsPossible.has_value()) {
    return std::nullopt;
  }
  const PatternTrickData& patternTrickData =
      patternTrickDataIfTripleIsPossible.value();

  if (parsedQuery->_groupByVariables[0] !=
      patternTrickData.predicateVariable_) {
    return std::nullopt;
  }

  // If the query returns a COUNT then the part of the `patternTrickData`
  // that refers to the COUNT has to match.
  if (countedVariable.has_value() &&
      !(countedVariable.value().variable_ ==
            patternTrickData.allowedCountVariable_ &&
        patternTrickData.countMustBeDistinct_ ==
            countedVariable.value().isDistinct_)) {
    return std::nullopt;
  }

  // Check that the pattern trick triple is the only place in the query
  // where the predicate variable (and the object variable in the three
  // variables case) occurs.
  if (ql::ranges::any_of(patternTrickData.variablesNotAllowedInRestOfQuery_,
                         [&](const Variable& variable) {
                           return isVariableContainedInGraphPattern(
                               variable, parsedQuery->_rootGraphPattern,
                               &triple);
                         })) {
    return std::nullopt;
  }

  PatternTrickTuple patternTrickTuple{triple.s_.getVariable(),
                                      patternTrickData.predicateVariable_};
  return patternTrickTuple;
}
}  // namespace checkUsePatternTrick
