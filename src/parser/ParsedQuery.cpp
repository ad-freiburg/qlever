// Copyright 2014 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2014 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "parser/ParsedQuery.h"

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "backports/StartsWithAndEndsWith.h"
#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "global/RuntimeParameters.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "util/TransparentFunctors.h"

using std::string;
using std::vector;

// _____________________________________________________________________________
string SparqlPrefix::asString() const {
  std::ostringstream os;
  os << "{" << _prefix << ": " << _uri << "}";
  return std::move(os).str();
}

// _____________________________________________________________________________
string SparqlTriple::asString() const {
  std::ostringstream os;
  os << "{s: " << s_ << ", p: "
     << std::visit(ad_utility::OverloadCallOperator{
                       [](const Variable& var) { return var.name(); },
                       [](const PropertyPath& propertyPath) {
                         return propertyPath.asString();
                       }},
                   p_)
     << ", o: " << o_ << "}";
  return std::move(os).str();
}

// ________________________________________________________________________
Variable ParsedQuery::addInternalBind(
    sparqlExpression::SparqlExpressionPimpl expression,
    InternalVariableGenerator internalVariableGenerator) {
  // Internal variable name to which the result of the helper bind is
  // assigned.
  auto targetVariable = internalVariableGenerator();
  // Don't register the targetVariable as visible because it is used
  // internally and should not be selected by SELECT * (this is the `bool`
  // argument to `addBind`).
  // TODO<qup42, joka921> Implement "internal" variables, that can't be
  //  selected at all and can never interfere with variables from the
  //  query.
  addBind(std::move(expression), targetVariable, false);
  return targetVariable;
}

// ________________________________________________________________________
Variable ParsedQuery::addInternalAlias(
    sparqlExpression::SparqlExpressionPimpl expression,
    InternalVariableGenerator internalVariableGenerator) {
  // Internal variable name to which the result of the helper bind is
  // assigned.
  auto targetVariable = internalVariableGenerator();
  // Don't register the targetVariable as visible because it is used
  // internally and should not be visible to the user.
  addInternalAlias(Alias{std::move(expression), targetVariable});
  return targetVariable;
}

// ________________________________________________________________________
void ParsedQuery::addInternalAlias(Alias alias) {
  if (hasSelectClause()) {
    selectClause().addAlias(std::move(alias), true);
  } else {
    internalAliasesWithoutSelectClause_.push_back(std::move(alias));
  }
}

// ________________________________________________________________________
std::vector<Alias> ParsedQuery::takeAliases() {
  if (hasSelectClause()) {
    return selectClause().deleteAliasesButKeepVariables();
  }
  return std::exchange(internalAliasesWithoutSelectClause_, {});
}

// ________________________________________________________________________
void ParsedQuery::addBind(sparqlExpression::SparqlExpressionPimpl expression,
                          Variable targetVariable, bool targetIsVisible) {
  if (targetIsVisible) {
    registerVariableVisibleInQueryBody(targetVariable);
  }
  parsedQuery::Bind bind{std::move(expression), std::move(targetVariable)};
  _rootGraphPattern._graphPatterns.emplace_back(std::move(bind));
}

// ________________________________________________________________________
void ParsedQuery::addSolutionModifiers(
    SolutionModifiers modifiers,
    InternalVariableGenerator internalVariableGenerator,
    std::optional<parsedQuery::Values> postQueryValues) {
  // Process groupClause
  addGroupByClause(std::move(modifiers.groupByVariables_),
                   internalVariableGenerator);

  const bool isExplicitGroupBy = !_groupByVariables.empty();
  const bool isImplicitGroupBy =
      ql::ranges::any_of(getAliases(),
                         [](const Alias& alias) {
                           return alias._expression.containsAggregate();
                         }) &&
      !isExplicitGroupBy;
  const bool isGroupBy = isExplicitGroupBy || isImplicitGroupBy;
  using namespace std::string_literals;
  std::string noteForImplicitGroupBy =
      isImplicitGroupBy
          ? " Note: The GROUP BY in this query is implicit because an aggregate expression was used in the SELECT clause"s
          : ""s;
  std::string noteForGroupByError =
      " All non-aggregated variables must be part of the GROUP BY "
      "clause." +
      noteForImplicitGroupBy;

  // Process HAVING clause
  addHavingClause(std::move(modifiers.havingClauses_), isGroupBy,
                  internalVariableGenerator);

  // The trailing `VALUES` clause is joined after `GROUP BY` and `HAVING`, but
  // before the `SELECT` expressions and `ORDER BY` (SPARQL 1.1, sec. 18.2.4.3).
  // Without `GROUP BY`, we join it with the `WHERE` clause here, before the
  // `BIND`s for the `ORDER BY` expressions and the `SELECT` aliases are added
  // below. With `GROUP BY`, the `QueryPlanner` joins it after the `GroupBy`,
  // and the aliases that use its variables are computed after that join, see
  // `moveAliasesAfterPostQueryValues`.
  //
  // NOTE: Without `GROUP BY`, the `WHERE` clause gets its own group, such that
  // its `FILTER`s don't see the `VALUES` variables.
  if (postQueryValues.has_value()) {
    registerVariablesVisibleInQueryBody(
        postQueryValues->_inlineValues._variables);
    if (isGroupBy) {
      postQueryValuesClause_ = std::move(postQueryValues);
    } else {
      auto where = std::exchange(_rootGraphPattern, GraphPattern{});
      _rootGraphPattern._graphPatterns.emplace_back(
          parsedQuery::GroupGraphPattern{std::move(where)});
      _rootGraphPattern._graphPatterns.emplace_back(
          std::move(postQueryValues.value()));
    }
  }

  // Process ORDER BY clause
  addOrderByClause(std::move(modifiers.orderBy_), isGroupBy,
                   noteForImplicitGroupBy, internalVariableGenerator);

  // Process limitOffsetClause
  _limitOffset = modifiers.limitOffset_;

  auto checkAliasTargetsHaveNoOverlap = [this]() {
    ad_utility::HashMap<Variable, size_t> variable_counts;

    for (const Variable& v : selectClause().getSelectedVariables()) {
      variable_counts[v]++;
    }

    for (const auto& alias : selectClause().getAliases()) {
      if (ad_utility::contains(selectClause().getVisibleVariables(),
                               alias._target)) {
        throw InvalidSparqlQueryException(absl::StrCat(
            "The target ", alias._target.name(),
            " of an AS clause was already used in the query body."));
      }

      // The variable was already added to the selected variables while
      // parsing the alias, thus it should appear exactly once
      if (variable_counts[alias._target] > 1) {
        throw InvalidSparqlQueryException(absl::StrCat(
            "The target ", alias._target.name(),
            " of an AS clause was already used before in the SELECT clause."));
      }
    }
  };

  if (hasSelectClause()) {
    checkAliasTargetsHaveNoOverlap();

    // Check that all the variables that are used in aliases are either visible
    // in the query body or are bound by a previous alias from the same SELECT
    // clause.
    ad_utility::HashSet<Variable> variablesBoundInAliases;
    for (const auto& alias : selectClause().getAliases()) {
      checkUsedVariablesAreVisible(alias._expression, "SELECT",
                                   variablesBoundInAliases);
      variablesBoundInAliases.insert(alias._target);
    }

    if (isGroupBy) {
      // The variables of the trailing `VALUES` clause are bound after the
      // grouping, so they can be used like the grouped variables.
      ad_utility::HashSet<Variable> groupVariables{};
      for (const auto& variable : _groupByVariables) {
        groupVariables.emplace(variable);
      }
      if (postQueryValuesClause_.has_value()) {
        groupVariables.insert(
            postQueryValuesClause_->_inlineValues._variables.begin(),
            postQueryValuesClause_->_inlineValues._variables.end());
      }

      if (selectClause().isAsterisk()) {
        throw InvalidSparqlQueryException(
            "GROUP BY is not allowed when all variables are selected via "
            "SELECT *");
      }

      // Check if all selected variables are either aggregated or
      // part of the group by statement.
      const auto& aliases = selectClause().getAliases();
      for (const Variable& var : selectClause().getSelectedVariables()) {
        if (auto it = ql::ranges::find(aliases, var, &Alias::_target);
            it != aliases.end()) {
          const auto& alias = *it;
          auto relevantVariables = groupVariables;
          for (auto i = aliases.begin(); i < it; ++i) {
            relevantVariables.insert(i->_target);
          }
          if (alias._expression.isAggregate(relevantVariables)) {
            continue;
          } else {
            auto unaggregatedVars =
                alias._expression.getUnaggregatedVariables(groupVariables);
            throw InvalidSparqlQueryException(absl::StrCat(
                "The expression \"", alias._expression.getDescriptor(),
                "\" does not aggregate ",
                absl::StrJoin(unaggregatedVars, ", ", Variable::AbslFormatter),
                "." + noteForGroupByError));
          }
        }
        if (!groupVariables.contains(var)) {
          throw InvalidSparqlQueryException(absl::StrCat(
              "Variable ", var.name(), " is selected but not aggregated.",
              noteForGroupByError));
        }
      }
    } else {
      // If there is no GROUP BY clause and there is a SELECT clause, then the
      // aliases like SELECT (?x as ?y) have to be added as ordinary BIND
      // expressions to the query body. In CONSTRUCT queries there are no such
      // aliases, and in case of a GROUP BY clause the aliases are read
      // directly from the SELECT clause by the `GroupBy` operation.

      auto& selectClause = std::get<SelectClause>(_clause);
      for (const auto& alias : selectClause.getAliases()) {
        // As the clause is NOT `SELECT *` it is not required to register the
        // target variable as visible, but it helps with several sanity
        // checks.
        addBind(alias._expression, alias._target, true);
      }

      // We do not need the aliases anymore as we have converted them to BIND
      // expressions
      selectClause.deleteAliasesButKeepVariables();
    }
  } else if (hasConstructClause()) {
    if (!_groupByVariables.empty()) {
      for (const auto& variable : constructClause().containedVariables()) {
        if (!ad_utility::contains(_groupByVariables, variable) &&
            !(postQueryValuesClause_.has_value() &&
              ad_utility::contains(
                  postQueryValuesClause_->_inlineValues._variables,
                  variable))) {
          throw InvalidSparqlQueryException("Variable " + variable.name() +
                                            " is used but not aggregated." +
                                            noteForGroupByError);
        }
      }
    }
  } else {
    // TODO<joka921> refactor this to use `std::visit`. It is much safer.
    AD_CORRECTNESS_CHECK(hasAskClause());
  }

  // Only set with `GROUP BY`, see above.
  if (postQueryValuesClause_.has_value()) {
    moveAliasesAfterPostQueryValues(internalVariableGenerator);
  }
}

// _____________________________________________________________________________
const std::vector<Variable>& ParsedQuery::getVisibleVariables() const {
  return std::visit(std::mem_fn(&parsedQuery::ClauseBase::getVisibleVariables),
                    _clause);
}

// _____________________________________________________________________________
void ParsedQuery::registerVariablesVisibleInQueryBody(
    const vector<Variable>& variables) {
  for (const auto& var : variables) {
    registerVariableVisibleInQueryBody(var);
  }
}

// _____________________________________________________________________________
void ParsedQuery::registerVariableVisibleInQueryBody(const Variable& variable) {
  auto addVariable = [&variable](auto& clause) {
    if (!ql::starts_with(variable.name(), QLEVER_INTERNAL_VARIABLE_PREFIX)) {
      clause.addVisibleVariable(variable);
    }
  };
  std::visit(addVariable, _clause);
}

// __________________________________________________________________________
ParsedQuery::GraphPattern::GraphPattern() : _optional(false) {}

// __________________________________________________________________________
bool ParsedQuery::GraphPattern::addLanguageFilter(
    const Variable& variable,
    const ad_utility::HashSet<std::string>& langTags) {
  AD_CORRECTNESS_CHECK(!langTags.empty());
  // Since most literals have an empty language tag we don't create extra
  // triples for them, so we can't use this optimization.
  if (langTags.contains("")) {
    return false;
  }
  // Find all triples where the object is the `variable` and the predicate is
  // a simple `IRIREF` (neither a variable nor a complex property path).
  // Search in all the basic graph patterns, as filters have the complete
  // graph patterns as their scope.
  // TODO<joka921> In theory we could also recurse into GroupGraphPatterns,
  // Subqueries etc.
  // TODO<joka921> Also support property paths (^rdfs:label,
  // skos:altLabel|rdfs:label, ...)
  std::vector<SparqlTriple*> matchingTriples;
  using BasicPattern = parsedQuery::BasicGraphPattern;
  namespace ad = ad_utility;
  namespace stdv = ql::views;
  bool variableFoundInTriple = false;
  for (BasicPattern* basicPattern :
       _graphPatterns | stdv::transform(ad::getIf<BasicPattern>) |
           stdv::filter(ad::toBool)) {
    for (auto& triple : basicPattern->_triples) {
      auto predicate = triple.getSimplePredicate();
      if (triple.o_ == variable && predicate.has_value() &&
          !ql::starts_with(
              predicate.value(),
              QLEVER_INTERNAL_PREFIX_IRI_WITHOUT_CLOSING_BRACKET)) {
        matchingTriples.push_back(&triple);
      }
      // TODO<RobinTF> There might be more cases where the variable is matched
      // against a pattern.
      if (triple.s_ == variable || triple.o_ == variable ||
          triple.predicateIs(variable)) {
        // If at some point we find a triple using the variable, we know that it
        // is safe to join it with an index scan to filter out non-matching
        // language tags.
        variableFoundInTriple = true;
      }
    }
  }

  // Replace all the matching triples.
  for (auto* triplePtr : matchingTriples) {
    AD_CORRECTNESS_CHECK(std::holds_alternative<PropertyPath>(triplePtr->p_));
    auto& predicate = std::get<PropertyPath>(triplePtr->p_);
    AD_CORRECTNESS_CHECK(predicate.isIri());
    std::vector<PropertyPath> predicates;
    for (const std::string& langTag : langTags) {
      predicates.push_back(
          PropertyPath::fromIri(predicate.getIri().withLanguageTag(langTag)));
    }
    predicate = predicates.size() == 1
                    ? std::move(predicates[0])
                    : PropertyPath::makeAlternative(std::move(predicates));
  }

  // Handle the case, that no suitable triple (see above) was found. In this
  // case a triple `?variable ql:langtag "language"` is added at the end of
  // the graph pattern.
  if (matchingTriples.empty()) {
    if (!variableFoundInTriple) {
      return false;
    }
    AD_CORRECTNESS_CHECK(!_graphPatterns.empty());
    AD_LOG_DEBUG << "language filter variable " + variable.name() +
                        " did not appear as object in any suitable "
                        "triple. "
                        "Using literal-to-language predicate instead.\n";

    std::vector<BasicGraphPattern> operations;
    for (const auto& langTag : langTags) {
      operations.push_back({std::vector{SparqlTriple{
          variable,
          PropertyPath::fromIri(ad_utility::triple_component::Iri::fromIriref(
              LANGUAGE_PREDICATE)),
          ad_utility::triple_component::Iri::fromLangtag(langTag)}}});
    }

    // Optimization if there already is a `BasicGraphPattern` we can use.
    // TODO<joka921> It might be beneficial to place this triple not at the
    // end but close to other occurrences of `variable`.
    if (operations.size() == 1 &&
        std::holds_alternative<BasicGraphPattern>(_graphPatterns.back())) {
      std::get<BasicGraphPattern>(_graphPatterns.back())
          ._triples.push_back(std::move(operations.at(0)._triples.at(0)));
    } else {
      auto makeOp = [](GraphPatternOperation basicPattern) {
        GraphPattern pattern;
        pattern._graphPatterns.push_back(std::move(basicPattern));
        return pattern;
      };
      GraphPatternOperation operation{std::move(operations.at(0))};
      for (auto& basicPattern : operations | ql::views::drop(1)) {
        operation = Union{makeOp(std::move(operation)),
                          makeOp(std::move(basicPattern))};
      }
      _graphPatterns.push_back(std::move(operation));
    }
  }
  return true;
}

// ____________________________________________________________________________
const std::vector<Alias>& ParsedQuery::getAliases() const {
  if (hasSelectClause()) {
    return selectClause().getAliases();
  }
  return internalAliasesWithoutSelectClause_;
}

// _____________________________________________________________________________
void ParsedQuery::updateExportLimit(std::optional<uint64_t> sendLimit) {
  if (sendLimit.has_value()) {
    _limitOffset.exportLimit_ = sendLimit;
  }
}

// ____________________________________________________________________________
bool ParsedQuery::isAggregatingQuery() const {
  return !_groupByVariables.empty() ||
         ql::ranges::any_of(getAliases(), [](const Alias& alias) {
           return alias._expression.containsAggregate();
         });
}

// ____________________________________________________________________________
bool ParsedQuery::isDeterministic() const {
  // The expressions of `GROUP BY` and `ORDER BY` are `BIND`s in the root
  // pattern or (like those of `HAVING`) internal aliases, so they are covered
  // by the following checks. With a `GROUP BY` and a trailing `VALUES`, the
  // aliases that use a `VALUES` variable are in `postQueryValuesBinds_` (see
  // `moveAliasesAfterPostQueryValues`).
  return _rootGraphPattern.isDeterministic() &&
         ql::ranges::all_of(getAliases(),
                            [](const Alias& alias) {
                              return alias._expression.isDeterministic();
                            }) &&
         ql::ranges::all_of(postQueryValuesBinds_,
                            [](const parsedQuery::Bind& bind) {
                              return bind._expression.isDeterministic();
                            });
}

// ____________________________________________________________________________
bool ParsedQuery::GraphPattern::isDeterministic() const {
  using namespace parsedQuery;
  // Not generic, so that its branches are not instantiated once per type.
  auto isMagicServiceQueryDeterministic = [](const MagicServiceQuery& query) {
    return !query.childGraphPattern_.has_value() ||
           query.childGraphPattern_->isDeterministic();
  };
  auto isOperationDeterministic =
      [&isMagicServiceQueryDeterministic](const auto& op) -> bool {
    using T = std::decay_t<decltype(op)>;
    if constexpr (ad_utility::SimilarToAny<T, Optional, Minus,
                                           GroupGraphPattern>) {
      return op._child.isDeterministic();
    } else if constexpr (std::is_same_v<T, Union>) {
      return op._child1.isDeterministic() && op._child2.isDeterministic();
    } else if constexpr (std::is_same_v<T, Subquery>) {
      return op.get().isDeterministic();
    } else if constexpr (std::is_same_v<T, Bind>) {
      return op._expression.isDeterministic();
    } else if constexpr (ad_utility::SimilarToAny<
                             T, PathQuery, SpatialQuery, TextSearchQuery,
                             NamedCachedResult, MaterializedViewQuery>) {
      return isMagicServiceQueryDeterministic(op);
    } else if constexpr (std::is_same_v<T, Service>) {
      // Mirror `Service::isDeterministicImpl()`.
      return getRuntimeParameter<&RuntimeParameters::cacheServiceResults_>();
    } else if constexpr (std::is_same_v<T, ExternalValuesQuery>) {
      return false;
    } else if constexpr (ad_utility::SimilarToAny<T, BasicGraphPattern,
                                                  Values>) {
      return true;
    } else {
      // The remaining types are unreachable: this function is only called for
      // the argument of an `EXISTS`, but a `Describe` or `Load` can only be
      // the root of a `DESCRIBE` query or a `LOAD` update, and a `TransPath`
      // is only created by the `QueryPlanner`.
      static_assert(ad_utility::SimilarToAny<T, Describe, TransPath, Load>);
      AD_FAIL();
    }
  };
  return ql::ranges::all_of(_filters,
                            [](const SparqlFilter& filter) {
                              return filter.expression_.isDeterministic();
                            }) &&
         ql::ranges::all_of(_graphPatterns,
                            [&isOperationDeterministic](const auto& op) {
                              return op.visit(isOperationDeterministic);
                            });
}

// ____________________________________________________________________________
void ParsedQuery::checkVariableIsVisible(
    const Variable& variable, const std::string& locationDescription,
    const ad_utility::HashSet<Variable>& additionalVisibleVariables,
    std::string_view otherPossibleLocationDescription) {
  if (!ad_utility::contains(getVisibleVariables(), variable) &&
      !additionalVisibleVariables.contains(variable)) {
    addWarningOrThrow(absl::StrCat("Variable ", variable.name(),
                                   " was used by " + locationDescription,
                                   ", but is not defined in the query body",
                                   otherPossibleLocationDescription, "."));
  }
}

// ____________________________________________________________________________
void ParsedQuery::checkUsedVariablesAreVisible(
    const sparqlExpression::SparqlExpressionPimpl& expression,
    const std::string& locationDescription,
    const ad_utility::HashSet<Variable>& additionalVisibleVariables,
    std::string_view otherPossibleLocationDescription) {
  // Note: We pass `excludeExists = true`, because the variables that occur only
  // inside the body of an `EXISTS` live in their own scope and thus are not
  // visible in the surrounding query.
  for (const auto* var : expression.containedVariables(true)) {
    checkVariableIsVisible(*var,
                           locationDescription + " in expression \"" +
                               expression.getDescriptor() + "\"",
                           additionalVisibleVariables,
                           otherPossibleLocationDescription);
  }
}

// ____________________________________________________________________________
void ParsedQuery::addGroupByClause(
    std::vector<GroupKey> groupKeys,
    InternalVariableGenerator internalVariableGenerator) {
  // Deduplicate the group by variables to support e.g. `GROUP BY ?x ?x ?x`.
  // Note: The `GroupBy` class expects the grouped variables to be unique.
  ad_utility::HashSet<Variable> deduplicatedGroupByVars;
  auto processVariable = [this,
                          &deduplicatedGroupByVars](const Variable& groupKey) {
    checkVariableIsVisible(groupKey, "GROUP BY");
    if (deduplicatedGroupByVars.insert(groupKey).second) {
      _groupByVariables.push_back(groupKey);
    }
  };

  ad_utility::HashSet<Variable> variablesDefinedInGroupBy;
  auto processExpression =
      [this, &variablesDefinedInGroupBy, &processVariable,
       &internalVariableGenerator](
          sparqlExpression::SparqlExpressionPimpl groupKey) {
        // Handle the case of redundant braces around a variable, e.g. `GROUP BY
        // (?x)`, which is parsed as an expression by the parser.
        if (auto var = groupKey.getVariableOrNullopt(); var.has_value()) {
          processVariable(var.value());
          return;
        }
        checkUsedVariablesAreVisible(groupKey, "GROUP BY",
                                     variablesDefinedInGroupBy,
                                     " or previously in the same GROUP BY");
        auto helperTarget =
            addInternalBind(std::move(groupKey), internalVariableGenerator);
        _groupByVariables.push_back(helperTarget);
      };

  auto processAlias = [this, &variablesDefinedInGroupBy](Alias groupKey) {
    checkUsedVariablesAreVisible(groupKey._expression, "GROUP BY",
                                 variablesDefinedInGroupBy,
                                 " or previously in the same GROUP BY");
    variablesDefinedInGroupBy.insert(groupKey._target);
    addBind(std::move(groupKey._expression), groupKey._target, true);
    _groupByVariables.push_back(groupKey._target);
  };

  for (auto& groupByKey : groupKeys) {
    std::visit(
        ad_utility::OverloadCallOperator{processVariable, processExpression,
                                         processAlias},
        std::move(groupByKey));
  }
}

// ____________________________________________________________________________
void ParsedQuery::addHavingClause(
    std::vector<SparqlFilter> havingClauses, bool isGroupBy,
    InternalVariableGenerator internalVariableGenerator) {
  if (!isGroupBy && !havingClauses.empty()) {
    throw InvalidSparqlQueryException(
        "A HAVING clause is only supported in queries with GROUP BY");
  }

  ad_utility::HashSet<Variable> variablesFromAliases;
  if (hasSelectClause()) {
    for (const auto& alias : selectClause().getAliases()) {
      variablesFromAliases.insert(alias._target);
    }
  }
  for (auto& havingClause : havingClauses) {
    checkUsedVariablesAreVisible(
        havingClause.expression_, "HAVING", variablesFromAliases,
        " and also not bound inside the SELECT clause");
    auto newVariable = addInternalAlias(std::move(havingClause.expression_),
                                        internalVariableGenerator);
    _havingClauses.push_back(
        {sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
            newVariable)});
  }
}

// ____________________________________________________________________________
void ParsedQuery::addOrderByClause(
    OrderClause orderClause, bool isGroupBy,
    std::string_view noteForImplicitGroupBy,
    InternalVariableGenerator internalVariableGenerator) {
  // The variables that are used in the ORDER BY can also come from aliases in
  // the SELECT clause
  ad_utility::HashSet<Variable> variablesFromAliases;
  if (hasSelectClause()) {
    for (const auto& alias : selectClause().getAliases()) {
      variablesFromAliases.insert(alias._target);
    }
  }

  std::string_view additionalError =
      " and also not bound inside the SELECT clause";

  // Process orderClause
  auto processVariableOrderKey = [this, isGroupBy, &noteForImplicitGroupBy,
                                  &variablesFromAliases,
                                  additionalError](VariableOrderKey orderKey) {
    if (!isGroupBy) {
      checkVariableIsVisible(orderKey.variable_, "ORDER BY",
                             variablesFromAliases, additionalError);
    } else if (!ad_utility::contains(_groupByVariables, orderKey.variable_) &&
               !variablesFromAliases.contains(orderKey.variable_) &&
               !(postQueryValuesClause_.has_value() &&
                 ad_utility::contains(
                     postQueryValuesClause_->_inlineValues._variables,
                     orderKey.variable_))) {
      // If the query (in addition to the `ORDER BY`) also contains a
      // `GROUP BY`, the variables in the `ORDER BY` must be either grouped,
      // the result of an alias in the `SELECT` clause, or bound by the
      // trailing `VALUES`.
      addWarningOrThrow(absl::StrCat(
          "Variable " + orderKey.variable_.name(),
          " was used in an ORDER BY clause, but is neither grouped nor "
          "created as an alias in the SELECT clause.",
          noteForImplicitGroupBy));
    }
    _orderBy.push_back(std::move(orderKey));
  };

  // QLever currently only supports ordering by variables. To allow
  // all `orderConditions`, the corresponding expression is bound to a new
  // internal variable. Ordering is then done by this variable.
  auto processExpressionOrderKey =
      [this, isGroupBy, &variablesFromAliases, additionalError,
       &internalVariableGenerator](ExpressionOrderKey orderKey) {
        checkUsedVariablesAreVisible(orderKey.expression_, "ORDER BY",
                                     variablesFromAliases, additionalError);
        if (isGroupBy) {
          auto newVariable = addInternalAlias(std::move(orderKey.expression_),
                                              internalVariableGenerator);
          _orderBy.emplace_back(std::move(newVariable), orderKey.isDescending_);
        } else {
          auto additionalVariable = addInternalBind(
              std::move(orderKey.expression_), internalVariableGenerator);
          _orderBy.emplace_back(additionalVariable, orderKey.isDescending_);
        }
      };

  for (auto& orderKey : orderClause.orderKeys) {
    std::visit(ad_utility::OverloadCallOperator{processVariableOrderKey,
                                                processExpressionOrderKey},
               std::move(orderKey));
  }
  _isInternalSort = orderClause.isInternalSort;
}

// _____________________________________________________________________________
void ParsedQuery::moveAliasesAfterPostQueryValues(
    InternalVariableGenerator internalVariableGenerator) {
  using namespace sparqlExpression;
  // Replace the aggregates in the subtree of `expression` by new internal
  // variables, which are computed by the `GroupBy`.
  auto replaceAggregates = [this, &internalVariableGenerator](
                               auto& self,
                               SparqlExpression& expression) -> void {
    for (size_t i = 0; i < expression.children().size(); ++i) {
      SparqlExpression& child = *expression.children()[i];
      if (child.isAggregate() ==
          SparqlExpression::AggregateStatus::NoAggregate) {
        self(self, child);
        continue;
      }
      auto variable = internalVariableGenerator();
      auto aggregate = expression.replaceChild(
          i, std::make_unique<VariableExpression>(variable));
      auto descriptor = aggregate->descriptor();
      addInternalAlias(Alias{
          SparqlExpressionPimpl{std::move(aggregate), std::move(descriptor)},
          variable});
    }
  };

  const auto& valuesVariables =
      postQueryValuesClause_->_inlineValues._variables;
  ad_utility::HashSet<Variable> lateVariables{valuesVariables.begin(),
                                              valuesVariables.end()};
  // `HAVING` is applied before the join, so its (internal) aliases are always
  // computed by the `GroupBy`, where the `VALUES` variables are unbound.
  ad_utility::HashSet<Variable> havingVariables;
  for (const auto& having : _havingClauses) {
    havingVariables.insert(having.expression_.getVariableOrNullopt().value());
  }
  for (auto& alias : takeAliases()) {
    if (havingVariables.contains(alias._target) ||
        ql::ranges::none_of(alias._expression.getUnaggregatedVariables({}),
                            [&lateVariables](const Variable& variable) {
                              return lateVariables.contains(variable);
                            })) {
      // The target is already selected (if it is not internal), so we add the
      // alias back as internal, such that the target is not selected twice.
      addInternalAlias(std::move(alias));
      continue;
    }
    lateVariables.insert(alias._target);
    // An aggregate never has unaggregated variables, so the root of the
    // expression is not an aggregate itself.
    replaceAggregates(replaceAggregates, *alias._expression.getPimpl());
    postQueryValuesBinds_.push_back(
        {std::move(alias._expression), std::move(alias._target)});
  }
}

// _____________________________________________________________________________
void ParsedQuery::addWarningOrThrow(std::string warning) {
  if (getRuntimeParameter<&RuntimeParameters::throwOnUnboundVariables_>()) {
    throw InvalidSparqlQueryException(std::move(warning));
  } else {
    addWarning(std::move(warning));
  }
}
