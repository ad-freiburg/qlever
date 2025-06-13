// Copyright 2014 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2014 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#include "ParsedQuery.h"

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "global/RuntimeParameters.h"
#include "parser/RdfEscaping.h"
#include "parser/sparqlParser/SparqlQleverVisitor.h"
#include "util/Conversions.h"
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
    sparqlExpression::SparqlExpressionPimpl expression) {
  // Internal variable name to which the result of the helper bind is
  // assigned.
  auto targetVariable = getNewInternalVariable();
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
    sparqlExpression::SparqlExpressionPimpl expression) {
  // Internal variable name to which the result of the helper bind is
  // assigned.
  auto targetVariable = getNewInternalVariable();
  // Don't register the targetVariable as visible because it is used
  // internally and should not be visible to the user.
  selectClause().addAlias(Alias{std::move(expression), targetVariable}, true);
  return targetVariable;
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
void ParsedQuery::addSolutionModifiers(SolutionModifiers modifiers) {
  // Process groupClause
  addGroupByClause(std::move(modifiers.groupByVariables_));

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
  addHavingClause(std::move(modifiers.havingClauses_), isGroupBy);

  // Process ORDER BY clause
  addOrderByClause(std::move(modifiers.orderBy_), isGroupBy,
                   noteForImplicitGroupBy);

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
      ad_utility::HashSet<Variable> groupVariables{};
      for (const auto& variable : _groupByVariables) {
        groupVariables.emplace(variable);
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
        if (!ad_utility::contains(_groupByVariables, var)) {
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
    if (_groupByVariables.empty()) {
      return;
    }

    for (const auto& variable : constructClause().containedVariables()) {
      if (!ad_utility::contains(_groupByVariables, variable)) {
        throw InvalidSparqlQueryException("Variable " + variable.name() +
                                          " is used but not aggregated." +
                                          noteForGroupByError);
      }
    }
  } else {
    // TODO<joka921> refactor this to use `std::visit`. It is much safer.
    AD_CORRECTNESS_CHECK(hasAskClause());
  }
}

// _____________________________________________________________________________
const std::vector<Variable>& ParsedQuery::getVisibleVariables() const {
  return std::visit(&parsedQuery::ClauseBase::getVisibleVariables, _clause);
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
    if (!variable.name().starts_with(QLEVER_INTERNAL_VARIABLE_PREFIX)) {
      clause.addVisibleVariable(variable);
    }
  };
  std::visit(addVariable, _clause);
}

// __________________________________________________________________________
ParsedQuery::GraphPattern::GraphPattern() : _optional(false) {}

// __________________________________________________________________________
bool ParsedQuery::GraphPattern::addLanguageFilter(const Variable& variable,
                                                  const std::string& langTag) {
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
          !predicate.value().starts_with(
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
    predicate =
        PropertyPath::fromIri(ad_utility::convertToLanguageTaggedPredicate(
            predicate.getIri(), langTag));
  }

  // Handle the case, that no suitable triple (see above) was found. In this
  // case a triple `?variable ql:langtag "language"` is added at the end of
  // the graph pattern.
  if (matchingTriples.empty()) {
    if (!variableFoundInTriple) {
      return false;
    }
    LOG(DEBUG) << "language filter variable " + variable.name() +
                      " did not appear as object in any suitable "
                      "triple. "
                      "Using literal-to-language predicate instead.\n";

    // If necessary create an empty `BasicGraphPattern` at the end to which we
    // can append a triple.
    // TODO<joka921> It might be beneficial to place this triple not at the
    // end but close to other occurrences of `variable`.
    if (_graphPatterns.empty() ||
        !std::holds_alternative<parsedQuery::BasicGraphPattern>(
            _graphPatterns.back())) {
      _graphPatterns.emplace_back(parsedQuery::BasicGraphPattern{});
    }
    auto& t = std::get<parsedQuery::BasicGraphPattern>(_graphPatterns.back())
                  ._triples;

    auto langEntity = ad_utility::convertLangtagToEntityUri(langTag);
    SparqlTriple triple{
        variable,
        PropertyPath::fromIri(
            ad_utility::triple_component::Iri::fromIriref(LANGUAGE_PREDICATE)),
        langEntity};
    t.push_back(std::move(triple));
  }
  return true;
}

// ____________________________________________________________________________
const std::vector<Alias>& ParsedQuery::getAliases() const {
  if (hasSelectClause()) {
    return selectClause().getAliases();
  } else {
    static const std::vector<Alias> dummyForConstructClause;
    return dummyForConstructClause;
  }
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
  for (const auto* var : expression.containedVariables()) {
    checkVariableIsVisible(*var,
                           locationDescription + " in expression \"" +
                               expression.getDescriptor() + "\"",
                           additionalVisibleVariables,
                           otherPossibleLocationDescription);
  }
}

// ____________________________________________________________________________
void ParsedQuery::addGroupByClause(std::vector<GroupKey> groupKeys) {
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
      [this, &variablesDefinedInGroupBy,
       &processVariable](sparqlExpression::SparqlExpressionPimpl groupKey) {
        // Handle the case of redundant braces around a variable, e.g. `GROUP BY
        // (?x)`, which is parsed as an expression by the parser.
        if (auto var = groupKey.getVariableOrNullopt(); var.has_value()) {
          processVariable(var.value());
          return;
        }
        checkUsedVariablesAreVisible(groupKey, "GROUP BY",
                                     variablesDefinedInGroupBy,
                                     " or previously in the same GROUP BY");
        auto helperTarget = addInternalBind(std::move(groupKey));
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
void ParsedQuery::addHavingClause(std::vector<SparqlFilter> havingClauses,
                                  bool isGroupBy) {
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
    auto newVariable = addInternalAlias(std::move(havingClause.expression_));
    _havingClauses.emplace_back(
        sparqlExpression::SparqlExpressionPimpl::makeVariableExpression(
            newVariable));
  }
}

// ____________________________________________________________________________
void ParsedQuery::addOrderByClause(OrderClause orderClause, bool isGroupBy,
                                   std::string_view noteForImplicitGroupBy) {
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
               (!variablesFromAliases.contains(orderKey.variable_))) {
      // If the query (in addition to the ORDER BY) also contains a GROUP BY,
      // the variables in the ORDER BY must be either grouped or the result
      // of an alias in the SELECT clause.
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
      [this, isGroupBy, &variablesFromAliases,
       additionalError](ExpressionOrderKey orderKey) {
        checkUsedVariablesAreVisible(orderKey.expression_, "ORDER BY",
                                     variablesFromAliases, additionalError);
        if (isGroupBy) {
          auto newVariable = addInternalAlias(std::move(orderKey.expression_));
          _orderBy.emplace_back(std::move(newVariable), orderKey.isDescending_);
        } else {
          auto additionalVariable =
              addInternalBind(std::move(orderKey.expression_));
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

// ________________________________________________________________
Variable ParsedQuery::getNewInternalVariable() {
  auto variable = Variable{
      absl::StrCat(QLEVER_INTERNAL_VARIABLE_PREFIX, numInternalVariables_)};
  numInternalVariables_++;
  return variable;
}

// _____________________________________________________________________________
Variable ParsedQuery::blankNodeToInternalVariable(std::string_view blankNode) {
  AD_CONTRACT_CHECK(blankNode.starts_with("_:"));
  return Variable{absl::StrCat(QLEVER_INTERNAL_BLANKNODE_VARIABLE_PREFIX,
                               blankNode.substr(2))};
}

// _____________________________________________________________________________
void ParsedQuery::addWarningOrThrow(std::string warning) {
  if (RuntimeParameters().get<"throw-on-unbound-variables">()) {
    throw InvalidSparqlQueryException(std::move(warning));
  } else {
    addWarning(std::move(warning));
  }
}
