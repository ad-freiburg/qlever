// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "ParsedQuery.h"

#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/RdfEscaping.h"
#include "util/Conversions.h"

using std::string;
using std::vector;

// _____________________________________________________________________________
string ParsedQuery::asString() const {
  std::ostringstream os;

  bool usesSelect = hasSelectClause();
  bool usesAsterisk = usesSelect && selectClause().isAsterisk();

  if (usesSelect) {
    const auto& selectClause = this->selectClause();
    // SELECT
    os << "\nSELECT: {\n\t";
    // TODO<joka921> is this needed?
    /*
    os <<
    absl::StrJoin(selectClause.varsAndAliasesOrAsterisk_.getSelectedVariables(),
                        ", ");
                        */
    os << "\n}";

    // ALIASES
    os << "\nALIASES: {\n\t";
    if (!usesAsterisk) {
      for (const auto& alias : selectClause.getAliases()) {
        os << alias._expression.getDescriptor() << "\n\t";
      }
      os << "{";
    }
  } else if (hasConstructClause()) {
    const auto& constructClause = this->constructClause().triples_;
    os << "\n CONSTRUCT {\n\t";
    for (const auto& triple : constructClause) {
      os << triple[0].toSparql();
      os << ' ';
      os << triple[1].toSparql();
      os << ' ';
      os << triple[2].toSparql();
      os << " .\n";
    }
    os << "}";
  }

  // WHERE
  os << "\nWHERE: \n";
  _rootGraphPattern.toString(os, 1);

  os << "\nLIMIT: " << (_limitOffset.limitOrDefault());
  os << "\nTEXTLIMIT: " << (_limitOffset._textLimit);
  os << "\nOFFSET: " << (_limitOffset._offset);
  if (usesSelect) {
    const auto& selectClause = this->selectClause();
    os << "\nDISTINCT modifier is " << (selectClause.distinct_ ? "" : "not ")
       << "present.";
    os << "\nREDUCED modifier is " << (selectClause.reduced_ ? "" : "not ")
       << "present.";
  }
  os << "\nORDER BY: ";
  if (_orderBy.empty()) {
    os << "not specified";
  } else {
    for (auto& key : _orderBy) {
      os << key.variable_.name() << (key.isDescending_ ? " (DESC)" : " (ASC)")
         << "\t";
    }
  }
  os << "\n";
  return std::move(os).str();
}

// _____________________________________________________________________________
string SparqlPrefix::asString() const {
  std::ostringstream os;
  os << "{" << _prefix << ": " << _uri << "}";
  return std::move(os).str();
}

// _____________________________________________________________________________
string SparqlTriple::asString() const {
  std::ostringstream os;
  os << "{s: " << _s << ", p: " << _p << ", o: " << _o << "}";
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
      std::ranges::any_of(getAliases(),
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
        if (auto it = std::ranges::find(aliases, var, &Alias::_target);
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
  } else {
    AD_CORRECTNESS_CHECK(hasConstructClause());
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
  }
}

void ParsedQuery::merge(const ParsedQuery& p) {
  auto& children = _rootGraphPattern._graphPatterns;
  auto& otherChildren = p._rootGraphPattern._graphPatterns;
  children.insert(children.end(), otherChildren.begin(), otherChildren.end());

  // update the ids
  _numGraphPatterns = 0;
  _rootGraphPattern.recomputeIds(&_numGraphPatterns);
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
    if (!variable.name().starts_with(INTERNAL_VARIABLE_PREFIX)) {
      clause.addVisibleVariable(variable);
    }
  };
  std::visit(addVariable, _clause);
}

// _____________________________________________________________________________
void ParsedQuery::GraphPattern::toString(std::ostringstream& os,
                                         int indentation) const {
  for (int j = 1; j < indentation; ++j) os << "  ";
  os << "{";
  for (size_t i = 0; i + 1 < _filters.size(); ++i) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _filters[i].asString() << ',';
  }
  if (!_filters.empty()) {
    os << "\n";
    for (int j = 0; j < indentation; ++j) os << "  ";
    os << _filters.back().asString();
  }
  for (const GraphPatternOperation& child : _graphPatterns) {
    os << "\n";
    child.toString(os, indentation + 1);
  }
  os << "\n";
  for (int j = 1; j < indentation; ++j) os << "  ";
  os << "}";
}

// _____________________________________________________________________________
void ParsedQuery::GraphPattern::recomputeIds(size_t* id_count) {
  bool allocatedIdCounter = false;
  if (id_count == nullptr) {
    id_count = new size_t(0);
    allocatedIdCounter = true;
  }
  _id = *id_count;
  (*id_count)++;
  for (auto& op : _graphPatterns) {
    op.visit([&id_count](auto&& arg) {
      using T = std::decay_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, parsedQuery::Union>) {
        arg._child1.recomputeIds(id_count);
        arg._child2.recomputeIds(id_count);
      } else if constexpr (std::is_same_v<T, parsedQuery::Optional> ||
                           std::is_same_v<T, parsedQuery::GroupGraphPattern> ||
                           std::is_same_v<T, parsedQuery::Minus>) {
        arg._child.recomputeIds(id_count);
      } else if constexpr (std::is_same_v<T, parsedQuery::TransPath>) {
        // arg._childGraphPattern.recomputeIds(id_count);
      } else if constexpr (std::is_same_v<T, parsedQuery::Values>) {
        arg._id = (*id_count)++;
      } else {
        static_assert(std::is_same_v<T, parsedQuery::Subquery> ||
                      std::is_same_v<T, parsedQuery::Service> ||
                      std::is_same_v<T, parsedQuery::BasicGraphPattern> ||
                      std::is_same_v<T, parsedQuery::Bind>);
        // subquery children have their own id space
        // TODO:joka921 look at the optimizer if it is ok, that
        // BasicGraphPatterns and Values have no ids at all. at the same time
        // assert that the above else-if is exhaustive.
      }
    });
  }

  if (allocatedIdCounter) {
    delete id_count;
  }
}

// __________________________________________________________________________
ParsedQuery::GraphPattern::GraphPattern() : _optional(false) {}

// __________________________________________________________________________
void ParsedQuery::GraphPattern::addLanguageFilter(
    const Variable& variable, const std::string& languageInQuotes) {
  auto langTag = languageInQuotes.substr(1, languageInQuotes.size() - 2);
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
  namespace stdv = std::views;
  for (BasicPattern* basicPattern :
       _graphPatterns | stdv::transform(ad::getIf<BasicPattern>) |
           stdv::filter(ad::toBool)) {
    for (auto& triple : basicPattern->_triples) {
      if (triple._o == variable &&
          (triple._p._operation == PropertyPath::Operation::IRI &&
           !isVariable(triple._p)) &&
          !triple._p._iri.starts_with(INTERNAL_ENTITIES_URI_PREFIX)) {
        matchingTriples.push_back(&triple);
      }
    }
  }

  // Replace all the matching triples.
  for (auto* triplePtr : matchingTriples) {
    triplePtr->_p._iri = '@' + langTag + '@' + triplePtr->_p._iri;
  }

  // Handle the case, that no suitable triple (see above) was found. In this
  // case a triple `?variable ql:langtag "language"` is added at the end of
  // the graph pattern.
  if (matchingTriples.empty()) {
    LOG(DEBUG) << "language filter variable " + variable.name() +
                      " did not appear as object in any suitable "
                      "triple. "
                      "Using literal-to-language predicate instead.\n";

    // If necessary create an empty `BasicGraphPattern` at the end to which we
    // can append a triple.
    // TODO<joka921> It might be beneficial to place this triple not at the
    // end but close to other occurences of `variable`.
    if (_graphPatterns.empty() ||
        !std::holds_alternative<parsedQuery::BasicGraphPattern>(
            _graphPatterns.back())) {
      _graphPatterns.emplace_back(parsedQuery::BasicGraphPattern{});
    }
    auto& t = std::get<parsedQuery::BasicGraphPattern>(_graphPatterns.back())
                  ._triples;

    auto langEntity = ad_utility::convertLangtagToEntityUri(langTag);
    SparqlTriple triple(variable, PropertyPath::fromIri(LANGUAGE_PREDICATE),
                        langEntity);
    t.push_back(std::move(triple));
  }
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
cppcoro::generator<const Variable>
ParsedQuery::getConstructedOrSelectedVariables() const {
  if (hasSelectClause()) {
    for (const auto& variable : selectClause().getSelectedVariables()) {
      co_yield variable;
    }
  } else {
    for (const auto& variable : constructClause().containedVariables()) {
      co_yield variable;
    }
  }
  // Nothing to yield in the CONSTRUCT case.
}

// ____________________________________________________________________________
void ParsedQuery::checkVariableIsVisible(
    const Variable& variable, const std::string& locationDescription,
    const ad_utility::HashSet<Variable>& additionalVisibleVariables,
    std::string_view otherPossibleLocationDescription) const {
  if (!ad_utility::contains(getVisibleVariables(), variable) &&
      !additionalVisibleVariables.contains(variable)) {
    throw InvalidSparqlQueryException(absl::StrCat(
        "Variable ", variable.name(), " was used by " + locationDescription,
        ", but is not defined in the query body",
        otherPossibleLocationDescription, "."));
  }
}

// ____________________________________________________________________________
void ParsedQuery::checkUsedVariablesAreVisible(
    const sparqlExpression::SparqlExpressionPimpl& expression,
    const std::string& locationDescription,
    const ad_utility::HashSet<Variable>& additionalVisibleVariables,
    std::string_view otherPossibleLocationDescription) const {
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
  // Process groupClause
  auto processVariable = [this](const Variable& groupKey) {
    checkVariableIsVisible(groupKey, "GROUP BY");

    _groupByVariables.push_back(groupKey);
  };

  ad_utility::HashSet<Variable> variablesDefinedInGroupBy;
  auto processExpression =
      [this, &variablesDefinedInGroupBy](
          sparqlExpression::SparqlExpressionPimpl groupKey) {
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
    checkVariableIsVisible(orderKey.variable_, "ORDER BY", variablesFromAliases,
                           additionalError);

    // Check whether grouping is done. The variable being ordered by
    // must then be either grouped or the result of an alias in the select
    // clause.
    if (isGroupBy &&
        !ad_utility::contains(_groupByVariables, orderKey.variable_) &&
        (!variablesFromAliases.contains(orderKey.variable_))) {
      throw InvalidSparqlQueryException(
          "Variable " + orderKey.variable_.name() +
          " was used in an ORDER BY clause, but is neither grouped nor "
          "created as an alias in the SELECT clause." +
          noteForImplicitGroupBy);
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
  auto variable =
      Variable{absl::StrCat(INTERNAL_VARIABLE_PREFIX, numInternalVariables_)};
  numInternalVariables_++;
  return variable;
}

Variable ParsedQuery::blankNodeToInternalVariable(std::string_view blankNode) {
  AD_CONTRACT_CHECK(blankNode.starts_with("_:"));
  return Variable{
      absl::StrCat(INTERNAL_BLANKNODE_VARIABLE_PREFIX, blankNode.substr(2))};
}
