// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)

#include "ParsedQuery.h"

#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <parser/RdfEscaping.h>
#include <util/Conversions.h>

#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

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

  os << "\nLIMIT: " << (_limitOffset._limit);
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
  auto targetVariable = Variable{INTERNAL_VARIABLE_PREFIX +
                                 std::to_string(numInternalVariables_)};
  numInternalVariables_++;
  parsedQuery::Bind bind{std::move(expression), targetVariable};
  _rootGraphPattern._graphPatterns.emplace_back(std::move(bind));
  // Don't register the targetVariable as visible because it is used
  // internally and should not be selected by SELECT *.
  // TODO<qup42, joka921> Implement "internal" variables, that can't be
  //  selected at all and can never interfere with variables from the
  //  query.
  return targetVariable;
}

// ________________________________________________________________________
void ParsedQuery::addSolutionModifiers(SolutionModifiers modifiers) {
  auto checkVariableIsVisible = [this](const Variable& var,
                                       const std::string& locationDescription) {
    if (!ad_utility::contains(getVisibleVariables(), var)) {
      throw ParseException("Variable " + var.name() + " was used in " +
                           locationDescription +
                           ", but is not visible in the Query Body.");
    }
  };
  auto checkUsedVariablesAreVisible =
      [&checkVariableIsVisible](
          const sparqlExpression::SparqlExpressionPimpl& expression,
          const std::string& locationDescription) {
        for (const auto* var : expression.containedVariables()) {
          checkVariableIsVisible(*var, locationDescription + " in Expression " +
                                           expression.getDescriptor());
        }
      };

  // Process groupClause
  auto processVariable = [this,
                          &checkVariableIsVisible](const Variable& groupKey) {
    checkVariableIsVisible(groupKey, "GROUP BY");

    _groupByVariables.emplace_back(groupKey.name());
  };
  auto processExpression =
      [this, &checkUsedVariablesAreVisible](
          sparqlExpression::SparqlExpressionPimpl groupKey) {
        checkUsedVariablesAreVisible(groupKey, "Group Key");
        auto helperTarget = addInternalBind(std::move(groupKey));
        _groupByVariables.emplace_back(helperTarget.name());
      };
  auto processAlias = [this](Alias groupKey) {
    parsedQuery::Bind helperBind{std::move(groupKey._expression),
                                 groupKey._target};
    _rootGraphPattern._graphPatterns.emplace_back(std::move(helperBind));
    registerVariableVisibleInQueryBody(groupKey._target);
    _groupByVariables.emplace_back(groupKey._target);
  };

  for (auto& orderKey : modifiers.groupByVariables_) {
    std::visit(
        ad_utility::OverloadCallOperator{processVariable, processExpression,
                                         processAlias},
        std::move(orderKey));
  }

  // Process havingClause
  // TODO<joka921, qup42> as soon as FILTER and HAVING support proper
  //  expressions, also add similar sanity checks for the HAVING clause here.
  _havingClauses = std::move(modifiers.havingClauses_);

  // Process orderClause
  auto processVariableOrderKey = [this, &checkVariableIsVisible](
                                     VariableOrderKey orderKey) {
    // Check whether grouping is done. The variable being ordered by
    // must then be either grouped or the result of an alias in the select.
    const vector<Variable>& groupByVariables = _groupByVariables;
    if (groupByVariables.empty()) {
      checkVariableIsVisible(orderKey.variable_, "ORDERY BY");
    } else if (!ad_utility::contains(groupByVariables, orderKey.variable_) &&
               // `ConstructClause` has no Aliases. So the variable can never be
               // the result of an Alias.
               (hasConstructClause() ||
                !ad_utility::contains_if(selectClause().getAliases(),
                                         [&orderKey](const Alias& alias) {
                                           return alias._target ==
                                                  orderKey.variable_;
                                         }))) {
      throw ParseException(
          "Variable " + orderKey.variable_.name() +
          " was used in an ORDER BY "
          "clause, but is neither grouped, nor created as an alias in the "
          "SELECT clause.");
    }

    _orderBy.push_back(std::move(orderKey));
  };

  // QLever currently only supports ordering by variables. To allow
  // all `orderConditions`, the corresponding expression is bound to a new
  // internal variable. Ordering is then done by this variable.
  auto processExpressionOrderKey = [this, &checkUsedVariablesAreVisible](
                                       ExpressionOrderKey orderKey) {
    checkUsedVariablesAreVisible(orderKey.expression_, "Order Key");
    if (!_groupByVariables.empty()) {
      // TODO<qup42> Implement this by adding a hidden alias in the
      //  SELECT clause.
      throw ParseException(
          "Ordering by an expression while grouping is not supported by "
          "QLever. (The expression is \"" +
          orderKey.expression_.getDescriptor() +
          "\"). Please assign this expression to a "
          "new variable in the SELECT clause and then order by this "
          "variable.");
    }
    auto additionalVariable = addInternalBind(std::move(orderKey.expression_));
    _orderBy.emplace_back(additionalVariable, orderKey.isDescending_);
  };

  for (auto& orderKey : modifiers.orderBy_) {
    std::visit(ad_utility::OverloadCallOperator{processVariableOrderKey,
                                                processExpressionOrderKey},
               std::move(orderKey));
  }

  // Process limitOffsetClause
  _limitOffset = modifiers.limitOffset_;

  // Check that the query is valid

  auto checkAliasOutNamesHaveNoOverlapWith =
      [this](const auto& container, const std::string& message) {
        for (const auto& alias : selectClause().getAliases()) {
          if (ad_utility::contains(container, alias._target)) {
            throw ParseException(absl::StrCat(alias._target.name(), message));
          }
        }
      };

  if (hasSelectClause()) {
    if (!_groupByVariables.empty()) {
      ad_utility::HashSet<string> groupVariables{};
      for (const auto& variable : _groupByVariables) {
        groupVariables.emplace(variable.toSparql());
      }

      if (selectClause().isAsterisk()) {
        throw ParseException(
            "GROUP BY is not allowed when all variables are selected via "
            "SELECT *");
      }

      // Check if all selected variables are either aggregated or
      // part of the group by statement.
      for (const Variable& var : selectClause().getSelectedVariables()) {
        if (ad_utility::contains_if(
                selectClause().getAliases(),
                [&var, &groupVariables](const Alias& alias) {
                  return alias._target == var &&
                         alias._expression.isAggregate(groupVariables);
                })) {
          continue;
        }
        if (!ad_utility::contains(_groupByVariables, var)) {
          throw ParseException(
              absl::StrCat("Variable ", var.name(),
                           " is selected but not aggregated despite the "
                           "query not being grouped by ",
                           var.name(), "."));
        }
      }
      checkAliasOutNamesHaveNoOverlapWith(_groupByVariables,
                                          " is the target of an alias although "
                                          "the query is grouped by it. This "
                                          "is not allowed.");
    } else {
      checkAliasOutNamesHaveNoOverlapWith(
          selectClause().getVisibleVariables(),
          " is the target of an alias although it is also visible in the query "
          "body. This is not allowed.");
    }

    ad_utility::HashMap<std::string, size_t> variable_counts;

    for (const std::string& s :
         selectClause().getSelectedVariablesAsStrings()) {
      variable_counts[s]++;
    }

    for (const Alias& a : selectClause().getAliases()) {
      // The variable was already added to the selected variables while
      // parsing the alias, thus it should appear exactly once
      if (variable_counts[a._target.name()] > 1) {
        throw ParseException("The variable name " + a._target.name() +
                             " used in an alias was already selected on.");
      }

      checkUsedVariablesAreVisible(a._expression, "Alias");
    }
  } else if (hasConstructClause()) {
    if (_groupByVariables.empty()) {
      return;
    }

    for (const auto& variable : constructClause().containedVariables()) {
      if (!ad_utility::contains(_groupByVariables, variable)) {
        throw ParseException("Variable " + variable.name() +
                             " is used but not "
                             "aggregated despite the query not being "
                             "grouped by " +
                             variable.name() + ".");
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
    clause.addVisibleVariable(variable);
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
  if (_filters.size() > 0) {
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

// TODO<joka921> Change the first argument to `Variable`, but first merge
// the filter-PR.
void ParsedQuery::GraphPattern::addLanguageFilter(
    const Variable& variable, const std::string& languageInQuotes) {
  auto langTag = languageInQuotes.substr(1, languageInQuotes.size() - 2);
  // Find all triples where the object is the `variable` and the predicate is a
  // simple `IRIREF` (neither a variable nor a complex property path). Search in
  // all the basic graph patterns, as filters have the complete graph patterns
  // as their scope.
  // TODO<joka921> In theory we could also recurse into GroupGraphPatterns,
  // Subqueries etc.
  // TODO<joka921> Also support property paths (^rdfs:label,
  // skos:altLabel|rdfs:label, ...)
  std::vector<SparqlTriple*> matchingTriples;
  for (auto& graphPattern : _graphPatterns) {
    auto* basicPattern =
        std::get_if<parsedQuery::BasicGraphPattern>(&graphPattern);
    if (!basicPattern) {
      continue;
    }
    for (auto& triple : basicPattern->_triples) {
      if (triple._o == variable &&
          (triple._p._operation == PropertyPath::Operation::IRI &&
           !isVariable(triple._p))) {
        matchingTriples.push_back(&triple);
      }
    }
  }

  // Replace all the matching triples.
  for (auto* triplePtr : matchingTriples) {
    triplePtr->_p._iri = '@' + langTag + '@' + triplePtr->_p._iri;
  }

  // Handle the case, that no suitable triple (see above) was found. In this
  // case a triple `?variable ql:langtag "language"` is added at the end of the
  // graph pattern.
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
