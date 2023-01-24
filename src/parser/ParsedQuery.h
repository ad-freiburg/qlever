// Copyright 2014, University of Freiburg,
// Chair of Algorithms and Data Structures.
// Author: Björn Buchhold (buchhold@informatik.uni-freiburg.de)
#pragma once

#include <initializer_list>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "engine/ResultType.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/Alias.h"
#include "parser/ConstructClause.h"
#include "parser/GraphPattern.h"
#include "parser/GraphPatternOperation.h"
#include "parser/ParseException.h"
#include "parser/PropertyPath.h"
#include "parser/SelectClause.h"
#include "parser/TripleComponent.h"
#include "parser/data/GroupKey.h"
#include "parser/data/LimitOffsetClause.h"
#include "parser/data/OrderKey.h"
#include "parser/data/SolutionModifiers.h"
#include "parser/data/SparqlFilter.h"
#include "parser/data/Types.h"
#include "parser/data/VarOrTerm.h"
#include "util/Algorithm.h"
#include "util/Exception.h"
#include "util/Generator.h"
#include "util/HashMap.h"
#include "util/OverloadCallOperator.h"
#include "util/StringUtils.h"

using std::string;
using std::vector;

// Data container for prefixes
class SparqlPrefix {
 public:
  SparqlPrefix(string prefix, string uri)
      : _prefix(std::move(prefix)), _uri(std::move(uri)) {}

  string _prefix;
  string _uri;

  [[nodiscard]] string asString() const;

  bool operator==(const SparqlPrefix&) const = default;
};

inline bool isVariable(const string& elem) { return elem.starts_with("?"); }
inline bool isVariable(const TripleComponent& elem) {
  return elem.isVariable();
}

inline bool isVariable(const PropertyPath& elem) {
  return elem._operation == PropertyPath::Operation::IRI &&
         isVariable(elem._iri);
}

std::ostream& operator<<(std::ostream& out, const PropertyPath& p);

// Data container for parsed triples from the where clause
class SparqlTriple {
 public:
  SparqlTriple(TripleComponent s, PropertyPath p, TripleComponent o)
      : _s(std::move(s)), _p(std::move(p)), _o(std::move(o)) {}

  SparqlTriple(TripleComponent s, const std::string& p_iri, TripleComponent o)
      : _s(std::move(s)),
        _p(PropertyPath::Operation::IRI, 0, p_iri, {}),
        _o(std::move(o)) {}

  bool operator==(const SparqlTriple& other) const {
    return _s == other._s && _p == other._p && _o == other._o;
  }
  TripleComponent _s;
  PropertyPath _p;
  TripleComponent _o;

  [[nodiscard]] string asString() const;
};

// Forward declaration
namespace parsedQuery {
struct GraphPatternOperation;
}

// A parsed SPARQL query. To be extended.
class ParsedQuery {
 public:
  using GraphPattern = parsedQuery::GraphPattern;

  using SelectClause = parsedQuery::SelectClause;

  using ConstructClause = parsedQuery::ConstructClause;

  ParsedQuery() = default;

  GraphPattern _rootGraphPattern;
  vector<SparqlFilter> _havingClauses;
  size_t _numGraphPatterns = 1;
  // The number of additional internal variables that were added by the
  // implementation of ORDER BY as BIND+ORDER BY.
  int64_t numInternalVariables_ = 0;
  vector<VariableOrderKey> _orderBy;
  IsInternalSort _isInternalSort = IsInternalSort::False;
  vector<Variable> _groupByVariables;
  LimitOffsetClause _limitOffset{};
  string _originalString;

  // explicit default initialisation because the constructor
  // of SelectClause is private
  std::variant<SelectClause, ConstructClause> _clause{SelectClause{}};

  [[nodiscard]] bool hasSelectClause() const {
    return std::holds_alternative<SelectClause>(_clause);
  }

  [[nodiscard]] bool hasConstructClause() const {
    return std::holds_alternative<ConstructClause>(_clause);
  }

  [[nodiscard]] decltype(auto) selectClause() const {
    return std::get<SelectClause>(_clause);
  }

  [[nodiscard]] decltype(auto) constructClause() const {
    return std::get<ConstructClause>(_clause);
  }

  [[nodiscard]] decltype(auto) selectClause() {
    return std::get<SelectClause>(_clause);
  }

  [[nodiscard]] decltype(auto) constructClause() {
    return std::get<ConstructClause>(_clause);
  }

  // Add a variable, that was found in the query body.
  void registerVariableVisibleInQueryBody(const Variable& variable);

  // Add variables, that were found in the query body.
  void registerVariablesVisibleInQueryBody(const vector<Variable>& variables);

  // Returns all variables that are visible in the Query Body.
  const std::vector<Variable>& getVisibleVariables() const;

  auto& children() { return _rootGraphPattern._graphPatterns; }
  [[nodiscard]] const auto& children() const {
    return _rootGraphPattern._graphPatterns;
  }

  // TODO<joka921> This is currently necessary because of the missing scoping of
  // subqueries
  [[nodiscard]] int64_t getNumInternalVariables() const {
    return numInternalVariables_;
  }

  void setNumInternalVariables(int64_t numInternalVariables) {
    numInternalVariables_ = numInternalVariables;
  }

  /// Generates an internal bind that binds the given expression using a bind.
  /// The bind is added to the query as child. The variable that the expression
  /// is bound to is returned.
  Variable addInternalBind(sparqlExpression::SparqlExpressionPimpl expression);
  void addSolutionModifiers(SolutionModifiers modifiers);

  /**
   * @brief Adds all elements from p's rootGraphPattern to this parsed query's
   * root graph pattern. This changes the graph patterns ids.
   */
  void merge(const ParsedQuery& p);

  [[nodiscard]] string asString() const;

  // If this is a SELECT query, return all the selected aliases. Return an empty
  // vector for construct clauses.
  [[nodiscard]] const std::vector<Alias>& getAliases() const;
  // If this is a SELECT query, yield all the selected variables. If this is a
  // CONSTRUCT query, yield all the variables that are used in the CONSTRUCT
  // clause. Note that the result may contain duplicates in the CONSTRUCT case.
  [[nodiscard]] cppcoro::generator<const Variable>
  getConstructedOrSelectedVariables() const;
};
