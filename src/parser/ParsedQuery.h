// Copyright 2014 - 2024, University of Freiburg
// Chair of Algorithms and Data Structures
// Authors: Björn Buchhold <buchhold@cs.uni-freiburg.de> [2014 - 2017]
//          Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_PARSER_PARSEDQUERY_H
#define QLEVER_SRC_PARSER_PARSEDQUERY_H

#include <absl/functional/function_ref.h>

#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "backports/three_way_comparison.h"
#include "engine/sparqlExpressions/SparqlExpressionPimpl.h"
#include "parser/Alias.h"
#include "parser/ConstructClause.h"
#include "parser/DatasetClauses.h"
#include "parser/GraphPattern.h"
#include "parser/GraphPatternOperation.h"
#include "parser/SelectClause.h"
#include "parser/UpdateClause.h"
#include "parser/data/GroupKey.h"
#include "parser/data/LimitOffsetClause.h"
#include "parser/data/OrderKey.h"
#include "parser/data/SolutionModifiers.h"
#include "parser/data/SparqlFilter.h"

// Data container for prefixes
class SparqlPrefix {
 public:
  SparqlPrefix(std::string prefix, std::string uri)
      : _prefix(std::move(prefix)), _uri(std::move(uri)) {}

  std::string _prefix;
  std::string _uri;

  [[nodiscard]] std::string asString() const;

  QL_DEFINE_DEFAULTED_EQUALITY_OPERATOR_LOCAL(SparqlPrefix, _prefix, _uri)
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

  using UpdateClause = parsedQuery::UpdateClause;

  using DatasetClauses = parsedQuery::DatasetClauses;

  using InternalVariableGenerator = absl::FunctionRef<Variable()>;

  // ASK queries have no further context in the header, so we use an empty
  // struct
  struct AskClause : public parsedQuery::ClauseBase {};

  ParsedQuery() = default;

  GraphPattern _rootGraphPattern;
  std::vector<SparqlFilter> _havingClauses;
  std::vector<VariableOrderKey> _orderBy;
  IsInternalSort _isInternalSort = IsInternalSort::False;
  std::vector<Variable> _groupByVariables;
  LimitOffsetClause _limitOffset{};
  std::string _originalString;
  std::optional<parsedQuery::Values> postQueryValuesClause_ = std::nullopt;

  // Contains warnings about queries that are valid according to the SPARQL
  // standard, but are probably semantically wrong.
  std::vector<std::string> warnings_;

  using HeaderClause =
      std::variant<SelectClause, ConstructClause, UpdateClause, AskClause>;
  // Use explicit default initialization for `SelectClause` because its
  // constructor is private.
  HeaderClause _clause{SelectClause{}};

  // The IRIs from the FROM and FROM NAMED clauses.
  DatasetClauses datasetClauses_;

  [[nodiscard]] bool hasSelectClause() const {
    return std::holds_alternative<SelectClause>(_clause);
  }

  [[nodiscard]] bool hasConstructClause() const {
    return std::holds_alternative<ConstructClause>(_clause);
  }

  [[nodiscard]] bool hasUpdateClause() const {
    return std::holds_alternative<UpdateClause>(_clause);
  }

  bool hasAskClause() const {
    return std::holds_alternative<AskClause>(_clause);
  }

  [[nodiscard]] decltype(auto) selectClause() const {
    return std::get<SelectClause>(_clause);
  }

  [[nodiscard]] decltype(auto) constructClause() const {
    return std::get<ConstructClause>(_clause);
  }

  [[nodiscard]] decltype(auto) updateClause() const {
    return std::get<UpdateClause>(_clause);
  }

  [[nodiscard]] decltype(auto) selectClause() {
    return std::get<SelectClause>(_clause);
  }

  [[nodiscard]] decltype(auto) constructClause() {
    return std::get<ConstructClause>(_clause);
  }

  [[nodiscard]] decltype(auto) updateClause() {
    return std::get<UpdateClause>(_clause);
  }

  // Add a variable, that was found in the query body.
  void registerVariableVisibleInQueryBody(const Variable& variable);

  // Add variables, that were found in the query body.
  void registerVariablesVisibleInQueryBody(
      const std::vector<Variable>& variables);

  // Return all the warnings that have been added via `addWarning()` or
  // `addWarningOrThrow`.
  const std::vector<std::string>& warnings() const { return warnings_; }

  // Add a warning to the query. The warning becomes part of the return value of
  // the `warnings()` function above.
  void addWarning(std::string warning) {
    warnings_.push_back(std::move(warning));
  }

  // If unbound variables that are used in a query are supposed to throw because
  // the corresponding `RuntimeParameter` is set, then throw. Else add a
  // warning.
  void addWarningOrThrow(std::string warning);

  // Returns all variables that are visible in the Query Body.
  const std::vector<Variable>& getVisibleVariables() const;

  auto& children() { return _rootGraphPattern._graphPatterns; }
  [[nodiscard]] const auto& children() const {
    return _rootGraphPattern._graphPatterns;
  }

 private:
  // Add a BIND clause to the body of the query. The second argument determines
  // whether the `targetVariable` will be part of the visible variables that are
  // selected by `SELECT *`. For an example usage see `addInternalBind` and
  // `addSolutionModifiers`.
  void addBind(sparqlExpression::SparqlExpressionPimpl expression,
               Variable targetVariable, bool targetIsVisible);

  // Generates an internal BIND that binds the given expression. The BIND is
  // added to the query as a child. The variable that the expression is bound to
  // is returned. Return the `Variable` generated by calling
  // `internalVariableGenerator`.
  Variable addInternalBind(sparqlExpression::SparqlExpressionPimpl expression,
                           InternalVariableGenerator internalVariableGenerator);

  // Add an internal AS clause to the SELECT clause that computes the given
  // expression. This is needed by the `addSolutionModifiers` function to
  // implement aggregating expressions in the ORDER BY and HAVING clauses of
  // queries with a GROUP BY. Return the `Variable` generated by calling
  // `internalVariableGenerator`.
  Variable addInternalAlias(
      sparqlExpression::SparqlExpressionPimpl expression,
      InternalVariableGenerator internalVariableGenerator);

  // If the `variable` is neither visible in the query body nor contained in the
  // `additionalVisibleVariables`, add a warning or throw an exception (see
  // `addWarningOrThrow`) that uses the `locationDescription` inside the
  // message.
  void checkVariableIsVisible(
      const Variable& variable, const std::string& locationDescription,
      const ad_utility::HashSet<Variable>& additionalVisibleVariables = {},
      std::string_view otherPossibleLocationDescription = "");

  // Similar to `checkVariableIsVisible` above, but performs the check for each
  // of the variables that are used inside the `expression`.
  void checkUsedVariablesAreVisible(
      const sparqlExpression::SparqlExpressionPimpl& expression,
      const std::string& locationDescription,
      const ad_utility::HashSet<Variable>& additionalVisibleVariables = {},
      std::string_view otherPossibleLocationDescription = "");

  // Add the `groupKeys` (either variables or expressions) to the query and
  // check whether all the variables are visible inside the query body. Uses
  // `internalVariableGenerator` to generate distinct internal variables as
  // required.
  void addGroupByClause(std::vector<GroupKey> groupKeys,
                        InternalVariableGenerator internalVariableGenerator);

  // Add the `havingClause` to the query. The argument `isGroupBy` denotes
  // whether the query performs a GROUP BY. If it is set to false, then an
  // exception is thrown (HAVING without GROUP BY is not allowed). The function
  // also throws if one of the variables that is used in the `havingClause` is
  // neither grouped nor aggregate by the expression it is contained in. Uses
  // `internalVariableGenerator` to generate distinct internal variables as
  // required.
  void addHavingClause(std::vector<SparqlFilter> havingClause, bool isGroupBy,
                       InternalVariableGenerator internalVariableGenerator);

  // Add the `orderClause` to the query. Throw an exception if the `orderClause`
  // is not valid. This might happen if it uses variables that are not visible
  // or (in case of a GROUP BY) not grouped or aggregated. Uses
  // `internalVariableGenerator` to generate distinct internal variables as
  // required.
  void addOrderByClause(OrderClause orderClause, bool isGroupBy,
                        std::string_view noteForImplicitGroupBy,
                        InternalVariableGenerator internalVariableGenerator);

 public:
  // Add the `modifiers` (like GROUP BY, HAVING, ORDER BY) to the query. Throw
  // an `InvalidQueryException` if the modifiers are invalid. This might happen
  // if one of the modifiers uses a variable that is either not visible in the
  // query before it is used, or if it uses a variable that is not properly
  // grouped or aggregated in the presence of a GROUP BY clause.
  // `internalVariableGenerator` has to generate distinct internal variables so
  // they can be used by operations that might be added as a consequence of this
  // function.
  void addSolutionModifiers(
      SolutionModifiers modifiers,
      InternalVariableGenerator internalVariableGenerator);

  // If this is a SELECT query, return all the selected aliases. Return an empty
  // vector for construct clauses.
  [[nodiscard]] const std::vector<Alias>& getAliases() const;
};

#endif  // QLEVER_SRC_PARSER_PARSEDQUERY_H
