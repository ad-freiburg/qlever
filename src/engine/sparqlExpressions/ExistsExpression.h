// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EXISTSEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EXISTSEXPRESSION_H

#include <variant>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "parser/ParsedQuery.h"

// The `SparqlExpression` for `EXISTS`. The implementation is straightforward
// because it only reads the value computed by the special `ExistsJoin`
// operation, where the actual work is done (see the comments there).
namespace sparqlExpression {
class ExistsExpression : public SparqlExpression {
 private:
  // The argument of the `EXISTS`, which is a group graph pattern. This is set
  // during parsing and is used by the `ExistsJoin` operation.
  ParsedQuery argument_;

  // Each `ExistsExpression` has a unique index and a unique variable name that
  // is used to communicate the result computed by the `ExistsJoin` to this
  // `ExistsExpression`.
  static inline std::atomic<size_t> indexCounter_ = 0;
  size_t index_ = ++indexCounter_;
  Variable variable_{absl::StrCat("?ql_internal_exists_", index_)};

 public:
  explicit ExistsExpression(ParsedQuery query) : argument_{std::move(query)} {}
  const auto& argument() const { return argument_; }
  const auto& variable() const { return variable_; }

  // To evaluate, just return the variable of the column computed by the
  // `ExistsJoin`.
  ExpressionResult evaluate(EvaluationContext* context) const override {
    AD_CONTRACT_CHECK(context->_variableToColumnMap.contains(variable_));
    return variable_;
  }

  // Return the cache key, which in the normal case depends on the column index
  // of the variable computed by the `ExistsJoin`.
  //
  // There is a special case, where the corresponding `ExistsJoin` has not
  // been set up yet (because the query planning is not yet complete). Since we
  // cannot cache incomplete operations, we return a random cache key in this
  // case.
  [[nodiscard]] std::string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    if (varColMap.contains(variable_)) {
      return absl::StrCat("ExistsExpression col# ",
                          varColMap.at(variable_).columnIndex_);
    } else {
      // This means that the necessary `ExistsJoin` hasn't been set up yet. For
      // example, this can happen if `getCacheKey` is called during query
      // planning (which is done to avoid redundant evaluation in the case of
      // identical subtrees in the query plan).
      return absl::StrCat("Uninitialized Exists: ",
                          ad_utility::FastRandomIntGenerator<size_t>{}());
    }
  }

  // This is the one expression, where this function should return `true`.
  // Used to extract `EXISTS` expressions from a general expression tree.
  bool isExistsExpression() const override { return true; }

  // Return all the variables that are used in this expression.
  ql::span<const Variable> getContainedVariablesNonRecursive() const override {
    return argument_.selectClause().getSelectedVariables();
  }

  // Set the `SELECT` of the argument of this exists expression to all the
  // variables that are visible in the argument AND contained in variables.
  void selectVariables(ql::span<const Variable> variables) {
    std::vector<parsedQuery::SelectClause::VarOrAlias> intersection;
    const auto& visibleVariables = argument_.getVisibleVariables();
    for (const auto& var : variables) {
      if (ad_utility::contains(visibleVariables, var)) {
        intersection.emplace_back(var);
      }
    }
    argument_.selectClause().setSelected(intersection);
  }

 private:
  ql::span<Ptr> childrenImpl() override { return {}; }
};
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_EXISTSEXPRESSION_H
