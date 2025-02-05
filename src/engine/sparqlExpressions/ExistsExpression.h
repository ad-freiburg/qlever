// Copyright 2025, University of Freiburg
// Chair of Algorithms and Data Structures
// Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

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
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    if (varColMap.contains(variable_)) {
      return absl::StrCat("ExistsExpression col# ",
                          varColMap.at(variable_).columnIndex_);
    } else {
      return std::to_string(ad_utility::FastRandomIntGenerator<size_t>{}());
    }
  }

  // This is the one expression, where this function should return `true`.
  // Used to extract `EXISTS` expressions from a general expression tree.
  bool isExistsExpression() const override { return true; }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};
}  // namespace sparqlExpression
