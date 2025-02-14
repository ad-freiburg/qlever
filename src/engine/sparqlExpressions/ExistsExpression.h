//  Copyright 2025, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include <variant>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "parser/ParsedQuery.h"

// The expression that corresponds to the `EXISTS` function.
// The implementation only reads the value of a precomputed variable. The actual
// computation of EXISTS is done by the `ExistsJoin` class.
namespace sparqlExpression {
class ExistsExpression : public SparqlExpression {
 private:
  // The argument (a group graph pattern) of the EXISTS. This is set during the
  // parsing and is required and read by the `ExistsJoin` class.
  ParsedQuery argument_;

  // Each `ExistsExpression` has a unique index and a unique variable name that
  // is used to communicate between the `ExistsExpression` and the `ExistsJoin`.
  static inline std::atomic<size_t> indexCounter_ = 0;
  size_t index_ = ++indexCounter_;
  Variable variable_{absl::StrCat("?ql_internal_exists_", index_)};

 public:
  explicit ExistsExpression(ParsedQuery query) : argument_{std::move(query)} {}
  const auto& argument() const { return argument_; }
  const auto& variable() const { return variable_; }

  // Evaluate only reads the variable which is written by the `ExistsJoin`.
  ExpressionResult evaluate(EvaluationContext* context) const override {
    AD_CONTRACT_CHECK(context->_variableToColumnMap.contains(variable_));
    return variable_;
  }

  //____________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    if (varColMap.contains(variable_)) {
      return absl::StrCat("ExistsExpression col# ",
                          varColMap.at(variable_).columnIndex_);
    } else {
      // This means that the necessary `ExistsJoin` hasn't been set up yet. This
      // can for example happen if `getCacheKey` is called during the query
      // planning.
      return absl::StrCat("Uninitialized Exists: ",
                          ad_utility::FastRandomIntGenerator<size_t>{}());
    }
  }

  // This is in fact an `ExistsExpression`.
  bool isExistsExpression() const override { return true; }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};
}  // namespace sparqlExpression
