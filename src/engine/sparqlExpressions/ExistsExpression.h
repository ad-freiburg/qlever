//
// Created by kalmbacj on 1/7/25.
//

#pragma once

#include <variant>

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "parser/ParsedQuery.h"

namespace sparqlExpression {
class ExistsExpression : public SparqlExpression {
 private:
  ParsedQuery argument_;
  static inline std::atomic<size_t> indexCounter_ = 0;
  size_t index_ = ++indexCounter_;
  Variable variable_{absl::StrCat("?ql_internal_exists_", index_)};

 public:
  const auto& argument() const { return argument_; }
  const auto& variable() const { return variable_; }
  ExistsExpression(ParsedQuery query) : argument_{std::move(query)} {}

  ExpressionResult evaluate(EvaluationContext* context) const override {
    AD_CONTRACT_CHECK(context->_variableToColumnMap.contains(variable_));
    return variable_;
  }

  //____________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    // TODO<joka921> get a proper cache key here
    AD_CONTRACT_CHECK(varColMap.contains(variable_));
    return absl::StrCat("EXISTS WITH COL ",
                        varColMap.at(variable_).columnIndex_);
  }

  // ____________________________________________________________________________
  bool isExistsExpression() const override { return true; }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};
}  // namespace sparqlExpression
