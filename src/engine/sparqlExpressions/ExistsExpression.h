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
  std::variant<Variable, ParsedQuery> argument_;

 public:
  auto& argument() { return argument_; }
  ExistsExpression(ParsedQuery query) : argument_{std::move(query)} {}

  ExpressionResult evaluate(EvaluationContext* context) const override {
    AD_CONTRACT_CHECK(std::holds_alternative<Variable>(argument_));
    return std::get<Variable>(argument_);
  }

  //_________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override {
    // TODO<joka921> get a proper cache key here
    AD_CONTRACT_CHECK(std::holds_alternative<Variable>(argument_));
    return absl::StrCat(
        "EXISTS WITH COLUMN ",
        varColMap.at(std::get<Variable>(argument_)).columnIndex_);
  }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};
}  // namespace sparqlExpression
