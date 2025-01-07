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
    if (varColMap.contains(variable_)) {
      return absl::StrCat("EXISTS WITH COL ",
                          varColMap.at(variable_).columnIndex_);
    } else {
      // This means that the necessary `ExistsScan` hasn't been set up yet.
      // It is not possible to cache such incomplete operations, so we return
      // a random cache key.
      return std::to_string(ad_utility::FastRandomIntGenerator<size_t>{}());
    }
  }

  // ____________________________________________________________________________
  bool isExistsExpression() const override { return true; }

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override { return {}; }
};
}  // namespace sparqlExpression
