//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
class LangExpression : public SparqlExpression {
 private:
  Variable variable_{"?uninitialized"};

 public:
  LangExpression(SparqlExpression::Ptr child) {
    if (auto stringPtr = dynamic_cast<const VariableExpression*>(child.get())) {
      variable_ = stringPtr->value();
    } else {
      throw std::runtime_error{
          "The argument to the LANG function must be a single variable"};
    }
  }

  const Variable& variable() const { return variable_; }

  ExpressionResult evaluate(EvaluationContext*) const override { AD_FAIL(); }

  std::string getCacheKey(const VariableToColumnMap&) const override {
    AD_FAIL();
  }

  std::span<SparqlExpression::Ptr> children() override { AD_FAIL(); }

  bool containsLangExpression() const override { return true; }
};
}  // namespace sparqlExpression
