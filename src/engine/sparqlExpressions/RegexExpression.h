//  Copyright 2022, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#pragma once

#include <string>

#include "engine/sparqlExpressions/SparqlExpression.h"

namespace sparqlExpression {
class RegexExpression : public SparqlExpression {
 private:
  SparqlExpression::Ptr child_;
  std::string regex_;
  bool isPrefixRegex_ = false;

 public:
  // `child` must be a `VariableExpression` and `regex` must be a `LiteralExpression` that stores a string, else an exception will be thrown.
  RegexExpression(SparqlExpression::Ptr child, SparqlExpression::Ptr regex);

  ExpressionResult evaluate(EvaluationContext* context) const override;

  std::span<SparqlExpression::Ptr> children() override;

  // _________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;
};
}  // namespace sparqlExpression
