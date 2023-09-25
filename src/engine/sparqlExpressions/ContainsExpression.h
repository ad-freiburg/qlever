//  Copyright 2023, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Noah Nock <noah.v.nock@gmail.com>

#pragma once

#include <string>

#include "engine/sparqlExpressions/LiteralExpression.h"
#include "engine/sparqlExpressions/SparqlExpression.h"

class BoundingBoxType;

namespace sparqlExpression {
class ContainsExpression : public SparqlExpression {
 private:
  SparqlExpression::Ptr child_;
  std::string boundingBoxAsString_;
  BoundingBoxType* boundingBox_{};

 public:
  // `child` must be a `VariableExpression` and `boundingBox` must be a
  // `LiteralExpression` that stores a string, else an exception will be thrown.
  ContainsExpression(SparqlExpression::Ptr child,
                     SparqlExpression::Ptr boundingBox);

  ExpressionResult evaluate(EvaluationContext* context) const override;

  std::span<SparqlExpression::Ptr> childrenImpl() override;

  // _________________________________________________________________________
  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;
};
}  // namespace sparqlExpression
