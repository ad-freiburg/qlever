//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#pragma once

#include "engine/sparqlExpressions/SparqlExpression.h"
#include "global/ValueIdComparators.h"

namespace sparqlExpression::relational {

using valueIdComparators::Comparison;

// The class template for all the relational expressions like "less than",
// "greater equal", "not equal" etc. The comparison enum is specified as a
// template parameter for efficiency reasons.
template <Comparison Comp>
class RelationalExpression : public SparqlExpression {
 public:
  // All relational expressions are binary.
  using Children = std::array<SparqlExpression::Ptr, 2>;

 private:
  Children children_;
  static constexpr Comparison comparison_ = Comp;

 public:
  // Construct from the two children.
  explicit RelationalExpression(Children children)
      : children_{std::move(children)} {}

  ExpressionResult evaluate(EvaluationContext* context) const override;

  std::span<SparqlExpression::Ptr> children() override;

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;
};

}  // namespace sparqlExpression::relational

namespace sparqlExpression {
// Define aliases for the six relevant relational expressions.
using LessThanExpression =
    relational::RelationalExpression<valueIdComparators::Comparison::LT>;
using LessEqualExpression =
    relational::RelationalExpression<valueIdComparators::Comparison::LE>;
using EqualExpression =
    relational::RelationalExpression<valueIdComparators::Comparison::EQ>;
using NotEqualExpression =
    relational::RelationalExpression<valueIdComparators::Comparison::NE>;
using GreaterThanExpression =
    relational::RelationalExpression<valueIdComparators::Comparison::GT>;
using GreaterEqualExpression =
    relational::RelationalExpression<valueIdComparators::Comparison::GE>;
}  // namespace sparqlExpression
