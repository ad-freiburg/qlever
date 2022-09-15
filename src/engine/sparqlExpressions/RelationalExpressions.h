//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_RELATIONALEXPRESSIONS_H
#define QLEVER_RELATIONALEXPRESSIONS_H

#include "../../global/ValueIdComparators.h"
#include "./SparqlExpression.h"

#endif  // QLEVER_RELATIONALEXPRESSIONS_H

namespace sparqlExpression::relational {

using valueIdComparators::Comparison;

// The class template for all the relational expressions like "less than",
// "greater equal", "not equal" etc. The comparison function is specified as a
// template parameter for efficiency reasons.
template <Comparison Comp>
class RelationalExpression : public SparqlExpression {
 public:
  // All relational expressions are binary.
  using Children = std::array<SparqlExpression::Ptr, 2>;

 private:
  Children _children;
  static constexpr Comparison _comparison = Comp;

 public:
  // Construct from the two children.
  explicit RelationalExpression(Children children)
      : _children{std::move(children)} {}

  ExpressionResult evaluate(EvaluationContext* context) const override;

  std::span<SparqlExpression::Ptr> children() override;

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;
};

// Define aliases for the six relevant relational expressions.
using LessThanExpression = RelationalExpression<Comparison::LT>;
using LessEqualExpression = RelationalExpression<Comparison::LE>;
using EqualExpression = RelationalExpression<Comparison::EQ>;
using NotEqualExpression = RelationalExpression<Comparison::NE>;
using GreaterThanExpression = RelationalExpression<Comparison::GT>;
using GreaterEqualExpression = RelationalExpression<Comparison::GE>;

}  // namespace sparqlExpression::relational

namespace sparqlExpression {
// Make the relational expressions accessible from the `sparqlExpression`
// namespace.
using relational::EqualExpression;
using relational::GreaterEqualExpression;
using relational::GreaterThanExpression;
using relational::LessEqualExpression;
using relational::LessThanExpression;
using relational::NotEqualExpression;
}  // namespace sparqlExpression
