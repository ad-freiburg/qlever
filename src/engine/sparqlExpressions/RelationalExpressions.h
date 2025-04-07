//  Copyright 2022, University of Freiburg,
//  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbach@cs.uni-freiburg.de>

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_RELATIONALEXPRESSIONS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_RELATIONALEXPRESSIONS_H

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

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // Check if this expression has the form `LANG(?var) = "literal"` and return
  // the appropriate data.
  std::optional<LangFilterData> getLanguageFilterExpression() const override;

  // If this `RelationalExpression` is binary evaluable, return the
  // corresponding `PrefilterExpression` for the pre-filtering procedure on
  // `CompressedBlockMetadata`. In addition we return the `Variable` that
  // corresponds to the sorted column.
  std::vector<PrefilterExprVariablePair> getPrefilterExpressionForMetadata(
      [[maybe_unused]] bool isNegated) const override;

  // These expressions are typically used inside `FILTER` clauses, so we need
  // proper estimates.
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSizeEstimate,
      const std::optional<Variable>& firstSortedVariable) const override;

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override;
};

// Implementation of the `IN` expression
class InExpression : public SparqlExpression {
 public:
  using Children = std::vector<SparqlExpression::Ptr>;

 private:
  // The first child implicitly is the left hand side.
  Children children_;

 public:
  // Construct from the two children.
  explicit InExpression(SparqlExpression::Ptr lhs, Children children) {
    children_.reserve(children.size() + 1);
    children_.push_back(std::move(lhs));
    ql::ranges::move(children, std::back_inserter(children_));
  }

  ExpressionResult evaluate(EvaluationContext* context) const override;

  [[nodiscard]] string getCacheKey(
      const VariableToColumnMap& varColMap) const override;

  // These expressions are typically used inside `FILTER` clauses, so we need
  // proper estimates.
  Estimates getEstimatesForFilterExpression(
      uint64_t inputSizeEstimate,
      const std::optional<Variable>& firstSortedVariable) const override;

 private:
  std::span<SparqlExpression::Ptr> childrenImpl() override;
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

using InExpression = relational::InExpression;
}  // namespace sparqlExpression

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_RELATIONALEXPRESSIONS_H
