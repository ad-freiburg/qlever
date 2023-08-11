//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/AggregateExpression.h"

#include "engine/sparqlExpressions/GroupConcatExpression.h"

namespace sparqlExpression {
namespace detail {

// __________________________________________________________________________
template <typename AggregateOperation, typename FinalOp>
AggregateExpression<AggregateOperation, FinalOp>::AggregateExpression(
    bool distinct, Ptr&& child, AggregateOperation aggregateOp)
    : _distinct(distinct),
      _child{std::move(child)},
      _aggregateOp{std::move(aggregateOp)} {
  setIsInsideAggregate();
}

// __________________________________________________________________________
template <typename AggregateOperation, typename FinalOperation>
ExpressionResult
AggregateExpression<AggregateOperation, FinalOperation>::evaluate(
    EvaluationContext* context) const {
  auto childResult = _child->evaluate(context);

  return std::visit(
      [this, context](auto&& arg) {
        return evaluateOnChildOperand(_aggregateOp, FinalOperation{}, context,
                                      _distinct, AD_FWD(arg));
      },
      std::move(childResult));
}

// _________________________________________________________________________
template <typename AggregateOperation, typename FinalOperation>
std::span<SparqlExpression::Ptr>
AggregateExpression<AggregateOperation, FinalOperation>::childrenImpl() {
  return {&_child, 1};
}

// _________________________________________________________________________
template <typename AggregateOperation, typename FinalOperation>
vector<Variable> AggregateExpression<
    AggregateOperation, FinalOperation>::getUnaggregatedVariables() {
  // This is an aggregate, so it never leaves any unaggregated variables.
  return {};
}

// __________________________________________________________________________
template <typename AggregateOperation, typename FinalOperation>
[[nodiscard]] string
AggregateExpression<AggregateOperation, FinalOperation>::getCacheKey(
    const VariableToColumnMap& varColMap) const {
  return std::string(typeid(*this).name()) + std::to_string(_distinct) + "(" +
         _child->getCacheKey(varColMap) + ")";
}

// __________________________________________________________________________
template <typename AggregateOperation, typename FinalOperation>
[[nodiscard]] std::optional<SparqlExpressionPimpl::VariableAndDistinctness>
AggregateExpression<AggregateOperation, FinalOperation>::getVariableForCount()
    const {
  // This behavior is not correct for the `COUNT` aggregate. The count is
  // therefore implemented in a separate `CountExpression` class, which
  // overrides this function.
  return std::nullopt;
}

#define INSTANTIATE_AGG_EXP(...)      \
  template class AggregateExpression< \
      Operation<2, FunctionAndValueGetters<__VA_ARGS__>>>;
INSTANTIATE_AGG_EXP(decltype(addForSum), NumericValueGetter);

template class AggregateExpression<
    AGG_OP<decltype(addForSum), NumericValueGetter>, decltype(averageFinalOp)>;

INSTANTIATE_AGG_EXP(decltype(count), IsValidValueGetter);
INSTANTIATE_AGG_EXP(decltype(minLambdaForAllTypes), ActualValueGetter);
INSTANTIATE_AGG_EXP(decltype(maxLambdaForAllTypes), ActualValueGetter);
}  // namespace detail
}  // namespace sparqlExpression
