//  Copyright 2021, University of Freiburg,
//                  Chair of Algorithms and Data Structures.
//  Author: Johannes Kalmbach <kalmbacj@cs.uni-freiburg.de>

#include "engine/sparqlExpressions/AggregateExpression.h"

#include "engine/sparqlExpressions/GroupConcatExpression.h"

namespace sparqlExpression {
namespace detail {

// _____________________________________________________________________________
template <typename AggregateOperation, typename FinalOperation>
template <SingleExpressionResult Operand>
auto AggregateExpression<AggregateOperation, FinalOperation>::
    EvaluateOnChildOperand::operator()(
        const AggregateOperation& aggregateOperation,
        const ValueId resultForEmptyGroup, const FinalOperation& finalOperation,
        EvaluationContext* context, bool distinct, Operand&& operand)
        -> ExpressionResult {
  // Perform the more efficient calculation on `SetOfInterval`s if it is
  // possible.
  if (isAnySpecializedFunctionPossible(aggregateOperation._specializedFunctions,
                                       operand)) {
    auto optionalResult = evaluateOnSpecializedFunctionsIfPossible(
        aggregateOperation._specializedFunctions,
        std::forward<Operand>(operand));
    AD_CONTRACT_CHECK(optionalResult);
    return std::move(optionalResult.value());
  }

  // The number of values we aggregate.
  auto inputSize = getResultSize(*context, operand);

  // If there are no values, return the neutral element. It is important to
  // handle this case separately, because the following code only words if
  // there is at least one value.
  if (inputSize == 0) {
    return resultForEmptyGroup;
  }

  // All aggregate operations are binary, with the same value getter for each
  // operand.
  {
    using V = typename AggregateOperation::ValueGetters;
    static_assert(std::tuple_size_v<V> == 2);
    static_assert(
        std::is_same_v<std::tuple_element_t<0, V>, std::tuple_element_t<1, V>>);
  }
  const auto& valueGetter = std::get<0>(aggregateOperation._valueGetters);

  // Helper lambda for aggregating two values.
  //
  // TODO: Why is the `context` not passed by reference?
  auto aggregateTwoValues = [&aggregateOperation, context](
                                auto&& x, auto&& y) -> decltype(auto) {
    if constexpr (requires {
                    aggregateOperation._function(AD_FWD(x), AD_FWD(y));
                  }) {
      return aggregateOperation._function(AD_FWD(x), AD_FWD(y));
    } else {
      return aggregateOperation._function(AD_FWD(x), AD_FWD(y), context);
    }
  };

  // A generator for the operands (before the value getter is applied to get
  // the actual values).
  auto operands =
      makeGenerator(std::forward<Operand>(operand), inputSize, context);

  // Set up cancellation handling.
  auto checkCancellation =
      [context](ad_utility::source_location location =
                    ad_utility::source_location::current()) {
        context->cancellationHandle_->throwIfCancelled(location);
      };

  // Lambda to compute the aggregate of the given operands. This requires
  // that `inputs` is not empty.
  auto computeAggregate = [&valueGetter, context, &finalOperation,
                           &aggregateTwoValues,
                           &checkCancellation](auto&& inputs) {
    auto it = inputs.begin();
    AD_CORRECTNESS_CHECK(it != inputs.end());

    using ResultType = std::decay_t<decltype(aggregateTwoValues(
        std::move(valueGetter(*it, context)), valueGetter(*it, context)))>;
    ResultType result = valueGetter(*it, context);
    checkCancellation();
    size_t numValues = 1;

    for (++it; it != inputs.end(); ++it) {
      result = aggregateTwoValues(std::move(result),
                                  valueGetter(std::move(*it), context));
      checkCancellation();
      ++numValues;
    }
    result = finalOperation(std::move(result), numValues);
    checkCancellation();
    return result;
  };

  // Compute the aggregate (over all values or, if this is a DISTINCT
  // aggregate, only over the distinct values).
  auto result = [&]() {
    if (distinct) {
      auto uniqueValues =
          getUniqueElements(context, inputSize, std::move(operands));
      checkCancellation();
      return computeAggregate(std::move(uniqueValues));
    } else {
      return computeAggregate(std::move(operands));
    }
  }();

  // If the result is numeric, convert it to an `Id`.
  //
  // TODO<joka921> Check if this is really necessary, or if we can also use
  // IDs in the intermediate steps without loss of efficiency.
  if constexpr (requires { makeNumericId(result); }) {
    return makeNumericId(result);
  } else {
    return result;
  }
};

// __________________________________________________________________________
template <typename AggregateOperation, typename FinalOperation>
AggregateExpression<AggregateOperation, FinalOperation>::AggregateExpression(
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
        return EvaluateOnChildOperand{}(_aggregateOp, this->resultForEmptyGroup(),
                                      FinalOperation{}, context, _distinct,
                                      AD_FWD(arg));
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

// Explicit instantiation for the AVG expression.
template class AggregateExpression<AvgOperation, decltype(avgFinalOperation)>;

// Explicit instantiations for the other aggregate expressions.
#define INSTANTIATE_AGG_EXP(Function, ValueGetter) \
  template class AggregateExpression<              \
      Operation<2, FunctionAndValueGetters<Function, ValueGetter>>>;
INSTANTIATE_AGG_EXP(decltype(addForSum), NumericValueGetter);
INSTANTIATE_AGG_EXP(decltype(count), IsValidValueGetter);
INSTANTIATE_AGG_EXP(decltype(minLambdaForAllTypes), ActualValueGetter);
INSTANTIATE_AGG_EXP(decltype(maxLambdaForAllTypes), ActualValueGetter);
}  // namespace detail
}  // namespace sparqlExpression
