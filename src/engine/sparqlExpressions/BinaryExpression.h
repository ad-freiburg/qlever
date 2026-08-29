#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H

#include <array>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail {

// Generic infrastructure for binary expressions. The function and value
// getter(s) are specified using `FunctionAndValueGetters`. A single value
// getter can be used for both operands, or separate getters can be specified.
//
// This is more efficient than the generic `NaryExpression` infrastructure for
// binary expressions because the operation is applied in direct loops over
// vector or constant operands, avoiding the generator-based per-element
// abstraction.

template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperation(Left&& left, Right&& right,
                                         EvaluationContext* context);

template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperationOnVectorOrConstant(
    Left&& left, Right&& right, EvaluationContext* context);

template <typename FunctionAndValueGettersT>
class BinaryExpression;

// Binary expression whose operation and value getters are known at compile
// time.
template <typename Function, typename... ValueGetters>
class BinaryExpression<FunctionAndValueGetters<Function, ValueGetters...>>
    : public NaryExpressionBase<2> {
 public:
  using Base = NaryExpressionBase<2>;
  using Children = typename Base::Children;
  using Getters = ValueGetterPack<2, std::tuple<ValueGetters...>>;
  using LeftValueGetter = std::tuple_element_t<0, Getters>;
  using RightValueGetter = std::tuple_element_t<1, Getters>;
  BinaryExpression(SparqlExpression::Ptr lhs, SparqlExpression::Ptr rhs)
      : Base{Children{std::move(lhs), std::move(rhs)}} {}

  ExpressionResult evaluate(EvaluationContext* context) const override {
    auto leftResult = this->children_[0]->evaluate(context);
    auto rightResult = this->children_[1]->evaluate(context);

    auto visitor = [context](auto&& left, auto&& right) -> ExpressionResult {
      return evaluateBinaryOperation<Function, LeftValueGetter,
                                     RightValueGetter>(AD_FWD(left),
                                                       AD_FWD(right), context);
    };

    return std::visit(visitor, std::move(leftResult), std::move(rightResult));
  }
};

// Convert an expression result into either a vector-like or constant
// representation that can be handled directly by the binary evaluation loop.
template <typename T>
decltype(auto) convertToVectorOrConstant(T&& value,
                                         EvaluationContext* context) {
  using Type = std::decay_t<T>;

  if constexpr (ad_utility::isSimilar<Type, ad_utility::SetOfIntervals>) {
    return ad_utility::SetOfIntervals::toIdVector(value, context->size(),
                                                  context->_allocator);
  } else if constexpr (ad_utility::isSimilar<Type, ::Variable>) {
    return getIdsFromVariable(value, context);
  } else {
    static_assert(isVectorResult<Type> || isConstantResult<Type>,
                  "BinaryExpression only supports vectors and constants after "
                  "conversion");
    return AD_FWD(value);
  }
}

// Convert both operands to the supported representations and evaluate the
// binary operation.
template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperation(Left&& left, Right&& right,
                                         EvaluationContext* context) {
  decltype(auto) leftConverted =
      convertToVectorOrConstant(AD_FWD(left), context);
  decltype(auto) rightConverted =
      convertToVectorOrConstant(AD_FWD(right), context);

  return evaluateBinaryOperationOnVectorOrConstant<Function, LeftValueGetter,
                                                   RightValueGetter>(
      AD_FWD(leftConverted), AD_FWD(rightConverted), context);
}

// Return a callable that provides the converted value at index `i`. For
// constant operands, the converted value is computed only once.
template <typename ValueGetter, typename Operand>
auto makeIndexedValueGetter(Operand&& operand, EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    AD_CORRECTNESS_CHECK(operand.size() == context->size());

    // TODO: The generator-based `NaryExpression` infrastructure forwards/moves
    // individual values into the value getter. Here, indexed vector elements
    // are passed as lvalues. This is irrelevant for the currently used value
    // getters, but should be revisited for move-sensitive value types.
    return [&operand, context](size_t i) {
      return ValueGetter{}(operand[i], context);
    };
  } else {
    return [value = ValueGetter{}(AD_FWD(operand), context)](
               size_t) -> decltype(auto) { return (value); };
  }
}

// Evaluate a binary operation whose operands are already vectors or constants.
template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperationOnVectorOrConstant(
    Left&& left, Right&& right, EvaluationContext* context) {
  using LeftType = std::decay_t<Left>;
  using RightType = std::decay_t<Right>;

  Function function;

  // Case 1: constant–constant.
  if constexpr (isConstantResult<LeftType> && isConstantResult<RightType>) {
    context->cancellationHandle_->throwIfCancelled();
    return function(LeftValueGetter{}(AD_FWD(left), context),
                    RightValueGetter{}(AD_FWD(right), context));

  } else if constexpr ((isVectorResult<LeftType> ||
                        isConstantResult<LeftType>) &&
                       (isVectorResult<RightType> ||
                        isConstantResult<RightType>)) {
    auto getLeft =
        makeIndexedValueGetter<LeftValueGetter>(AD_FWD(left), context);
    auto getRight =
        makeIndexedValueGetter<RightValueGetter>(AD_FWD(right), context);

    VectorWithMemoryLimit<Id> result{context->_allocator};
    result.reserve(context->size());

    ad_utility::chunkedForLoop<1000>(
        0, context->size(),
        [&](size_t i) { result.push_back(function(getLeft(i), getRight(i))); },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });

    return result;

  } else {
    static_assert(ad_utility::alwaysFalse<std::tuple<LeftType, RightType>>,
                  "Unhandled binary expression operand types");
  }
}
#ifdef _QLEVER_TYPE_ERASED_EXPRESSIONS

#define BINARY_EXPRESSION(Name, ...) NARY_EXPRESSION(Name, 2, __VA_ARGS__)

#else

#define BINARY_EXPRESSION(Name, ...)                  \
  class Name : public BinaryExpression<__VA_ARGS__> { \
    using Base = BinaryExpression<__VA_ARGS__>;       \
    using Base::Base;                                 \
  }

#endif

}  // namespace sparqlExpression::detail

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H
