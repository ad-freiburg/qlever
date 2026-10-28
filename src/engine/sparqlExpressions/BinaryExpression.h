// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_BINARYEXPRESSION_H

#include <array>
#include <tuple>
#include <utility>

#include "engine/sparqlExpressions/ExpressionResultHelpers.h"
#include "engine/sparqlExpressions/HomogeneousNumericExpressionHelpers.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail {

// Generic infrastructure for binary expressions. The function and value
// getter(s) are specified using `FunctionAndValueGetters`. A single value
// getter can be used for both operands, or separate getters can be specified.
//
// This is a leaner variant of the `NaryExpression` infrastructure for exactly
// two operands: no specialized functions, no local-vocab results (the function
// has to return an `Id`), and a direct `std::visit` on both operand results.

// Evaluate a binary operation whose operands are already vectors or constants.
template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename Left, typename Right>
ExpressionResult evaluateBinaryOperationOnVectorOrConstant(
    Left&& left, Right&& right, EvaluationContext* context) {
  using namespace homogeneousNumeric;
  using LeftType = std::decay_t<Left>;
  using RightType = std::decay_t<Right>;

  Function function;
  using ValueGetters = std::tuple<LeftValueGetter, RightValueGetter>;

  // Constant–constant operands don't benefit from upfront classification.
  if constexpr (isConstantResult<LeftType> && isConstantResult<RightType>) {
    context->cancellationHandle_->throwIfCancelled();
    if (hasNonNumericOperand<Function, ValueGetters>(left, right)) {
      return Id::makeUndefined();
    }
    return function(LeftValueGetter{}(AD_FWD(left), context),
                    RightValueGetter{}(AD_FWD(right), context));

  } else if constexpr ((isVectorResult<LeftType> ||
                        isConstantResult<LeftType>) &&
                       (isVectorResult<RightType> ||
                        isConstantResult<RightType>)) {
    // Check for datatypes rejected by the numeric getters unless classification
    // proves that every value is accepted (including booleans). Only functions
    // with an `UNDEF` opt-in use this check; see `hasNonNumericOperand`.
    bool checkForValuesRejectedByGetter = true;
    // Use the shared numeric/date fast paths when both operands and value
    // getters support typed execution.
    if constexpr (supportsNumericFastPath<LeftValueGetter> &&
                  supportsNumericFastPath<RightValueGetter> &&
                  supportsNumericFastPathOperand<Left>() &&
                  supportsNumericFastPathOperand<Right>()) {
      constexpr bool includeDates = supportsDateOperands<ValueGetters>;
      const auto classifications =
          classifyNumericOperands<includeDates>(context, left, right);
      checkForValuesRejectedByGetter =
          classifications[0].containsValueRejectedByGetter_ ||
          classifications[1].containsValueRejectedByGetter_;

      if (auto homogeneousTypes = getHomogeneousNumericTypes(classifications);
          homogeneousTypes.has_value()) {
        return dispatchNumericTypes<includeDates>(
            homogeneousTypes.value(),
            [&left, &right, context](auto leftType,
                                     auto rightType) -> ExpressionResult {
              using LeftNumericType = typename decltype(leftType)::type;
              using RightNumericType = typename decltype(rightType)::type;

              return evaluateHomogeneousNumericOperation<
                  Function, LeftNumericType, RightNumericType>(
                  std::tie(left, right), context);
            });
      }

      if (auto majorityTypes = getMajorityNumericTypes(classifications);
          majorityTypes.has_value()) {
        return dispatchNumericTypes<includeDates>(
            majorityTypes.value(),
            [&left, &right, context](auto leftType,
                                     auto rightType) -> ExpressionResult {
              using LeftNumericType = typename decltype(leftType)::type;
              using RightNumericType = typename decltype(rightType)::type;

              return evaluateSpeculativeNumericOperation<
                  Function, LeftValueGetter, RightValueGetter, LeftNumericType,
                  RightNumericType>(left, right, context);
            });
      }
    }
    auto getLeft =
        makeIndexedValueGetter<LeftValueGetter>(AD_FWD(left), context);
    auto getRight =
        makeIndexedValueGetter<RightValueGetter>(AD_FWD(right), context);

    VectorWithMemoryLimit<Id> result{context->_allocator};
    result.reserve(context->size());

    ad_utility::chunkedForLoop<1000>(
        0, context->size(),
        [&left, &right, &result, &function, &getLeft, &getRight,
         checkForValuesRejectedByGetter](size_t i) {
          if (checkForValuesRejectedByGetter &&
              hasNonNumericOperand<Function, ValueGetters>(
                  getValueAt(left, i), getValueAt(right, i))) {
            result.push_back(Id::makeUndefined());
            return;
          }
          result.push_back(function(getLeft(i), getRight(i)));
        },
        [context]() { context->cancellationHandle_->throwIfCancelled(); });

    return result;
  } else {
    static_assert(ad_utility::alwaysFalse<std::tuple<LeftType, RightType>>,
                  "Unhandled binary expression operand types");
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
