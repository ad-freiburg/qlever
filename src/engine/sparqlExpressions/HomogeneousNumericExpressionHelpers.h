// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <tuple>
#include <type_traits>
#include <utility>

#include "backports/concepts.h"
#include "engine/CallFixedSize.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/NumericOperandClassification.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"
#include "util/CompilerExtensions.h"
#include "util/TypeIdentity.h"

namespace sparqlExpression::detail::homogeneousNumeric {

// Helpers for evaluating numeric expressions directly on primitive C++ numeric
// types after operand classification (see `NumericOperandClassification.h`).
//
// The generic numeric value getters return variants and therefore require
// variant dispatch for every result row. These helpers avoid that overhead for
// homogeneous inputs and provide a speculative fast path for mixed inputs.

// Map supported numeric datatypes to their primitive C++ types.
inline constexpr auto numericTypeMap = std::tuple{
    std::pair{NumericType::Int, ad_utility::use_type_identity::ti<int64_t>},
    std::pair{NumericType::Double, ad_utility::use_type_identity::ti<double>}};

// Extract the primitive numeric value from a `ValueId` whose datatype was
// already established by homogeneous classification.
template <typename Primitive>
Primitive getPrimitiveNumericValue(ValueId value) {
  if constexpr (ql::concepts::same_as<Primitive, int64_t>) {
    return value.getInt();
  } else if constexpr (ql::concepts::same_as<Primitive, double>) {
    return value.getDouble();
  } else {
    static_assert(ad_utility::alwaysFalse<Primitive>,
                  "Unsupported homogeneous numeric type");
  }
}

// Return an indexed getter that extracts already-classified primitive numeric
// values from either a vector-like operand or a constant.
template <typename Primitive, typename Operand>
auto makeHomogeneousNumericGetter(const Operand& operand) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    return [&operand](size_t i) {
      return getPrimitiveNumericValue<Primitive>(operand[i]);
    };
  } else {
    static_assert(ad_utility::isSimilar<OperandType, ValueId>,
                  "Homogeneous numeric fast path currently supports "
                  "ValueId constants");

    const auto value = getPrimitiveNumericValue<Primitive>(operand);

    return [value]([[maybe_unused]] size_t index) { return value; };
  }
}

// Select the function used for evaluation on primitive numeric types.
// By default, use the original function directly for primitive numeric inputs.
template <typename Function>
struct RawNumericFunction {
  using type = Function;
};

// Unwrap `MakeNumericExpression` so that the fast path operates directly on
// primitive numeric types while preserving numeric `ValueId` construction.
template <typename Function, bool NanOrInfToUndef>
struct RawNumericFunction<MakeNumericExpression<Function, NanOrInfToUndef>> {
  using type = NumericIdWrapper<Function, NanOrInfToUndef>;
};

// The function type used by the homogeneous numeric evaluation loop.
template <typename Function>
using RawNumericFunctionT = typename RawNumericFunction<Function>::type;

// Map a supported numeric type to the corresponding compile-time index used
// for dispatching to the primitive C++ numeric type. The `type` must be one of
// the types in `numericTypeMap` above, in particular it must not be `Other`,
// which has no primitive C++ type. Callers therefore have to classify
// all operands first and only dispatch if none of them is `Other`, see
// `evaluateBinaryOperationOnVectorOrConstant`.
template <size_t I = 0>
inline int numericTypeToIndex(NumericType type) {
  if constexpr (I == std::tuple_size_v<decltype(numericTypeMap)>) {
    AD_FAIL();
  } else {
    if (std::get<I>(numericTypeMap).first == type) {
      return static_cast<int>(I);
    }
    return numericTypeToIndex<I + 1>(type);
  }
}

// Dispatch runtime numeric types to compile-time primitive numeric types and
// invoke the supplied function with those types.
template <size_t N, typename Function>
decltype(auto) dispatchNumericTypes(const std::array<NumericType, N>& types,
                                    Function&& function) {
  std::array<int, N> indices;

  ql::ranges::transform(types, indices.begin(), [](NumericType type) {
    return numericTypeToIndex(type);
  });

  constexpr int maxIndex = std::tuple_size_v<decltype(numericTypeMap)> - 1;

  return ad_utility::callFixedSizeVi<maxIndex>(
      indices, [function = AD_FWD(function)](auto... typeIndices) {
        return function(
            std::get<decltype(typeIndices)::value>(numericTypeMap).second...);
      });
}

// Check that a vector-like operand has the expected size. Constant operands
// don't require a size check.
template <typename Operand>
void checkNumericOperandSize(const Operand& operand,
                             const EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;
  if constexpr (isVectorResult<OperandType>) {
    AD_CORRECTNESS_CHECK(ql::ranges::size(operand) == context->size());
  }
}

// Apply the size check to all operands of a homogeneous numeric operation.
template <typename... Operands>
void checkNumericOperandSizes(const std::tuple<Operands...>& operands,
                              EvaluationContext* context) {
  std::apply(
      [context](const auto&... operand) {
        (checkNumericOperandSize(operand, context), ...);
      },
      operands);
}

// Evaluate a homogeneous numeric operation when at least one operand is
// non-constant. For the currently supported operand representations, this
// means that at least one operand is vector-like.
template <typename Function, typename... NumericTypes, typename... Operands>
ExpressionResult evaluateHomogeneousNumericOperation(
    std::tuple<Operands...> operands, EvaluationContext* context) {
  static_assert(sizeof...(NumericTypes) == sizeof...(Operands));
  static_assert((... || isVectorResult<std::decay_t<Operands>>),
                "At least one operand must be vector-like");

  // Check the size of every vector-like operand.
  checkNumericOperandSizes(operands, context);

  using FastFunction = RawNumericFunctionT<Function>;
  FastFunction function;

  // Create one primitive numeric getter for every operand.
  auto getters = std::apply(
      [](const auto&... operand) {
        return std::tuple{
            makeHomogeneousNumericGetter<NumericTypes>(operand)...};
      },
      operands);

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.resize(context->size());

  ad_utility::chunkedForLoop<1000>(
      0, context->size(),
      [&result, &function, &getters](size_t i) {
        std::apply(
            [&result, &function, i](const auto&... getter) {
              result[i] = function(getter(i)...);
            },
            getters);
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return result;
}

// Return the `ValueId` at `index` for a supported operand. Constant operands
// return the same value for every index.
template <typename Operand>
ValueId getIdAt(const Operand& operand, size_t index) {
  using OperandType = std::decay_t<Operand>;

  static_assert(supportsNumericFastPathOperand<Operand>(),
                "Unsupported operand representation for numeric fast path");

  if constexpr (isVectorResult<OperandType>) {
    return operand[index];
  } else {
    return operand;
  }
}

// The fallback path for the speculative evaluation below when a pair of
// operands does not match the majority types. Integer and double combinations
// are dispatched explicitly because this was measurably faster than always
// using the generic fallback. Other datatypes use the regular value getters to
// preserve the generic expression semantics.
template <typename Function, typename LeftValueGetter,
          typename RightValueGetter>
AD_NO_INLINE Id evaluateSpeculativeNumericSlowPath(
    ValueId leftValue, ValueId rightValue, EvaluationContext* context,
    RawNumericFunctionT<Function>& fastFunction, Function& genericFunction) {
  using enum Datatype;
  const auto leftType = leftValue.getDatatype();
  const auto rightType = rightValue.getDatatype();

  if (leftType == Int && rightType == Int) {
    return fastFunction(leftValue.getInt(), rightValue.getInt());
  }

  if (leftType == Int && rightType == Double) {
    return fastFunction(leftValue.getInt(), rightValue.getDouble());
  }

  if (leftType == Double && rightType == Int) {
    return fastFunction(leftValue.getDouble(), rightValue.getInt());
  }

  if (leftType == Double && rightType == Double) {
    return fastFunction(leftValue.getDouble(), rightValue.getDouble());
  }

  return genericFunction(LeftValueGetter{}(leftValue, context),
                         RightValueGetter{}(rightValue, context));
}

// Return the `ValueId` datatype corresponding to a primitive numeric C++ type.
template <typename Primitive>
constexpr Datatype datatypeForNumericType() {
  if constexpr (ql::concepts::same_as<Primitive, int64_t>) {
    return Datatype::Int;
  } else {
    static_assert(ql::concepts::same_as<Primitive, double>,
                  "Unsupported primitive numeric type");
    return Datatype::Double;
  }
}

// Evaluate a numeric binary operation using one expected numeric datatype for
// each operand. Rows that match both expected datatypes stay on the small hot
// path. All other rows are handled by the out-of-line slow path.
template <typename Function, typename LeftValueGetter,
          typename RightValueGetter, typename LeftNumericType,
          typename RightNumericType, typename Left, typename Right>
ExpressionResult evaluateSpeculativeNumericOperation(
    const Left& left, const Right& right, EvaluationContext* context) {
  checkNumericOperandSize(left, context);
  checkNumericOperandSize(right, context);

  using FastFunction = RawNumericFunctionT<Function>;
  FastFunction fastFunction;
  Function genericFunction;

  constexpr auto expectedLeftType = datatypeForNumericType<LeftNumericType>();
  constexpr auto expectedRightType = datatypeForNumericType<RightNumericType>();

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.resize(context->size());

  ad_utility::chunkedForLoop<1000>(
      0, context->size(),
      [&left, &right, &result, &fastFunction, &genericFunction,
       context](size_t i) {
        const auto leftValue = getIdAt(left, i);
        const auto rightValue = getIdAt(right, i);

        if (leftValue.getDatatype() == expectedLeftType &&
            rightValue.getDatatype() == expectedRightType) {
          result[i] = fastFunction(
              getPrimitiveNumericValue<LeftNumericType>(leftValue),
              getPrimitiveNumericValue<RightNumericType>(rightValue));
        } else {
          result[i] =
              evaluateSpeculativeNumericSlowPath<Function, LeftValueGetter,
                                                 RightValueGetter>(
                  leftValue, rightValue, context, fastFunction,
                  genericFunction);
        }
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return result;
}

}  // namespace sparqlExpression::detail::homogeneousNumeric

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H
