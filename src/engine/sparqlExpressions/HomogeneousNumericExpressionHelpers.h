// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_HOMOGENEOUSNUMERICEXPRESSIONHELPERS_H

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>

#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail::homogeneousNumeric {

// Helpers for evaluating binary numeric expressions whose operands are
// homogeneous with respect to their numeric datatype.
//
// The generic numeric value getters return variants and therefore require
// variant dispatch for every result row. For operands that contain only
// integers or only doubles, these helpers first classify the complete operand
// and then evaluate the expression using the corresponding primitive C++
// types. Mixed or non-numeric operands are handled by the generic
// BinaryExpression path.

// The numeric type shared by all elements of an operand. `Other` represents
// mixed numeric types as well as non-numeric values.
enum class HomogeneousNumericType {
  Int,
  Double,
  Other,
};

struct HomogeneousNumericTypes {
  HomogeneousNumericType left;
  HomogeneousNumericType right;
};

// Whether a value getter can participate in the homogeneous numeric fast path.
// Only getters whose numeric values can be represented directly as `int64_t`
// or `double` opt in.
template <typename ValueGetter>
inline constexpr bool supportsHomogeneousNumericFastPath = false;

template <>
inline constexpr bool supportsHomogeneousNumericFastPath<NumericValueGetter> =
    true;

template <>
inline constexpr bool
    supportsHomogeneousNumericFastPath<NumericOrDateValueGetter> = true;

// Return whether `Operand` has a representation that can be inspected directly
// by the homogeneous numeric classifier. Currently this is restricted to
// `ValueId` constants and vectors whose elements are `ValueId`s.
template <typename Operand>
constexpr bool supportsHomogeneousNumericOperand() {
  using OperandType = std::decay_t<Operand>;

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return true;
  } else if constexpr (isVectorResult<OperandType>) {
    using ElementType =
        std::decay_t<decltype(std::declval<const OperandType&>()[size_t{}])>;

    return ad_utility::isSimilar<ElementType, ValueId>;
  } else {
    return false;
  }
}

// Classify an entire vector as integer, double, or other. The complete vector
// is scanned without datatype-dependent early exits so that a successful
// classification enables a single typed dispatch for the subsequent
// evaluation loop.
inline HomogeneousNumericType classifyNumericOperand(
    ql::span<const ValueId> values, EvaluationContext* context) {
  // An empty vector has no meaningful homogeneous numeric type.
  if (values.empty()) {
    return HomogeneousNumericType::Other;
  }

  bool allInt = true;
  bool allDouble = true;

  ad_utility::chunkedForLoop<1000>(
      0, values.size(),
      [&](size_t i) {
        const auto type = values[i].getDatatype();

        allInt &= type == Datatype::Int;
        allDouble &= type == Datatype::Double;
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  if (allInt) {
    return HomogeneousNumericType::Int;
  }

  if (allDouble) {
    return HomogeneousNumericType::Double;
  }

  return HomogeneousNumericType::Other;
}

inline HomogeneousNumericType classifyNumericOperand(ValueId value) {
  switch (value.getDatatype()) {
    case Datatype::Int:
      return HomogeneousNumericType::Int;
    case Datatype::Double:
      return HomogeneousNumericType::Double;
    default:
      return HomogeneousNumericType::Other;
  }
}

template <typename Operand>
inline HomogeneousNumericType classifyNumericOperand(
    const Operand& operand, EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  static_assert(supportsHomogeneousNumericOperand<Operand>(),
                "Unsupported operand representation for homogeneous numeric "
                "classification");

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return classifyNumericOperand(operand);
  } else {
    return classifyNumericOperand(
        ql::span<const ValueId>{operand.data(), operand.size()}, context);
  }
}

template <typename Left, typename Right>
inline HomogeneousNumericTypes classifyNumericOperands(
    const Left& left, const Right& right, EvaluationContext* context) {
  return {
      classifyNumericOperand(left, context),
      classifyNumericOperand(right, context),
  };
}

template <typename NumericType>
NumericType getHomogeneousNumericValue(ValueId value) {
  if constexpr (std::same_as<NumericType, int64_t>) {
    return value.getInt();
  } else if constexpr (std::same_as<NumericType, double>) {
    return value.getDouble();
  } else {
    static_assert(ad_utility::alwaysFalse<NumericType>,
                  "Unsupported homogeneous numeric type");
  }
}

template <typename NumericType, typename Operand>
auto makeHomogeneousNumericGetter(const Operand& operand) {
  using OperandType = std::decay_t<Operand>;

  if constexpr (isVectorResult<OperandType>) {
    return [&operand](size_t i) {
      return getHomogeneousNumericValue<NumericType>(operand[i]);
    };
  } else {
    static_assert(ad_utility::isSimilar<OperandType, ValueId>,
                  "Homogeneous numeric fast path currently supports "
                  "ValueId constants");

    const auto value = getHomogeneousNumericValue<NumericType>(operand);

    return [value](size_t) { return value; };
  }
}

// Map functions wrapped in `MakeNumericExpression` to a function that accepts
// primitive numeric types directly while preserving the usual conversion to
// `ValueId` and the handling of NaN/Inf.
template <typename Function>
struct RawNumericFunction {
  using type = Function;
};

template <typename Function, bool NanOrInfToUndef>
struct RawNumericFunction<MakeNumericExpression<Function, NanOrInfToUndef>> {
  using type = NumericIdWrapper<Function, NanOrInfToUndef>;
};

template <typename Function>
using RawNumericFunctionT = typename RawNumericFunction<Function>::type;

// Evaluate a binary operation after both operands have been classified as
// homogeneous numeric types. The value getters below extract primitive C++
// values directly, avoiding per-row variant dispatch.
template <typename Function, typename LeftNumericType,
          typename RightNumericType, typename Left, typename Right>
ExpressionResult evaluateHomogeneousNumericOperation(
    const Left& left, const Right& right, EvaluationContext* context) {
  using LeftType = std::decay_t<Left>;
  using RightType = std::decay_t<Right>;

  if constexpr (isVectorResult<LeftType>) {
    AD_CORRECTNESS_CHECK(left.size() == context->size());
  }

  if constexpr (isVectorResult<RightType>) {
    AD_CORRECTNESS_CHECK(right.size() == context->size());
  }

  using FastFunction = RawNumericFunctionT<Function>;
  FastFunction function;

  auto getLeft = makeHomogeneousNumericGetter<LeftNumericType>(left);
  auto getRight = makeHomogeneousNumericGetter<RightNumericType>(right);

  VectorWithMemoryLimit<Id> result{context->_allocator};
  result.reserve(context->size());

  ad_utility::chunkedForLoop<1000>(
      0, context->size(),
      [&](size_t i) { result.push_back(function(getLeft(i), getRight(i))); },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  return result;
}

}  // namespace sparqlExpression::detail::homogeneousNumeric

#endif
