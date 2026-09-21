// Copyright 2026 The QLever Authors, in particular:
//
// 2026 Prashanth Premakumar <prashanthp0703@gmail.com>, UFR

// UFR = University of Freiburg, Chair of Algorithms and Data Structures

// You may not use this file except in compliance with the Apache 2.0 License,
// which can be found in the `LICENSE` file at the root of the QLever project.

#ifndef QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICOPERANDCLASSIFICATION_H
#define QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICOPERANDCLASSIFICATION_H

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <type_traits>

#include "backports/concepts.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail::homogeneousNumeric {

// Helpers for classifying numeric operands by their homogeneous and majority
// datatypes for use by the numeric fast paths.

// A numeric datatype supported by the optimized numeric evaluation paths.
// `Other` represents values that cannot be represented as `Int` or `Double`.
enum class NumericType {
  Int,
  Double,
  Other,
};

// Classification of a numeric operand. `homogeneousType_` is `Int` or `Double`
// only if every value has that datatype. Otherwise it is `Other`.
// `majorityType_` is `Int` or `Double` if that datatype is the unique most
// common datatype in the operand. Otherwise it is `Other`. It is used for
// speculative evaluation.
struct NumericOperandClassification {
  NumericType homogeneousType_ = NumericType::Other;
  NumericType majorityType_ = NumericType::Other;
};

// Whether a value getter can participate in the homogeneous numeric fast path.
template <typename ValueGetter>
inline constexpr bool supportsHomogeneousNumericFastPath =
    ad_utility::SameAsAny<ValueGetter, NumericValueGetter,
                          NumericOrDateValueGetter>;

// Return whether `Operand` has a representation that can be inspected directly
// by the homogeneous numeric classifier. Currently this is restricted to
// `ValueId` constants and vectors whose elements are `ValueId`s.
template <typename Operand>
constexpr bool supportsHomogeneousNumericOperand() {
  using OperandType = std::decay_t<Operand>;

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return true;
  } else if constexpr (isVectorResult<OperandType>) {
    using ElementType = ql::ranges::range_value_t<OperandType>;
    return ad_utility::isSimilar<ElementType, ValueId>;
  } else {
    return false;
  }
}

// Classify a span by determining whether all values have the same numeric
// datatype and whether an integer or double is the unique most common datatype.
inline NumericOperandClassification classifyNumericOperand(
    ql::span<const ValueId> values, const EvaluationContext* context) {
  if (values.empty()) {
    return {};
  }

  size_t numInts = 0;
  size_t numDoubles = 0;

  ad_utility::chunkedForLoop<100'000>(
      0, values.size(),
      [&values, &numInts, &numDoubles](size_t i) {
        const auto type = values[i].getDatatype();
        numInts += static_cast<size_t>(type == Datatype::Int);
        numDoubles += static_cast<size_t>(type == Datatype::Double);
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  NumericOperandClassification result;

  if (numInts == values.size()) {
    result.homogeneousType_ = NumericType::Int;
  } else if (numDoubles == values.size()) {
    result.homogeneousType_ = NumericType::Double;
  }

  if (numInts == numDoubles) {
    return result;
  }

  const auto majorityType =
      numInts > numDoubles ? NumericType::Int : NumericType::Double;
  const auto majorityCount = std::max(numInts, numDoubles);

  // If more than half of all values have this datatype, it is necessarily the
  // unique most common datatype.
  if (majorityCount > values.size() / 2) {
    result.majorityType_ = majorityType;
    return result;
  }

  // Otherwise, check whether some non-numeric datatype occurs at least as often
  // as the numeric candidate.
  constexpr size_t numDatatypes = static_cast<size_t>(Datatype::MaxValue) + 1;
  std::array<size_t, numDatatypes> datatypeCounts{};

  ad_utility::chunkedForLoop<100'000>(
      0, values.size(),
      [&values, &datatypeCounts](size_t i) {
        const auto type = values[i].getDatatype();
        if (type != Datatype::Int && type != Datatype::Double) {
          ++datatypeCounts[static_cast<size_t>(type)];
        }
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  size_t maxOtherCount = 0;
  for (const auto count : datatypeCounts) {
    maxOtherCount = std::max(maxOtherCount, count);
  }

  if (majorityCount > maxOtherCount) {
    result.majorityType_ = majorityType;
  }

  return result;
}

// Classify a single `ValueId` and use its numeric datatype as both the
// homogeneous and majority type.
inline NumericOperandClassification classifyNumericOperand(ValueId value) {
  switch (value.getDatatype()) {
    using enum NumericType;
    case Datatype::Int:
      return {Int, Int};
    case Datatype::Double:
      return {Double, Double};
    default:
      return {};
  }
}

// Classify a supported operand representation. Constants are classified
// directly, while vector-like operands are viewed as spans of `ValueId`.
template <typename Operand>
inline NumericOperandClassification classifyNumericOperand(
    const Operand& operand, const EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  static_assert(
      supportsHomogeneousNumericOperand<Operand>(),
      "Unsupported operand representation for numeric classification");

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return classifyNumericOperand(operand);
  } else {
    return classifyNumericOperand(
        ql::span<const ValueId>{operand.data(), operand.size()}, context);
  }
}

// Classify all operands and determine both their homogeneous and majority
// numeric datatypes.
template <typename... Operands>
inline auto classifyNumericOperands(const EvaluationContext* context,
                                    const Operands&... operands) {
  return std::array<NumericOperandClassification, sizeof...(Operands)>{
      classifyNumericOperand(operands, context)...};
}

// Return the homogeneous numeric type of every operand if all operands are
// homogeneous. Otherwise return `std::nullopt`.
template <size_t N>
std::optional<std::array<NumericType, N>> getHomogeneousNumericTypes(
    const std::array<NumericOperandClassification, N>& classifications) {
  std::array<NumericType, N> types;

  for (size_t i = 0; i < N; ++i) {
    if (classifications[i].homogeneousType_ == NumericType::Other) {
      return std::nullopt;
    }
    types[i] = classifications[i].homogeneousType_;
  }

  return types;
}

// Return the majority numeric type of every operand if all operands have one.
// Otherwise return `std::nullopt`.
template <size_t N>
std::optional<std::array<NumericType, N>> getMajorityNumericTypes(
    const std::array<NumericOperandClassification, N>& classifications) {
  std::array<NumericType, N> types;

  for (size_t i = 0; i < N; ++i) {
    if (classifications[i].majorityType_ == NumericType::Other) {
      return std::nullopt;
    }
    types[i] = classifications[i].majorityType_;
  }

  return types;
}

}  // namespace sparqlExpression::detail::homogeneousNumeric

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICOPERANDCLASSIFICATION_H
