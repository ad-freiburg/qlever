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
#include <type_traits>

#include "backports/concepts.h"
#include "engine/sparqlExpressions/NaryExpressionImpl.h"
#include "engine/sparqlExpressions/SparqlExpressionValueGetters.h"
#include "util/ChunkedForLoop.h"

namespace sparqlExpression::detail::homogeneousNumeric {

// The numeric type shared by all elements of an operand. `Other` represents
// mixed numeric types as well as non-numeric values.
enum class HomogeneousNumericType {
  Int,
  Double,
  Other,
};

// Classification of a numeric operand. `homogeneousType_` is `Int` or `Double`
// only if every value has that datatype. Otherwise it is `Other`.
// `preferredType_` is `Int` or `Double` if that datatype is the unique most
// common datatype in the operand. Otherwise it is `Other`. It is used for
// speculative evaluation.
struct NumericOperandClassification {
  HomogeneousNumericType homogeneousType_ = HomogeneousNumericType::Other;
  HomogeneousNumericType preferredType_ = HomogeneousNumericType::Other;
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

// Classify a span and determine its preferred numeric datatype when an integer
// or double is the unique most common datatype in the input.
inline NumericOperandClassification classifyNumericOperandWithPreferredType(
    ql::span<const ValueId> values, const EvaluationContext* context) {
  if (values.empty()) {
    return {};
  }

  size_t numInts = 0;
  size_t numDoubles = 0;

  ad_utility::chunkedForLoop<1000>(
      0, values.size(),
      [&values, &numInts, &numDoubles](size_t i) {
        const auto type = values[i].getDatatype();
        numInts += static_cast<size_t>(type == Datatype::Int);
        numDoubles += static_cast<size_t>(type == Datatype::Double);
      },
      [context]() { context->cancellationHandle_->throwIfCancelled(); });

  NumericOperandClassification result;

  if (numInts == values.size()) {
    result.homogeneousType_ = HomogeneousNumericType::Int;
  } else if (numDoubles == values.size()) {
    result.homogeneousType_ = HomogeneousNumericType::Double;
  }

  if (numInts == numDoubles) {
    return result;
  }

  const auto preferredType = numInts > numDoubles
                                 ? HomogeneousNumericType::Int
                                 : HomogeneousNumericType::Double;
  const auto preferredCount = std::max(numInts, numDoubles);

  // If more than half of all values have this datatype, it is necessarily the
  // unique most common datatype.
  if (preferredCount > values.size() / 2) {
    result.preferredType_ = preferredType;
    return result;
  }

  // Otherwise, check whether some non-numeric datatype occurs at least as often
  // as the numeric candidate.
  constexpr size_t numDatatypes = static_cast<size_t>(Datatype::MaxValue) + 1;
  std::array<size_t, numDatatypes> datatypeCounts{};

  ad_utility::chunkedForLoop<1000>(
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

  if (preferredCount > maxOtherCount) {
    result.preferredType_ = preferredType;
  }

  return result;
}

// Classify a single `ValueId` and use its numeric datatype as both the
// homogeneous and preferred type.
inline NumericOperandClassification classifyNumericOperandWithPreferredType(
    ValueId value) {
  switch (value.getDatatype()) {
    case Datatype::Int:
      return {HomogeneousNumericType::Int, HomogeneousNumericType::Int};
    case Datatype::Double:
      return {HomogeneousNumericType::Double, HomogeneousNumericType::Double};
    default:
      return {};
  }
}

// Classify a supported operand representation. Constants are classified
// directly, while vector-like operands are viewed as spans of `ValueId`.
template <typename Operand>
inline NumericOperandClassification classifyNumericOperandWithPreferredType(
    const Operand& operand, const EvaluationContext* context) {
  using OperandType = std::decay_t<Operand>;

  static_assert(
      supportsHomogeneousNumericOperand<Operand>(),
      "Unsupported operand representation for numeric classification");

  if constexpr (ad_utility::isSimilar<OperandType, ValueId>) {
    return classifyNumericOperandWithPreferredType(operand);
  } else {
    return classifyNumericOperandWithPreferredType(
        ql::span<const ValueId>{operand.data(), operand.size()}, context);
  }
}

// Classify all operands and determine both their homogeneous and preferred
// numeric datatypes.
template <typename... Operands>
inline auto classifyNumericOperandsWithPreferredType(
    const EvaluationContext* context, const Operands&... operands) {
  return std::array<NumericOperandClassification, sizeof...(Operands)>{
      classifyNumericOperandWithPreferredType(operands, context)...};
}

// Classify all values in a span as integer, double, or other.
inline HomogeneousNumericType classifyNumericOperand(
    ql::span<const ValueId> values, const EvaluationContext* context) {
  return classifyNumericOperandWithPreferredType(values, context)
      .homogeneousType_;
}

// Classify a single `ValueId` by its numeric datatype.
inline HomogeneousNumericType classifyNumericOperand(ValueId value) {
  return classifyNumericOperandWithPreferredType(value).homogeneousType_;
}

// Classify a supported operand representation. Constants are classified
// directly, while vector-like operands are viewed as spans of `ValueId`.
template <typename Operand>
inline HomogeneousNumericType classifyNumericOperand(
    const Operand& operand, const EvaluationContext* context) {
  return classifyNumericOperandWithPreferredType(operand, context)
      .homogeneousType_;
}

// Classify all operands by their homogeneous numeric datatype.
template <typename... Operands>
inline auto classifyNumericOperands(const EvaluationContext* context,
                                    const Operands&... operands) {
  return std::array<HomogeneousNumericType, sizeof...(Operands)>{
      classifyNumericOperand(operands, context)...};
}

}  // namespace sparqlExpression::detail::homogeneousNumeric

#endif  // QLEVER_SRC_ENGINE_SPARQLEXPRESSIONS_NUMERICOPERANDCLASSIFICATION_H
